%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_kv_exchange_fsm).
-behaviour(gen_fsm).

-compile({nowarn_deprecated_function, 
            [{gen_fsm, start, 3},
                {gen_fsm, send_event, 2},
                {gen_fsm, send_event_after, 2},
                {gen_fsm, cancel_timer, 1}]}).

%% API
-export([start/5]).

%% FSM states
-export([prepare_exchange/2,
         update_trees/2,
         key_exchange/2,
         repair_consistent/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.
-type vnode() :: {index(), node()}.

-record(state, {local       :: vnode(),
                remote      :: vnode(),
                index_n     :: index_n(),
                local_tree  :: pid(),
                remote_tree :: pid() | undefined,
                built       :: non_neg_integer(),
                timer       :: reference() | undefined,
                timeout     :: pos_integer()
               }).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 60000). %% 1 minute

%% Use occasional calls to disk_log:log() for some backpressure periodically
-define(LOG_BATCH_SIZE, 5000).

-include_lib("kernel/include/logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start(LocalVN, RemoteVN, IndexN, Tree, Manager) ->
    gen_fsm:start(?MODULE, [LocalVN, RemoteVN, IndexN, Tree, Manager], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([LocalVN, RemoteVN, IndexN, LocalTree, Manager]) ->
    Timeout = app_helper:get_env(riak_kv,
                                 anti_entropy_timeout,
                                 ?DEFAULT_ACTION_TIMEOUT),
    monitor(process, Manager),
    monitor(process, LocalTree),
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index_n=IndexN,
                   local_tree=LocalTree,
                   timeout=Timeout,
                   built=0},
    gen_fsm:send_event(self(), start_exchange),
    ?LOG_DEBUG("Starting exchange: ~p", [LocalVN]),
    {ok, prepare_exchange, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, normal, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm states
%%%===================================================================

%% @doc Initial state. Attempt to acquire all necessary exchange locks.
%%      In order, acquire local concurrency lock, local tree lock,
%%      remote concurrency lock, and remote tree lock. Exchange will
%%      timeout if locks cannot be acquired in a timely manner.
prepare_exchange(start_exchange, State=#state{remote=RemoteVN,
                                              index_n=IndexN,
                                              local_tree=Tree}) ->
    case riak_kv_entropy_manager:get_lock(exchange) of
        ok ->
            case riak_kv_index_hashtree:get_lock_and_version(Tree, local_fsm) of
                {ok, Version} ->
                    remote_exchange_request(RemoteVN, IndexN, Version),
                    Timer = gen_fsm:send_event_after(State#state.timeout,
                                                     timeout),
                    {next_state, prepare_exchange, State#state{timer=Timer}};
                _ ->
                    send_exchange_status(already_locked, State),
                    {stop, normal, State}
            end;
        Error ->
            send_exchange_status(Error, State),
            {stop, normal, State}
    end;
prepare_exchange(timeout, State) ->
    do_timeout(State);
prepare_exchange({remote_exchange, Pid}, State) when is_pid(Pid) ->
    _ = gen_fsm:cancel_timer(State#state.timer),
    monitor(process, Pid),
    State2 = State#state{remote_tree=Pid, timer=undefined},
    update_trees(start_exchange, State2);
prepare_exchange({remote_exchange, Error}, State) ->
    _ = gen_fsm:cancel_timer(State#state.timer),
    send_exchange_status({remote, Error}, State),
    {stop, normal, State#state{timer=undefined}}.

%% @doc Now that locks have been acquired, ask both the local and remote
%%      hashtrees to perform a tree update. If updates do not occur within
%%      a timely manner, the exchange will timeout. Since the trees will
%%      continue to finish the update even after the exchange times out,
%%      a future exchange should eventually make progress.
update_trees(start_exchange, State=#state{local=LocalVN,
                                          remote=RemoteVN,
                                          local_tree=LocalTree,
                                          remote_tree=RemoteTree,
                                          index_n=IndexN}) ->
    ?LOG_DEBUG("Sending to ~p", [LocalVN]),
    ?LOG_DEBUG("Sending to ~p", [RemoteVN]),

    update_request(LocalTree, LocalVN, IndexN),
    update_request(RemoteTree, RemoteVN, IndexN),
    {next_state, update_trees, State};

update_trees({not_responsible, VNodeIdx, IndexN}, State) ->
    ?LOG_DEBUG("VNode ~p does not cover preflist ~p", [VNodeIdx, IndexN]),
    send_exchange_status({not_responsible, VNodeIdx, IndexN}, State),
    {stop, normal, State};
update_trees({tree_built, _, _}, State) ->
    Built = State#state.built + 1,
    case Built of
        2 ->
            ?LOG_DEBUG("Moving to key exchange"),
            {next_state, key_exchange, State, 0};
        _ ->
            {next_state, update_trees, State#state{built=Built}}
    end.

%% @doc Now that locks have been acquired and both hashtrees have been updated,
%%      perform a key exchange and trigger read repair for any divergent keys.
key_exchange(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   local_tree=LocalTree,
                                   remote_tree=RemoteTree,
                                   index_n=IndexN}) ->
    ?LOG_DEBUG("Starting key exchange between ~p and ~p", [LocalVN, RemoteVN]),
    ?LOG_DEBUG("Exchanging hashes for preflist ~p", [IndexN]),

    TmpDir = tmp_dir(),
    {NA, NB, NC} = Now = WriteLog = os:timestamp(),
    LogFile1 = lists:flatten(io_lib:format("~s/in.~p.~p.~p",
                                           [TmpDir, NA, NB, NC])),
    LogFile2 = lists:flatten(io_lib:format("~s/out.~p.~p.~p",
                                           [TmpDir, NA, NB, NC])),
    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket(RemoteTree, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment(RemoteTree, IndexN, Segment);
                (init, _Y) ->
                     %% Our return value is ignored, so we can't return
                     %% the disk log handle here.  However, disk_log is
                     %% magically stateful, so we don't need to change
                     %% the exchange API to accomodate us.
                     {ok, _} = open_disk_log(Now, LogFile1, read_write),
                     ok;
                (final, _Y) ->
                     ok = disk_log:sync(Now),
                     ok = disk_log:close(Now),
                     ok;
                (start_exchange_level, {_Level, _Buckets}) ->
                     ok;
                (start_exchange_segments, _Segments) ->
                     ok;
                (_X, _Y) ->
                     ?LOG_ERROR("~s LINE ~p: ~p ~p", [?MODULE, ?LINE, _X, _Y]),
                     ok
             end,

    %% Unclear if we should allow exchange to run indefinitely or enforce
    %% a timeout. The problem is that depending on the number of keys and
    %% key differences, exchange can take arbitrarily long. For now, go with
    %% unbounded exchange, with the ability to cancel exchanges through the
    %% entropy manager if needed.
    {ok, RC} = riak:local_client(),
    AccFun = fun(KeyDiff, Acc) ->
                     lists:foldl(fun({DiffReason, BKeyBin}, Count) ->
                                         {B, K} = binary_to_term(BKeyBin),
                                         T = {B, K, DiffReason},
                                         if Count rem ?LOG_BATCH_SIZE == 0 ->
                                                 ok = disk_log:log(WriteLog, T);
                                            true ->
                                                 ok = disk_log:alog(WriteLog, T)
                                         end,
                                         Count+1
                                 end, Acc, KeyDiff)
             end,
    %% TODO: Add stats for AAE
    Count = riak_kv_index_hashtree:compare(IndexN, Remote, AccFun, 0, LocalTree),
    if Count == 0 ->
            Complete = true,
            ok;
       true ->
            %% Sort the keys.  For vnodes that use backends that preserve
            %% lexicographic sort order for BKeys, this is a big
            %% improvement.  For backends that do not, e.g. Bitcask, sorting
            %% by BKey is unlikely to be any worse.  For Riak CS's use
            %% pattern, sorting may have some benefit since block N is
            %% likely to be nearby on disk of block N+1.
            StartTime = os:timestamp(),
            ok = sort_disk_log(LogFile1, LogFile2),
            ?LOG_DEBUG("~s:key_exchange: sorting time = ~p seconds\n",
                        [?MODULE, timer:now_diff(os:timestamp(), StartTime) / 1000000]),
            {ok, ReadLog} = open_disk_log(Now, LogFile2, read_only),
            FoldRes =
                fold_disk_log(fun(Diff, Acc) ->
                                      read_repair_keydiff(RC, LocalVN, RemoteVN,
                                                          Diff),
                                      Acc + 1
                              end, 0, ReadLog),
            disk_log:close(ReadLog),
            if Count == FoldRes ->
                    Complete = true,
                    ok;
               true ->
                    ?LOG_ERROR("~s:key_exchange: Count ~p /= FoldRes ~p\n",
                                [?MODULE, Count, FoldRes]),
                    send_exchange_status(failed, State),
                    Complete = false
            end,
            ?LOG_INFO("Repaired ~b keys during active anti-entropy exchange "
                       "of ~p between ~p and ~p",
                       [Count, IndexN, LocalVN, RemoteVN])
    end,
    [exchange_complete(LocalVN, RemoteVN, IndexN, Count) || Complete],
    _ = file:delete(LogFile1),
    _ = file:delete(LogFile2),
    {stop, normal, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
exchange_bucket(Tree, IndexN, Level, Bucket) ->
    riak_kv_index_hashtree:exchange_bucket(IndexN, Level, Bucket, Tree).

%% @private
exchange_segment(Tree, IndexN, Segment) ->
    riak_kv_index_hashtree:exchange_segment(IndexN, Segment, Tree).

%% @private
read_repair_keydiff(RC, LocalVN, RemoteVN, {Bucket, Key, _Reason}) ->
    %% TODO: Even though this is at debug level, it's still extremely
    %%       spammy. Should this just be removed? We can always use
    %%       redbug to trace read_repair_keydiff when needed. Of course,
    %%       users can't do that.
    ?LOG_DEBUG("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
    case riak_kv_util:consistent_object(Bucket) of
        true ->
            BKey = {Bucket, Key},
            repair_consistent(BKey);
        false ->
            riak_client:get(Bucket, Key, RC)
    end,
    %% Force vnodes to update AAE tree in case read repair wasn't triggered
    riak_kv_vnode:rehash([LocalVN, RemoteVN], Bucket, Key),
    riak_kv_entropy_manager:throttle(),
    ok.

repair_consistent(BKey) ->
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 60000,
    _ = riak_ensemble_client:kget(node(), Ensemble, BKey, Timeout, [read_repair]),
    ok.

%% @private
update_request(Tree, {Index, _}, IndexN) ->
    as_event(fun() ->
                     case riak_kv_index_hashtree:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

remote_exchange_request(RemoteVN, IndexN, Version) ->
    FsmPid = self(),
    as_event(fun() ->
                     riak_kv_entropy_manager:start_exchange_remote(RemoteVN,
                                                                   IndexN,
                                                                   FsmPid,
                                                                   Version)
             end).

%% @private
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm:send_event(Self, Result)
               end),
    ok.

%% @private
do_timeout(State=#state{local=LocalVN,
                        remote=RemoteVN,
                        index_n=IndexN}) ->
    ?LOG_INFO("Timeout during exchange between (local) ~p and (remote) ~p, "
               "(preflist) ~p", [LocalVN, RemoteVN, IndexN]),
    send_exchange_status({timeout, RemoteVN, IndexN}, State),
    {stop, normal, State#state{timer=undefined}}.

%% @private
send_exchange_status(Status, #state{local=LocalVN,
                                    remote=RemoteVN,
                                    index_n=IndexN}) ->
    riak_kv_entropy_manager:exchange_status(LocalVN, RemoteVN, IndexN, Status).

exchange_complete({LocalIdx, _}, {RemoteIdx, RemoteNode}, IndexN, Repaired) ->
    riak_kv_entropy_info:exchange_complete(LocalIdx, RemoteIdx, IndexN, Repaired),
    rpc:call(RemoteNode, riak_kv_entropy_info, exchange_complete,
             [RemoteIdx, LocalIdx, IndexN, Repaired]).

open_disk_log(Name, Path, RWorRO) ->
    open_disk_log(Name, Path, RWorRO, [{type, halt}, {format, internal}]).

open_disk_log(Name, Path, RWorRO, OtherOpts) ->
    disk_log:open([{name, Name}, {file, Path}, {mode, RWorRO}|OtherOpts]).

sort_disk_log(InputFile, OutputFile) ->
    {ok, ReadLog} = open_disk_log(os:timestamp(), InputFile, read_only),
    _ = file:delete(OutputFile),
    {ok, WriteLog} = open_disk_log(os:timestamp(), OutputFile, read_write),
    Input = sort_disk_log_input(ReadLog),
    Output = sort_disk_log_output(WriteLog),
    try
        file_sorter:sort(Input, Output, [{format, term}, {tmpdir, tmp_dir()}])
    after
        ok = disk_log:close(ReadLog),
        ok = disk_log:close(WriteLog)
    end.

sort_disk_log_input(ReadLog) ->
    sort_disk_log_input(ReadLog, start).

sort_disk_log_input(ReadLog, Cont) ->
    fun(close) ->
            ok;
       (read) ->
            case disk_log:chunk(ReadLog, Cont) of
                {error, Reason} ->
                    {error, Reason};
                {Cont2, Terms} ->
                    {Terms, sort_disk_log_input(ReadLog, Cont2)};
                {Cont2, Terms, _Badbytes} ->
                    {Terms, sort_disk_log_input(ReadLog, Cont2)};
                eof ->
                    end_of_input
            end
    end.

sort_disk_log_output(WriteLog) ->
    sort_disk_log_output(WriteLog, 1).

sort_disk_log_output(WriteLog, Count) ->
    fun(close) ->
            ok;
       (Terms) ->
            %% Typical length of terms is on the order of 1-1500
            %% e.g. [{Bucket1, Key1, missing|remote_missing|different}, ...]
            if Count rem 100 == 0 ->
                    disk_log:log_terms(WriteLog, Terms);
                true ->
                    disk_log:alog_terms(WriteLog, Terms)
            end,
            sort_disk_log_output(WriteLog, Count + 1)
    end.

fold_disk_log(Fun, Acc, DiskLog) ->
    fold_disk_log(disk_log:chunk(DiskLog, start), Fun, Acc, DiskLog).

fold_disk_log(eof, _Fun, Acc, _DiskLog) ->
    Acc;
fold_disk_log({Cont, Terms}, Fun, Acc, DiskLog) ->
    Acc2 = try
               lists:foldl(Fun, Acc, Terms)
    catch Class:Reason:Stacktrace ->
            ?LOG_ERROR("~s:fold_disk_log: caught ~p ~p @ ~p\n",
                        [?MODULE, Class, Reason, Stacktrace]),
            Acc
    end,
    fold_disk_log(disk_log:chunk(DiskLog, Cont), Fun, Acc2, DiskLog).

tmp_dir() ->
    PDD = app_helper:get_env(riak_core, platform_data_dir, "/tmp"),
    TmpDir = filename:join(PDD, ?MODULE),
    TmpCanary = filename:join(TmpDir, "canary"),
    ok = filelib:ensure_dir(TmpCanary),
    TmpDir.
