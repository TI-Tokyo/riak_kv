%% -------------------------------------------------------------------
%%
%% riak_util: functions that are useful throughout Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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


%% @doc Various functions that are useful throughout riak_kv.
-module(riak_kv_util).


-export([is_x_deleted/1,
        obj_not_deleted/1,
        try_cast/3,
        fallback/4,
        expand_value/3,
        expand_rw_value/4,
        expand_sync_on_write/2,
        normalize_rw_value/2,
        make_request/2,
        get_index_n/1,
        preflist_siblings/1,
        fix_incorrect_index_entries/1,
        fix_incorrect_index_entries/0,
        responsible_preflists/1,
        responsible_preflists/2,
        responsible_preflists/3,
        make_vtag/1,
        puts_active/0,
        exact_puts_active/0,
        gets_active/0,
        consistent_object/1,
        get_write_once/1,
        overload_reply/1,
        get_backend_config/3,
        is_modfun_allowed/2,
        shuffle_list/1,
        kv_ready/0,
        ngr_initial_timeout/0,
        sys_monitor_count/0
    ]).
-export([report_hashtree_tokens/0, reset_hashtree_tokens/2]).

-export([
    profile_riak/1,
    top_n_binary_total_memory/1,
    summarise_binary_memory_by_initial_call/1,
    top_n_process_total_memory/1,
    summarise_process_memory_by_initial_call/1,
    get_initial_call/1
]
).

-include_lib("kernel/include/logger.hrl").

-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

-export_type([index_n/0]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec is_x_deleted(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as deleted; 'false' otherwise
%% @equiv obj_not_deleted(Obj) == undefined
is_x_deleted(Obj) ->
    case obj_not_deleted(Obj) of
        undefined -> true;
        _ -> false
    end.

%% @spec obj_not_deleted(riak_object:riak_object()) ->
%%          undefined|riak_object:riak_object()
%% @doc Determine whether all contents of an object are marked as
%%      deleted.  Return is the atom 'undefined' if all contents
%%      are marked deleted, or the input Obj if any of them are not.
obj_not_deleted(Obj) ->
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    dict:is_key(<<"X-Riak-Deleted">>, M) =:= false] of
        [] -> undefined;
        _ -> Obj
    end.

%% @spec try_cast(term(), [node()], [{Index :: term(), Node :: node()}]) ->
%%          {[{Index :: term(), Node :: node(), Node :: node()}],
%%           [{Index :: term(), Node :: node()}]}
%% @doc Cast {Cmd, {Index,Node}, Msg} at riak_kv_vnode_master on Node
%%      if Node is in UpNodes.  The list of successful casts is the
%%      first element of the return tuple, and the list of unavailable
%%      nodes is the second element.  Used in riak_kv_put_fsm and riak_kv_get_fsm.
try_cast(Msg, UpNodes, Targets) ->
    try_cast(Msg, UpNodes, Targets, [], []).
try_cast(_Msg, _UpNodes, [], Sent, Pangs) -> {Sent, Pangs};
try_cast(Msg, UpNodes, [{Index,Node}|Targets], Sent, Pangs) ->
    case lists:member(Node, UpNodes) of
        false ->
            try_cast(Msg, UpNodes, Targets, Sent, [{Index,Node}|Pangs]);
        true ->
            gen_server:cast({riak_kv_vnode_master, Node}, make_request(Msg, Index)),
            try_cast(Msg, UpNodes, Targets, [{Index,Node,Node}|Sent],Pangs)
    end.

%% @spec fallback(term(), term(), [{Index :: term(), Node :: node()}],
%%                [{any(), Fallback :: node()}]) ->
%%         [{Index :: term(), Node :: node(), Fallback :: node()}]
%% @doc Cast {Cmd, {Index,Node}, Msg} at a node in the Fallbacks list
%%      for each node in the Pangs list.  Pangs should have come
%%      from the second element of the response tuple of a call to
%%      try_cast/3.
%%      Used in riak_kv_put_fsm and riak_kv_get_fsm

fallback(Cmd, UpNodes, Pangs, Fallbacks) ->
    fallback(Cmd, UpNodes, Pangs, Fallbacks, []).
fallback(_Cmd, _UpNodes, [], _Fallbacks, Sent) -> Sent;
fallback(_Cmd, _UpNodes, _Pangs, [], Sent) -> Sent;
fallback(Cmd, UpNodes, [{Index,Node}|Pangs], [{_,FN}|Fallbacks], Sent) ->
    case lists:member(FN, UpNodes) of
        false -> fallback(Cmd, UpNodes, [{Index,Node}|Pangs], Fallbacks, Sent);
        true ->
            gen_server:cast({riak_kv_vnode_master, FN}, make_request(Cmd, Index)),
            fallback(Cmd, UpNodes, Pangs, Fallbacks, [{Index,Node,FN}|Sent])
    end.


-spec make_request(vnode_req(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Index) ->
    riak_core_vnode_master:make_request(Request,
                                        {fsm, undefined, self()},
                                        Index).

get_bucket_option(Type, BucketProps) ->
    case lists:keyfind(Type, 1, BucketProps) of
        {Type, Val} -> Val;
        _ ->
            get_default_bucket_option(Type)
    end.

get_default_bucket_option(Type) ->
    %% NOTE: the call to _type_ is because only bucket types don't
    %% automagically inherit new properties added to riak_kv
    %% https://github.com/nhs-riak/riak_kv/issues/9
    case lists:keyfind(Type, 1, riak_core_bucket_type:defaults()) of
        {Type, Val} ->
            Val;
        _ ->
            throw({unknown_bucket_option, Type})
    end.


expand_value(Type, default, BucketProps) ->
    get_bucket_option(Type, BucketProps);
expand_value(_Type, Value, _BucketProps) ->
    Value.

expand_sync_on_write(default, BucketProps) ->
    normalize_value(get_bucket_option(sync_on_write, BucketProps));
expand_sync_on_write(Value, _BucketProps) ->
    Value.

normalize_value(Val) when is_atom(Val) ->
    Val;
normalize_value(Val) when is_binary(Val) ->
    try
        binary_to_existing_atom(Val, utf8)
    catch _:badarg ->
        error
    end.

expand_rw_value(Type, default, BucketProps, N) ->
    normalize_rw_value(get_bucket_option(Type, BucketProps), N);
expand_rw_value(_Type, Val, _BucketProps, N) ->
    normalize_rw_value(Val, N).

normalize_rw_value(RW, _N) when is_integer(RW) -> RW;
normalize_rw_value(RW, N) when is_binary(RW) ->
    try
        ExistingAtom = binary_to_existing_atom(RW, utf8),
        normalize_rw_value(ExistingAtom, N)
    catch _:badarg ->
        error
    end;
normalize_rw_value(one, _N) -> 1;
normalize_rw_value(quorum, N) -> erlang:trunc((N/2)+1);
normalize_rw_value(all, N) -> N;
normalize_rw_value(_, _) -> error.

-spec consistent_object(binary() | {binary(),binary()}) -> true | false | {error,_}.
consistent_object(Bucket) ->
    case riak_core_bucket:get_bucket(Bucket) of
        Props when is_list(Props) ->
            lists:member({consistent, true}, Props);
        {error, _}=Err ->
            Err
    end.

-spec get_write_once(binary() | {binary(),binary()}) -> true | false | {error,_}.
get_write_once(Bucket) ->
    case riak_core_bucket:get_bucket(Bucket) of
        Props when is_list(Props) ->
            lists:member({write_once, true}, Props);
        {error, _}=Err ->
            Err
    end.

-spec kv_ready() -> boolean().
kv_ready() ->
    lists:member(riak_kv, riak_core_node_watcher:services(node())).

%% @doc
%% Replication services may wait a period on startup to ensure stability before
%% commencing.  Default 60s.  Normally only modified in test.
-spec ngr_initial_timeout() -> pos_integer().
ngr_initial_timeout() ->
    application:get_env(riak_kv, ngr_initial_timeout, 60000).

%% ===================================================================
%% Hashtree token management functions
%% ===================================================================

%% @doc
%% Report the maximum and minimum count of hashtree tokens across all online
%% primary vnodes
-spec report_hashtree_tokens() -> {non_neg_integer(), non_neg_integer()}.
report_hashtree_tokens() ->
    OnlinePrimaries = riak_core_apl:active_owners(riak_kv),
    ReportTokenFun = 
        fun({{P, N}, _T}, {Min, Max}) ->
            HT =
                riak_core_vnode_master:sync_command({P, N},
                                                    report_hashtree_tokens,
                                                    riak_kv_vnode_master),
            {min(Min, HT), max(Max, HT)}
        end,
    lists:foldl(ReportTokenFun, {infinity, 0}, OnlinePrimaries).

%% @doc
%% Reset the hashtree tokens on each online primary to a random integer between
%% the minimum and maximum amount.
-spec reset_hashtree_tokens(non_neg_integer(), non_neg_integer()) -> ok.
reset_hashtree_tokens(MinToken, MaxToken) when MaxToken >= MinToken ->
    OnlinePrimaries = riak_core_apl:active_owners(riak_kv),
    ResetTokenFun = 
        fun({{P, N}, _T}) ->
            ok =
                riak_core_vnode_master:sync_command({P, N},
                                                    {reset_hashtree_tokens,
                                                        MinToken, MaxToken},
                                                    riak_kv_vnode_master)
        end,
    lists:foreach(ResetTokenFun, OnlinePrimaries),
    ok.


%% ===================================================================
%% Preflist utility functions
%% ===================================================================

%% @doc Given a bucket/key, determine the associated preflist index_n.
-spec get_index_n({binary(), binary()}) -> index_n().
get_index_n({Bucket, Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Index = chashbin:responsible_index(ChashKey, CHBin),
    {Index, N}.

%% @doc Given an index, determine all sibling indices that participate in one
%%      or more preflists with the specified index.
-spec preflist_siblings(index()) -> [index()].
preflist_siblings(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    preflist_siblings(Index, Ring).

%% @doc See {@link preflist_siblings/1}.
-spec preflist_siblings(index(), riak_core_ring()) -> [index()].
preflist_siblings(Index, Ring) ->
    MaxN = determine_max_n(Ring),
    preflist_siblings(Index, MaxN, Ring).

-spec preflist_siblings(index(), pos_integer(), riak_core_ring()) -> [index()].
preflist_siblings(Index, N, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.

-spec responsible_preflists(index()) -> [index_n()].
responsible_preflists(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    responsible_preflists(Index, Ring).

-spec responsible_preflists(index(), riak_core_ring()) -> [index_n()].
responsible_preflists(Index, Ring) ->
    AllN = riak_core_bucket:all_n(Ring),
    responsible_preflists(Index, AllN, Ring).

-spec responsible_preflists(index(), [pos_integer(),...], riak_core_ring())
                           -> [index_n()].
responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
                          responsible_preflists_n(RevIndices, N)
                  end, AllN).

-spec responsible_preflists_n([index()], pos_integer()) -> [index_n()].
responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].

-spec determine_max_n(riak_core_ring()) -> pos_integer().
determine_max_n(Ring) ->
    lists:max(riak_core_bucket:all_n(Ring)).


fix_incorrect_index_entries() ->
    fix_incorrect_index_entries([]).

fix_incorrect_index_entries(Opts) when is_list(Opts) ->
    MaxN = proplists:get_value(concurrency, Opts, 2),
    ForUpgrade = not proplists:get_value(downgrade, Opts, false),
    BatchSize = proplists:get_value(batch_size, Opts, 100),
    ?LOG_INFO("index reformat: starting with concurrency: ~p, batch size: ~p, for upgrade: ~p",
               [MaxN, BatchSize, ForUpgrade]),
    IdxList = [Idx || {riak_kv_vnode, Idx, _} <- riak_core_vnode_manager:all_vnodes()],
    FixOpts = [{batch_size, BatchSize}, {downgrade, not ForUpgrade}],
    F = fun(X) -> fix_incorrect_index_entries(X, FixOpts) end,
    Counts = riak_core_util:pmap(F, IdxList, MaxN),
    {SuccessCounts, IgnoredCounts, ErrorCounts} = lists:unzip3(Counts),
    SuccessTotal = lists:sum(SuccessCounts),
    IgnoredTotal = lists:sum(IgnoredCounts),
    ErrorTotal = lists:sum(ErrorCounts),
    case ErrorTotal of
        0 ->
            ?LOG_INFO("index reformat: complete on all partitions. Fixed: ~p, Ignored: ~p",
                       [SuccessTotal, IgnoredTotal]);
        _ ->
            ?LOG_INFO("index reformat: encountered ~p errors reformatting keys. Please re-run",
                       [ErrorTotal])
    end,
    {SuccessTotal, IgnoredTotal, ErrorTotal}.

fix_incorrect_index_entries(Idx, FixOpts) ->
    fix_incorrect_index_entries(Idx, fun fix_incorrect_index_entry/4, {0, 0, 0}, FixOpts).

fix_incorrect_index_entries(Idx, FixFun, Acc0, FixOpts) ->
    Ref = make_ref(),
    ForUpgrade = not proplists:get_value(downgrade, FixOpts, false),
    ?LOG_INFO("index reformat: querying partition ~p for index entries to reformat", [Idx]),
    riak_core_vnode_master:command({Idx, node()},
                                   {get_index_entries, FixOpts},
                                   {raw, Ref, self()},
                                   riak_kv_vnode_master),
    case process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, Acc0) of
        ignore ->
                Acc0;
        {_,_,ErrorCount}=Res ->
            _ = mark_indexes_reformatted(Idx, ErrorCount, ForUpgrade),
            Res
    end.

fix_incorrect_index_entry(Idx, ForUpgrade, BadKeys, {Success, Ignore, Error}) ->
    Res = riak_core_vnode_master:sync_command({Idx, node()},
                                              {fix_incorrect_index_entry, BadKeys, ForUpgrade},
                                              riak_kv_vnode_master),
    case Res of
        ok ->
            {Success+1, Ignore, Error};
        ignore ->
            {Success, Ignore+1, Error};
        {error, _} ->
            {Success, Ignore, Error+1};
        {S, I, E} ->
            {Success+S, Ignore+I, Error+E}
    end.

%% needs to take an acc to count success/error/ignore
process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, {S, I, E} = Acc) ->
    receive
        {Ref, {error, Reason}} ->
            ?LOG_ERROR("index reformat: error on partition ~p: ~p", [Idx, Reason]),
            {S, I, E+1};
        {Ref, ignore} ->
            ?LOG_INFO("index reformat: ignoring partition ~p", [Idx]),
            ignore;
        {Ref, done} ->
            ?LOG_INFO("index reformat: finished with partition ~p, Fixed=~p, Ignored=~p, Errors=~p", [Idx, S, I, E]),
            Acc;
        {Ref, {Pid, BatchRef, Keys}} ->
            {NS, NI, NE} = NextAcc = FixFun(Idx, ForUpgrade, Keys, Acc),
            ReportN = 10000,
            case ((NS+NI+NE) div ReportN) /= ((S+I+E) div ReportN) of
               true ->
                    ?LOG_INFO("index reformat: reformatting partition ~p, Fixed=~p, Ignore=~p, Error=~p", [Idx, NS, NI, NE]);
                false ->
                    ok
            end,
            ack_incorrect_keys(Pid, BatchRef),
            process_incorrect_index_entries(Ref, Idx, ForUpgrade, FixFun, NextAcc)
    after
        120000 ->
            ?LOG_ERROR("index reformat: timed out waiting for response from partition ~p",
                        [Idx]),
            {S, I, E+1}
    end.

ack_incorrect_keys(Pid, Ref) ->
    Pid ! {ack_keys, Ref}.

mark_indexes_reformatted(Idx, 0, ForUpgrade) ->
    riak_core_vnode_master:sync_command({Idx, node()},
                                        {fix_incorrect_index_entry, {done, ForUpgrade}},
                                        riak_kv_vnode_master),
    ?LOG_INFO("index reformat: marked partition ~p as fixed", [Idx]),
    ok;
mark_indexes_reformatted(_Idx, _ErrorCount, _ForUpgrade) ->
    undefined.

-ifndef(old_hash).
md5(Bin) ->
    crypto:hash(md5, Bin).
-else.
md5(Bin) ->
    crypto:md5(Bin).
-endif.

%% @doc vtag creation function
-spec make_vtag(erlang:timestamp()) -> list().
make_vtag(Now) ->
    <<HashAsNum:128/integer>> = md5(term_to_binary({node(), Now})),
    riak_core_util:integer_to_list(HashAsNum,62).

overload_reply({raw, ReqId, Pid}) ->
    Pid ! {ReqId, {error, overload}};
overload_reply(_) ->
    ok.

puts_active() ->
    sidejob_resource_stats:usage(riak_kv_put_fsm_sj).

exact_puts_active() ->
    length(sidejob_supervisor:which_children(riak_kv_put_fsm_sj)).

gets_active() ->
    sidejob_resource_stats:usage(riak_kv_get_fsm_sj).

%% @doc Get backend config for backends without an associated application
%% eg, yessir, memory
get_backend_config(Key, Config, Category) ->
    case proplists:get_value(Key, Config) of
        undefined ->
            case proplists:get_value(Category, Config) of
                undefined ->
                    undefined;
                InnerConfig ->
                    proplists:get_value(Key, InnerConfig)
            end;
        Val ->
            Val
    end.

%% @doc Is the Module/Function from a mapreduce {modfun, ...} tuple allowed by
%% the security rules? This is to help prevent against attacks like the one
%% described in
%% http://aphyr.com/posts/224-do-not-expose-riak-directly-to-the-internet
%% by whitelisting the code path for 'allowed' mapreduce modules, which we
%% assume the user has written securely.
is_modfun_allowed(riak_kv_mapreduce, _) ->
    %% these are common mapreduce helpers, provided by riak KV, we trust them
    true;
is_modfun_allowed(Mod, _Fun) ->
    case riak_core_security:is_enabled() of
        true ->
            Paths = [filename:absname(N)
                     || N <- app_helper:get_env(riak_kv, add_paths, [])],
            case code:which(Mod) of
                non_existing ->
                    {error, {non_existing, Mod}};
                Path when is_list(Path) ->
                    %% ensure that the module is in one of the paths
                    %% explicitly configured for third party code
                    case lists:member(filename:dirname(Path), Paths) of
                        true ->
                            true;
                        _ ->
                            {error, {insecure_module_path, Path}}
                    end;
                Reason ->
                    {error, {illegal_module, Mod, Reason}}
            end;
        _ ->
            true
    end.


-spec shuffle_list(list()) -> list().
shuffle_list(L) ->
    lists:map(fun({_R, X0}) -> X0 end,
        lists:keysort(1, lists:map(fun(X) -> {rand:uniform(), X} end, L))).


%% ===================================================================
%% Troubleshooting functions
%% ===================================================================

%% Note that recon is also available
%% https://ferd.github.io/recon/overview.html
%% Useful functions
%% recon_alloc:fragementation(current) - current state of fragmentation by
%% allocator, with worse allocators higher in the list
%% recon:bin_leak(N) - Top N processes with binary references cleared by GC


%% @doc top_n_binary_memory/2
%% Look at all processes on the node, and return them by Top N of total binary
%% memory size.  Returns sorted results {P, IC, Count, Sum} - where P is the
%% pid of the process, IC is the initial call that started the process, Count
%% is the number of references, and Sum is the total amout of memory.
top_n_binary_total_memory(N) ->
    BinSums =
        lists:map(
            fun(P) ->
                case process_info(P, binary) of
                    {binary, BinList} ->
                        {P,
                            length(BinList),
                            lists:sum(
                                lists:map(
                                    fun(BR) -> element(2, BR) end,
                                    BinList
                                )
                            )
                        };
                    _ ->
                        {P, 0, 0}
                    end
                end,
                erlang:processes()
            ),
    lists:map(
        fun({P, BC, BS}) ->
            {P, get_initial_call(P), BC, BS}
        end,
        lists:sublist(lists:reverse(lists:keysort(3, BinSums)), N)
    ).

%% @doc top_n_binary_memory/2
%% Look at all processes on the node, and return them by Top N of total process
%% memory size.
top_n_process_total_memory(N) ->
    MemoryMap =
        lists:map(
            fun(P) ->
                case process_info(P, memory) of
                    {memory, MemSz} ->
                        {P, MemSz};
                    _ ->
                        {P, 0}
                end
            end,
            erlang:processes()
        ),
    lists:map(
        fun({P, MS}) ->
            {P, get_initial_call(P), MS}
        end,
        lists:sublist(lists:reverse(lists:keysort(2, MemoryMap)), N)
    ).

%% @doc summarise_binary_memory_by_initial_call/1
%% Takes the output of a call to top_n_binary_total_memory, and summarises by
%% initial call - returning, for each initial call the count of PIDs with that
%% initial call, the total memory and a map of reference counts to total memory
summarise_binary_memory_by_initial_call(N) when is_integer(N) ->
    summarise_binary_memory_by_initial_call(top_n_binary_total_memory(N));
summarise_binary_memory_by_initial_call(TopN) when is_list(TopN) ->
    InitialCallMap =
        lists:foldl(
            fun({P, PIC, _BC, _BS}, Acc) ->
                case process_info(P, binary) of
                    {binary, BinList} ->
                        {PidCnt, SzAzz, RCMap} =
                            lists:foldl(
                                fun({_Ref, Sz, RC}, {PidAcc, SzAcc, MapAcc}) ->
                                    {InnerAccCt, InnerAccSz} =
                                        maps:get(RC, MapAcc, {0, 0}),
                                    {PidAcc,
                                        SzAcc + Sz,
                                        maps:put(
                                            RC,
                                            {InnerAccCt + 1, InnerAccSz + Sz},
                                            MapAcc
                                        )
                                    }
                                end,
                                maps:get(PIC, Acc, {1, 0, maps:new()}),
                                BinList
                            ),
                        maps:put(PIC, {PidCnt, SzAzz, RCMap}, Acc);
                    _ ->
                        Acc
                end
            end,
            maps:new(),
            TopN
        ),
    lists:reverse(
        lists:keysort(
            3,
            lists:map(
                fun({PIC, {Cnt, Sz, RCMap}}) -> {PIC, Cnt, Sz, RCMap} end,
                maps:to_list(InitialCallMap)
            )
        )
    ).

%% @doc summarise_process_memory_by_initial_call/1
%% Takes the output of a call to top_n_process_total_memory, and summarises by
%% initial call - returning, for each initial call the count of PIDs with that
%% initial call, and the total process memory
summarise_process_memory_by_initial_call(N) when is_integer(N) ->
    summarise_process_memory_by_initial_call(top_n_process_total_memory(N));
summarise_process_memory_by_initial_call(TopN) when is_list(TopN) ->
    MemoryMap =
        lists:foldl(
            fun({_P, IC, MemSz}, Acc) ->
                {AccCnt, AccSz} = maps:get(IC, Acc, {0, 0}),
                maps:put(
                    IC,
                    {AccCnt + 1, AccSz + MemSz},
                    Acc
                )
            end,
            maps:new(),
            TopN
        ),
    lists:reverse(
        lists:keysort(
            3,
            lists:map(
                fun({PIC, {Cnt, Sz}}) -> {PIC, Cnt, Sz} end,
                maps:to_list(MemoryMap)
            )
        )
    ).

%% @doc profile_riak/1
%% Run eprof for ProfileTime milliseconds.  Will have an impact, so normally
%% best to restrict ProfileTime to 100ms.  May fail on systems under heavy load
-spec profile_riak(pos_integer()) -> analyzed|failed.
profile_riak(ProfileTime) ->
    eprof:start(),
    case eprof:start_profiling(erlang:processes()) of
        profiling ->
            timer:sleep(ProfileTime),
            case eprof:stop_profiling() of
                profiling_stopped ->
                    eprof:analyze(
                        total, [{filter, [{time, float(10 * ProfileTime)}]}]
                    ),
                    stopped = eprof:stop(),
                    analyzed;
                _ ->
                    stopped = eprof:stop(),
                    failed_running
            end;
        _ ->
            failed_starting
    end.

%% @doc get_initial_Call/1
%% To be used in map functions - reliably either returns the initial call from
%% process dictionary, or undefined
get_initial_call(P) ->
    case process_info(P, dictionary) of
        {dictionary, DKV} ->
            case lists:keyfind('$initial_call', 1, DKV) of
                false ->
                    undefined;
                {'$initial_call', Call} ->
                    Call
            end;
        _ ->
            undefined
    end.

%% @doc sys_monitor_count/0
%% Count up all monitors, unfortunately has to obtain process_info
%% from all processes to work it out.
sys_monitor_count() ->
    lists:foldl(
        fun(Pid, Count) ->
                case erlang:process_info(Pid, monitors) of
                    {monitors, Mons} ->
                        Count + length(Mons);
                    _ ->
                        Count
                end
        end,
        0, processes()
    ).


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

normalize_test() ->
    3 = normalize_rw_value(3, 3),
    1 = normalize_rw_value(one, 3),
    2 = normalize_rw_value(quorum, 3),
    3 = normalize_rw_value(all, 3),
    1 = normalize_rw_value(<<"one">>, 3),
    2 = normalize_rw_value(<<"quorum">>, 3),
    3 = normalize_rw_value(<<"all">>, 3),
    error = normalize_rw_value(garbage, 3),
    error = normalize_rw_value(<<"garbage">>, 3).


deleted_test() ->
    O = riak_object:new(<<"test">>, <<"k">>, "v"),
    false = is_x_deleted(O),
    MD = dict:new(),
    O1 = riak_object:apply_updates(
           riak_object:update_metadata(
             O, dict:store(<<"X-Riak-Deleted">>, true, MD))),
    true = is_x_deleted(O1).

make_vtag_test() ->
    crypto:start(),
    ?assertNot(make_vtag(os:timestamp()) =:=
               make_vtag(os:timestamp())).

-endif.
