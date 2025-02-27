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

%% @doc
%% This module implements a gen_server process that manages a set of hashtrees
%% (see {@link //riak_core/hashtree}) containing key/hash pairs for all data owned by a
%% given partition. Each riak_kv vnode spawns its own index_hashtree. These
%% hashtrees are used for active anti-entropy exchange between vnodes.

-module(riak_kv_index_hashtree).
-behaviour(gen_server).

-include_lib("riak_kv_vnode.hrl").

%% API
-export([start/3, start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([get_lock/2,
         get_lock/3,
         get_lock_and_version/2,
         compare/3,
         compare/4,
         compare/5,
         determine_data_root/0,
         exchange_bucket/4,
         exchange_segment/3,
         estimate_keys/1,
         estimate_keys/2,
         hash_index_data/1,
         hash_object/3,
         update/2,
         update/3,
         start_exchange_remote/5,
         delete/2,
         async_delete/2,
         insert/3,
         async_insert/3,
         stop/1,
         sync_stop/1,
         clear/1,
         expire/1,
         destroy/1,
         index_2i_n/0,
         get_trees/1,
         get_version/1]).

-export([poke/1,
         get_build_time/1,
         handle_corrupted_object/4]).

-include_lib("kernel/include/logger.hrl").

-type index() :: non_neg_integer().
-type index_n() :: {index(), non_neg_integer()}.
-type orddict() :: orddict:orddict().
-type proplist() :: proplists:proplist().
-type riak_object_t2b() :: binary().
-type hashtree() :: hashtree:hashtree().
-type update_callback() :: fun(() -> term()).
-type version() :: legacy | non_neg_integer().
-type delete_item() :: {binary(), binary()}|{object, {binary(), binary()}}.

-record(state, {index,
                vnode_pid,
                built,
                expired :: boolean(),
                lock :: undefined | reference(),
                path,
                build_time,
                trees,
                use_2i = false :: boolean(),
                version = legacy :: version()}).

-type state() :: #state{}.

%% Time from build to expiration of tree, in millseconds
-define(DEFAULT_EXPIRE, 604800000). %% 1 week
%% Magic Tree id for 2i data.
-define(INDEX_2I_N, {0, 0}).

% gen_server call timeout for sync stop
-define(SYNC_STOP_TIMEOUT, 60000).

%% Throttle used when folding over K/V data to build AAE trees: {Limit, Wait}.
%% After traversing Limit bytes, the fold will sleep for Wait milliseconds.
%% Default: 1 MB limit / 100 ms wait
-define(DEFAULT_BUILD_THROTTLE, {1000000, 100}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawn an index_hashtree process that manages the hashtrees (one
%%      for each `index_n') for the specified partition index.
-spec start(index(), pid(), proplist()) -> {ok, pid()} | {error, term()}.
start(Index, VNPid, Opts) ->
    gen_server:start(?MODULE, [Index, VNPid, Opts], []).

%% @doc Spawn an index_hashtree process that manages the hashtrees (one
%%      for each `index_n') for the specified partition index.
-spec start_link(index(), pid(), proplist()) -> {ok, pid()} | {error, term()}.
start_link(Index, VNPid, Opts) ->
    gen_server:start_link(?MODULE, [Index, VNPid, Opts], []).

%%      Valid options:
%%       ``if_missing'' :: Only insert the key/hash pair if the key does not
%%                         already exist in the hashtree.
insert(Items, _Opts, Tree) when Tree =:= undefined; Items =:= [] ->
    ok;
insert(Items=[_|_], Opts, Tree) ->
    catch gen_server:call(Tree, {insert, Items, Opts}, infinity).

async_insert(Items, _Opts, Tree) when Tree =:= undefined; Items =:= [] ->
    ok;
async_insert(Items=[_|_], Opts, Tree) ->
    gen_server:cast(Tree, {insert, Items, Opts}).

-spec delete([delete_item()], pid()) -> ok.
delete(Items, Tree) when Tree =:= undefined; Items =:= [] ->
    ok;
delete(Items=[{_Id, _Key}|_], Tree) ->
    catch gen_server:call(Tree, {delete, Items}, infinity).

-spec async_delete(delete_item()|[delete_item()], pid()) -> ok.
async_delete(Items, Tree) when Tree =:= undefined; Items =:= [] ->
    ok;
async_delete(Items=[{_Id, _Key}|_], Tree) ->
    catch gen_server:cast(Tree, {delete, Items}).

%% @doc Called by the entropy manager to finish the process used to acquire
%%      remote vnode locks when starting an exchange. For more details,
%%      see {@link riak_kv_entropy_manager:start_exchange_remote/3}
-spec start_exchange_remote(pid(), version(), term(), index_n(), pid()) -> ok.
start_exchange_remote(FsmPid, Version, From, IndexN, Tree) ->
    gen_server:cast(Tree, {start_exchange_remote, FsmPid, Version, From, IndexN}).

%% @doc Update all hashtrees managed by the provided index_hashtree pid.
-spec update(index_n(), pid()) -> ok | not_responsible.
update(Id, Tree) ->
    update(Id, Tree, undefined).

%% @doc Update all hashtrees managed by the provided index_hashtree pid.
-spec update(index_n(), pid(), undefined | update_callback()) -> ok | not_responsible.
update(Id, Tree, undefined) ->
    gen_server:call(Tree, {update_tree, Id, undefined}, infinity);
update(Id, Tree, Callback) when is_function(Callback) ->
    gen_server:call(Tree, {update_tree, Id, Callback}, infinity).

%% @doc Return a hash bucket from the tree identified by the given tree id
%%      that is managed by the provided index_hashtree.
-spec exchange_bucket(index_n(), integer(), integer(), pid()) -> orddict().
exchange_bucket(Id, Level, Bucket, Tree) ->
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}, infinity).

%% @doc Return a segment from the tree identified by the given tree id that
%%      is managed by the provided index_hashtree.
-spec exchange_segment(index_n(), integer(), pid()) -> orddict().
exchange_segment(Id, Segment, Tree) ->
    gen_server:call(Tree, {exchange_segment, Id, Segment}, infinity).

%% @doc Start the key exchange between a given tree managed by the
%%      provided index_hashtree and a remote tree accessed through the
%%      provided remote function.
-spec compare(index_n(), hashtree:remote_fun(), pid()) -> [hashtree:keydiff()].
compare(Id, Remote, Tree) ->
    compare(Id, Remote, undefined, Tree).

%% @doc A variant of {@link compare/3} that takes a key difference accumulator
%%      function as an additional parameter.
-spec compare(index_n(), hashtree:remote_fun(),
              undefined | hashtree:acc_fun(T), pid()) -> T.
compare(Id, Remote, AccFun, Tree) ->
    compare(Id, Remote, AccFun, [], Tree).

%% @doc A variant of {@link compare/3} that takes a key difference accumulator
%%      function as an additional parameter.
-spec compare(index_n(), hashtree:remote_fun(),
              undefined | hashtree:acc_fun(T), any(), pid()) -> T.
compare(Id, Remote, AccFun, Acc, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote, AccFun, Acc}, infinity).

%% @doc For testing only, retrieve the hashtree data structures. It is
%% not safe to tamper with these structures due to the LevelDB backend.
get_trees({test, Pid}) ->
    gen_server:call(Pid, get_trees, infinity).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the calling process.
-spec get_lock(pid(), any()) -> ok | not_built | already_locked.
get_lock(Tree, Type) ->
    get_lock(Tree, Type, get_version(Tree), self()).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the calling process. Grab lock on
%%      specific version.
-spec get_lock(pid(), any(), version()) -> ok | not_built | already_locked | bad_version.
get_lock(Tree, Type, Version) ->
    get_lock(Tree, Type, Version, self()).

%% @doc Acquire the lock for the specified index_hashtree if not already
%%      locked, and associate the lock with the provided pid.
-spec get_lock(pid(), any(),version(), pid()) -> ok | not_built | already_locked | bad_version.
get_lock(Tree, Type, Version, Pid) ->
    gen_server:call(Tree, {get_lock, Type, Version, Pid}, infinity).

%% @doc Get the version of the specified index_hashtree
-spec get_version(pid()) -> version().
get_version(Tree) ->
    gen_server:call(Tree, get_version, infinity).

%% @doc Acquire the lock and return the version for the specified index_hashtree if not already
%%      locked.
-spec get_lock_and_version(pid(), any()) -> {ok | not_built | already_locked, version()}.
get_lock_and_version(Tree, Type) ->
    {get_lock(Tree, Type) , get_version(Tree)}.

%% @doc Poke the specified index_hashtree to ensure the tree is
%%      built/rebuilt as needed. This is periodically called by the
%%      {@link riak_kv_entropy_manager}.
-spec poke(pid()) -> ok.
poke(Tree) ->
    gen_server:cast(Tree, poke).

%% @doc Terminate the specified index_hashtree.
stop(Tree) ->
    gen_server:cast(Tree, stop).

%% @doc Terminate the specified index_hashtree and wait for exit
sync_stop(undefined) ->
    ok;
sync_stop(Tree) ->
    gen_server:call(Tree, stop, ?SYNC_STOP_TIMEOUT).

%% @doc Destroy the specified index_hashtree, which will destroy all
%%      associated hashtrees and terminate.
-spec destroy(pid()) -> ok.
destroy(Tree) ->
    gen_server:call(Tree, destroy, infinity).

%% @doc Clear the specified index_hashtree, clearing all associated hashtrees
clear(Tree) ->
    gen_server:call(Tree, clear, infinity).

%% @doc Expire the specified index_hashtree
expire(Tree) ->
    gen_server:cast(Tree, expire).

%% @doc Estimate total number of keys in index_hashtree
estimate_keys(Tree) ->
    gen_server:call(Tree, estimate_keys, infinity).

%% @doc Estimate total number of keys in index_hashtree
estimate_keys(Tree, IndexN) ->
    gen_server:call(Tree, {estimate_keys, IndexN}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index, VNPid, Opts]) ->
    case determine_data_root() of
        undefined ->
            case riak_kv_entropy_manager:enabled() of
                true ->
                    ?LOG_WARNING("Neither riak_kv/anti_entropy_data_dir or "
                                  "riak_core/platform_data_dir are defined. "
                                  "Disabling active anti-entropy."),
                    riak_kv_entropy_manager:disable();
                false ->
                    ok
            end,
            ignore;
        Root ->
            {Path0, Version} = case determine_version(Root, Index, Opts) of
                V when is_integer(V) ->
                    %% Must add "v" because integer partition dirs. Joining the version to the path to support
                    %% easy downgrades where the new trees will be unable to be found by old code.
                    {filename:join(Root, "v" ++ integer_to_list(V)), V};
                _ ->
                    {Root, legacy}
            end,
            Path = filename:join(Path0, integer_to_list(Index)),
            monitor(process, VNPid),
            Use2i = lists:member(use_2i, Opts),
            VNEmpty = lists:member(vnode_empty, Opts),
            State = #state{index=Index,
                           vnode_pid=VNPid,
                           trees=orddict:new(),
                           built=false,
                           use_2i=Use2i,
                           path=Path,
                           version=Version,
                           expired = false},
            IndexNs = responsible_preflists(State),
            State2 = init_trees(IndexNs, VNEmpty, State),
            %% If vnode is empty, mark tree as built without performing fold
            case VNEmpty of
                true ->
                    ?LOG_DEBUG("Built empty AAE tree for ~p", [Index]),
                    gen_server:cast(self(), build_finished);
                _ ->
                    ok
            end,
            {ok, State2}
    end.

handle_call({new_tree, Id}, _From, State) ->
    State2 = do_new_tree(Id, State, mark_open),
    {reply, ok, State2};

handle_call({get_lock, Type, Version, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Version, Pid, State),
    {reply, Reply, State2};

handle_call(get_version, _From, State=#state{version=Version}) ->
    {reply, Version, State};

handle_call({insert, Items, Options}, _From, State) ->
    State2 = do_insert(Items, Options, State),
    {reply, ok, State2};

handle_call({delete, Items}, _From, State) ->
    State2 = do_delete(Items, State),
    {reply, ok, State2};

handle_call(get_trees, _From, #state{trees=Trees}=State) ->
    {reply, Trees, State};

handle_call({update_tree, Id, Callback}, From, State) ->
    ?LOG_DEBUG("Updating tree: (vnode)=~p (preflist)=~p", [State#state.index, Id]),
    apply_tree(Id,
        fun(Tree) ->
            NewTree = snapshot_and_async_update_tree(Tree, Id, From, Callback),
            {noreply, NewTree}
            end,
        State
    );

handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       Result = hashtree:get_bucket(Level, Bucket, Tree),
                       {Result, Tree}
               end,
               State);

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                       {Result, Tree}
               end,
               State);

handle_call({compare, Id, Remote, AccFun, Acc}, From, State) ->
    do_compare(Id, Remote, AccFun, Acc, From, State),
    {noreply, State};

handle_call(destroy, _From, State) ->
    State2 = destroy_trees(State),
    {stop, normal, ok, State2};

handle_call(clear, _From, State) ->
    State2 = clear_tree(State),
    {reply, ok, State2};

handle_call(estimate_keys, _From,  State=#state{trees=Trees}) ->
    EstimateNrKeys =
        orddict:fold(fun(_, Tree, Acc) ->
                             {ok, Value} = hashtree:estimate_keys(Tree) ,
                             Value + Acc
                     end,
                     0, Trees),
    {reply, {ok, EstimateNrKeys}, State};

handle_call({estimate_keys, IndexN}, _From,  State=#state{trees=Trees}) ->
    case orddict:find(IndexN, Trees) of
        {ok, Tree} ->
            {ok, EstimateNrKeys} = hashtree:estimate_keys(Tree),
            {reply, {ok, EstimateNrKeys}, State};
        error ->
            {reply, not_responsible, State}
    end;

handle_call(stop, _From, State0) ->
    State1 = close_trees(State0),
    {stop, normal, ok, State1};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(poke, State) ->
    State2 = do_poke(State),
    {noreply, State2};

handle_cast(stop, State0) ->
    State1 = close_trees(State0),
    {stop, normal, State1};

handle_cast({insert, Items, Options}, State) ->
    State2 = do_insert(Items, Options, State),
    {noreply, State2};

handle_cast({delete, Items}, State) ->
    State2 = do_delete(Items, State),
    {noreply, State2};

handle_cast(build_failed, State) ->
    riak_kv_entropy_manager:requeue_poke(State#state.index),
    State2 = State#state{built=false},
    {noreply, State2};
handle_cast(build_finished, State) ->
    State2 = do_build_finished(State),
    {noreply, State2};

handle_cast(expire, State) ->
    State2 = State#state{expired=true},
    ?LOG_INFO("Manually expired tree: ~p", [State#state.index]),
    {noreply, State2};

handle_cast({start_exchange_remote, FsmPid, Version, From, _IndexN}, State) ->
    %% Concurrency lock already acquired, try to acquire tree lock.
    case do_get_lock(remote_fsm, Version, FsmPid, State) of
        {ok, State2} ->
            gen_server:reply(From, {remote_exchange, self()}),
            {noreply, State2};
        {Reply, State2} ->
            gen_server:reply(From, {remote_exchange, Reply}),
            {noreply, State2}
    end;

handle_cast({updated, Id}, State) ->
    Fun = fun(Tree) ->
              {noreply, hashtree:set_next_rebuild(Tree, incremental)}
          end,
    apply_tree(Id, Fun, State);

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, _, Pid, Reason}, State0=#state{trees=Trees0, vnode_pid=VnodePid})
  when Pid == VnodePid ->
    %% vnode has terminated, exit as well
    %% exiting as quickly as possible to reduce the window on the vnode restart
    %% and force full rebuild for next restart
    Trees = case Reason of
                normal ->
                    Trees0;
                shutdown ->
                    Trees0;
                _ ->
                    [hashtree:set_next_rebuild(Tree, full) || {_IdxN, Tree} <- Trees0]
            end,
    State1 = close_trees(State0#state{trees=Trees}),
    {stop, normal, State1};
handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_trees(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

determine_data_root() ->
    case application:get_env(riak_kv, anti_entropy_data_dir) of
        {ok, EntropyRoot} ->
            EntropyRoot;
        undefined ->
            case application:get_env(riak_core, platform_data_dir) of
                {ok, PlatformRoot} ->
                    Root = filename:join(PlatformRoot, "anti_entropy"),
                    ?LOG_WARNING("Config riak_kv/anti_entropy_data_dir is "
                                  "missing. Defaulting to: ~p", [Root]),
                    application:set_env(riak_kv, anti_entropy_data_dir, Root),
                    Root;
                undefined ->
                    undefined
            end
    end.

%% @doc Determine the version this tree should use at startup. If update atom is in Opts
%%      then we immediately use version in capabilities. Otherwise check if capabilities
%%      has flipped to a version yet and if it has, check if we've already upgraded by
%%      looking for versioned AAE directory.
-spec determine_version(list(), index(), list()) -> version().
determine_version(Root, Index, Opts) ->
    case force_upgrade(Opts) of
        true ->
            get_cap_hash_version();
        _ ->
            find_version(Root, Index)
    end.

-spec force_upgrade(list()) -> boolean().
force_upgrade(Opts) ->
    check_upgrade_opts(Opts, check_upgrade_env()).

-spec check_upgrade_opts(list(), boolean()) -> boolean().
check_upgrade_opts(_Opts, true) ->
    true;
check_upgrade_opts(Opts, _) ->
    lists:member(upgrade, Opts).

-spec check_upgrade_env() -> boolean().
check_upgrade_env() ->
    case application:get_env(riak_kv, force_hashtree_upgrade, false) of
        true ->
            true;
        false ->
            false;
        Value ->
            ?LOG_ERROR("Unsupported non-boolean value for environment variable force_hashtree_upgrade ~p",[Value]),
            false
    end.

-spec get_cap_hash_version() -> version().
get_cap_hash_version() ->
    riak_core_capability:get({riak_kv, object_hash_version}, legacy).

-spec find_version(list(), index()) -> version().
find_version(Root, Index) ->
    check_root_version(Root, Index, get_cap_hash_version()).

-spec check_root_version(list(), index(), version()) -> version().
check_root_version(Root, Index, Version) when is_integer(Version) ->
    case filelib:is_dir(filename:join(filename:join(Root, "v" ++ integer_to_list(Version)),integer_to_list(Index))) of
        true ->
            Version;
        false ->
            legacy
    end;
check_root_version(_Root, _Index, Version) ->
    Version.

%% @doc Init the trees.
%%
%%      MarkEmpty is a boolean dictating whether we're marking the tree empty for the
%%      the first creation or just marking it open instead.
-spec init_trees([index_n()], boolean(), state()) -> state().
init_trees(IndexNs, MarkEmpty, State) ->
    State2 = lists:foldl(
               fun(Id, StateAcc) ->
                   case MarkEmpty of
                       true  -> do_new_tree(Id, StateAcc, mark_empty);
                       false -> do_new_tree(Id, StateAcc, mark_open)
                   end
               end, State, IndexNs),
    State2#state{built=false, expired=false}.

-spec load_built(state()) -> boolean().
load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} ->
            true;
        _ ->
            false
    end.

%% Generate a hash value for a `riak_object'
-spec hash_object(
    {riak_object:bucket(), riak_object:key()},
    riak_object_t2b() | riak_object:riak_object() | riak_object:proxy_object(),
    version()) -> binary().
hash_object({Bucket, Key}, RObj, Version) ->
    riak_object:hash(Bucket, Key, RObj, Version).


hash_index_data(IndexData) when is_list(IndexData) ->
    Bin = term_to_binary(lists:usort(IndexData)),
    riak_core_util:sha(Bin).

%% Fold over a given vnode's data, inserting each object into the appropriate
%% hashtree. Use the `if_missing' option to only insert the key/hash pair if
%% the key does not already exist in the tree. This allows real-time updates
%% to the hashtree to occur concurrently with the fold. For example, if an
%% incoming write triggers a real-time insert of a key/hash pair for an object
%% before the fold reaches the now out-of-date version of the object, the old
%% key/hash pair will be ignored.
%% If `HasIndexTree` is true, also update the index spec tree.
-spec fold_keys(index(), pid(), partition(), boolean()) -> ok.
fold_keys(Partition, HashtreePid, Index, HasIndexTree) ->
    Version = get_version(HashtreePid),
    FoldFun = fold_fun(HashtreePid, HasIndexTree),
    {Limit, Wait} = get_build_throttle(),
    ?LOG_INFO("Making fold request to reconstruct AAE tree idx=~p"
                            ++ " with version ~w", 
                [Partition, Version]),
    Opts = 
        case Version of 
            legacy ->
                [aae_reconstruction, {iterator_refresh, true}];
            _ ->
                [aae_reconstruction, {iterator_refresh, true}, fold_heads]
        end,
    Req =
        riak_core_util:make_fold_req(FoldFun, {0, {Limit, Wait}}, false, Opts),
    Result =
        riak_core_vnode_master:sync_command({Partition, node()},
                                            Req,
                                            riak_kv_vnode_master,
                                            infinity),
    handle_fold_keys_result(Result, HashtreePid, Index).


%% The accumulator in the fold is the number of bytes hashed
%% modulo the "build limit" size. If we get an int back, everything is ok
handle_fold_keys_result({Result, {Limit, Delay}}, HashtreePid, Index) 
                                                when is_integer(Result) ->
    ?LOG_INFO("Finished AAE tree build idx=~p limit ~w delay ~w", 
                    [Index, Limit, Delay]),
    gen_server:cast(HashtreePid, build_finished);
handle_fold_keys_result(Result, HashtreePid, Index) ->
    ?LOG_ERROR("Failed to build hashtree for idx=~p. Result was: ~p", 
                    [Index, Result]),
    gen_server:cast(HashtreePid, build_failed).

get_build_throttle() ->
    app_helper:get_env(riak_kv,
                       anti_entropy_build_throttle,
                       ?DEFAULT_BUILD_THROTTLE).

maybe_throttle_build(RObjBin, Limit, Wait, Acc) ->
    ObjSize = byte_size(RObjBin),
    Acc2 = Acc + ObjSize,
    if (Limit =/= 0) andalso (Acc2 > Limit) ->
            ?LOG_DEBUG("Throttling AAE build for ~b ms", [Wait]),
            timer:sleep(Wait),
            NewLimit = get_build_throttle(),
            {0, NewLimit};
       true ->
            {Acc2, {Limit, Wait}}
    end.

-type hashtree_fold_fun() ::
    fun((
        {riak_object:bucket(), riak_object:key()}, riak_object_t2b(),
        {non_neg_integer(), {non_neg_integer(), non_neg_integer()}}) ->
            {non_neg_integer(), {non_neg_integer(), non_neg_integer()}}).

%% @doc Generate the folding function
%% for a riak fold_req
-spec fold_fun(pid(), boolean()) -> hashtree_fold_fun().
fold_fun(HashtreePid, _HasIndexTree = false) ->
    ObjectFoldFun = object_fold_fun(HashtreePid),
    fun(BKey = {Bucket, Key}, RObj, {Acc, {Limit, Wait}}) ->
        try
            BinBKey = term_to_binary(BKey),
            ObjectFoldFun(BKey, RObj, BinBKey),
            maybe_throttle_build(RObj, Limit, Wait, Acc)
        catch Error:Reason ->
            handle_corrupted_object(Bucket, Key, Error, Reason),
            {Acc, {Limit, Wait}}
        end
    end;
fold_fun(HashtreePid, _HasIndexTree = true) ->
    %% Index AAE backend, so hash the indexes
    ObjectFoldFun = object_fold_fun(HashtreePid),
    IndexFoldFun = index_fold_fun(HashtreePid),
    fun(BKey = {Bucket, Key}, BinObj, {Acc, {Limit, Wait}}) ->
        try
            RObj = riak_object:from_binary(Bucket, Key, BinObj),
            BinBKey = term_to_binary(BKey),
            ObjectFoldFun(BKey, RObj, BinBKey),
            IndexFoldFun(RObj, BinBKey),
            maybe_throttle_build(BinObj, Limit, Wait, Acc)
        catch Error:Reason ->
            handle_corrupted_object(Bucket, Key, Error, Reason),
            {Acc, {Limit, Wait}}
        end
    end.

%% @doc
%% If an object is corrupted, we dont want to include it in the AAE store, and
%% a read repair should be attempted in expectation that this will update the
%% object with an uncorrupted version
-spec handle_corrupted_object(
    riak_object:bucket(), riak_object:key(), term(), term()) -> ok.
handle_corrupted_object(Bucket, Key, Error, Reason) ->
    ?LOG_WARNING(
        "Unable to read B=~p K=~p due to ~p ~p during tree rebuild " ++
        "so will prompt read_repair - ignore warning during shutdown",
        [Bucket, Key, Error, Reason]),
    riak_kv_reader:request_read({Bucket, Key}).

-spec object_fold_fun(pid()) ->
    fun((
        {riak_object:bucket(), riak_object:key()},
        riak_object_t2b() | riak_object:riak_object() | riak_object:proxy_object(),
        binary()) -> ok).
object_fold_fun(HashtreePid) ->
    Version = get_version(HashtreePid),
    fun(BKey={Bucket,Key}, RObj, BinBKey) ->
            IndexN = riak_kv_util:get_index_n({Bucket, Key}),
            insert([{IndexN, BinBKey, hash_object(BKey, RObj, Version)}],
                   [if_missing],
                   HashtreePid)
    end.

-spec index_fold_fun(pid()) ->
    fun((riak_object:riak_object()|riak_object:proxy_object(), binary())
            -> ok).
index_fold_fun(HashtreePid) ->
    fun(RObj, BinBKey) ->
            IndexData = riak_object:index_data(RObj),
            insert([{?INDEX_2I_N, BinBKey, hash_index_data(IndexData)}],
                   [if_missing], HashtreePid)
    end.

%% @doc the 2i index hashtree as a Magic index_n of {0, 0}
-spec index_2i_n() -> ?INDEX_2I_N.
index_2i_n() ->
    ?INDEX_2I_N.

%% @doc Generate a new {@link hashtree} for the specified `index_n'. If this is
%%      the first hashtree created by this index_hashtree, then open/create a new
%%      on-disk store at `segment_path'. Otherwise, re-use the store from the first
%%      tree. In other words, all hashtrees for a given index_hashtree are stored in
%%      the same on-disk store.
%%
%%      Also, write to the tree-id's `opened' meta and check if `opened' =:= `closed'.
-spec do_new_tree(index_n(), state(), mark_open|mark_empty) -> state().
do_new_tree(Id, State=#state{trees=Trees, path=Path}, MarkType) ->
    Index = State#state.index,
    IdBin = tree_id(Id),
    NewTree0 = case Trees of
                  [] ->
                      hashtree:new({Index,IdBin}, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new({Index,IdBin}, Other)
               end,
    NewTree1 = case MarkType of
                   mark_empty -> hashtree:mark_open_empty(Id, NewTree0);
                   mark_open  -> hashtree:mark_open_and_check(Id, NewTree0)
               end,
    Trees2 = orddict:store(Id, NewTree1, Trees),
    State#state{trees=Trees2}.

%% This function never uses the Type field. Unsure why it is part of the API. Maybe was meant to be used
%% by the background manager which could manage tokens based on Type atom. Best guess...
-spec do_get_lock(any(), version(), pid(), state()) -> {not_built | ok | already_locked | bad_version, state()}.
do_get_lock(_, _, _, State) when State#state.built /= true ->
    ?LOG_DEBUG("Not built: ~p :: ~p", [State#state.index, State#state.built]),
    {not_built, State};
do_get_lock(_, _, _, State) when State#state.lock /= undefined ->
    ?LOG_DEBUG("Already locked: ~p", [State#state.index]),
    {already_locked, State};
do_get_lock(_Type, Version, Pid, State=#state{version=Version}) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock=Ref},
    {ok, State2};
do_get_lock(_Type, ReqVer, _Pid, State=#state{version=Version, index=Index}) ->
    ?LOG_DEBUG("Hashtree ~p lock attempted for version: ~p while local tree has version: ~p", [Index, ReqVer, Version]),
    {bad_version, State}.

-spec maybe_release_lock(reference(), state()) -> state().
maybe_release_lock(Ref, State) ->
    case State#state.lock of
        Ref ->
            State#state{lock=undefined};
        _ ->
            State
    end.

%% Utility function for passing a specific hashtree into a provided function
%% and storing the possibly-modified hashtree back in the index_hashtree state.
-spec apply_tree(index_n(),
                 fun((hashtree()) -> {'noreply' | any(), hashtree()}),
                 state())
                -> {'reply', 'not_responsible', state()} |
                   {'reply', any(), state()} |
                   {'noreply', state()}.
apply_tree(Id, Fun, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, State};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State2 = State#state{trees=Trees2},
            case Result of
                noreply ->
                    {noreply, State2};
                _ ->
                    {reply, Result, State2}
            end
    end.

-spec do_build_finished(state()) -> state().
do_build_finished(State=#state{index=Index, built=_Pid, trees=Trees0}) ->
    ?LOG_DEBUG("Finished build: ~p", [Index]),
    Trees = orddict:map(fun(_Id, Tree) ->
                            hashtree:flush_buffer(Tree)
                        end, Trees0),
    {_, Tree0} = hd(Trees),
    BuildTime = get_build_time(Tree0),
    _ = hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    _ = hashtree:write_meta(<<"build_time">>, term_to_binary(BuildTime), Tree0),
    riak_kv_entropy_info:tree_built(Index, BuildTime),
    State#state{built=true, build_time=BuildTime, expired=false, trees=Trees}.

%% Determine the build time for all trees associated with this
%% index. The build time is stored as metadata in the on-disk file. If
%% the tree was rehashed after a restart, this function should return
%% the original build time. If this is a newly created tree (or if the
%% on-disk time is invalid), the function returns the current time.
-spec get_build_time(hashtree()) -> erlang:timestamp().
get_build_time(Tree) ->
    Time = case hashtree:read_meta(<<"build_time">>, Tree) of
               {ok, TimeBin} ->
                   binary_to_term(TimeBin);
               _ ->
                   undefined
           end,
    case valid_time(Time) of
        true ->
            Time;
        false ->
            os:timestamp()
    end.

valid_time({X,Y,Z}) when is_integer(X) and is_integer(Y) and is_integer(Z) ->
    true;
valid_time(_) ->
    false.

do_insert(Items, Opts, State=#state{trees=Trees, version=Version}) ->
    HasIndex = has_index_tree(Trees),
    do_insert_expanded(expand_items(HasIndex, Items, Version), Opts, State).

expand_items(HasIndex, Items, Version) ->
    lists:foldl(fun(I, Acc) ->
                        expand_item(HasIndex, I, Version, Acc)
                end, [], Items).

expand_item(Has2ITree, {object, BKey, RObj}, Version, Others) ->
    IndexN = riak_kv_util:get_index_n(BKey),
    BinBKey = term_to_binary(BKey),
    ObjHash = 
        try
            hash_object(BKey, RObj, Version)
        catch Error:Reason ->
            ?LOG_WARNING("Unhashable object BKey=~p", [BKey]),
            ?LOG_WARNING("Hash failure due to ~w ~w", [Error, Reason]),
            Null = erlang:phash2(<<>>),
            term_to_binary(Null)
        end,
    Item0 = {IndexN, BinBKey, ObjHash},
    case Has2ITree of
        false ->
            [Item0 | Others];
        true ->
            IndexData = riak_object:index_data(RObj),
            Hash2i =  hash_index_data(IndexData),
            [Item0, {?INDEX_2I_N, BinBKey, Hash2i} | Others]
    end;
expand_item(_, Item, _Version, Others) ->
    [Item | Others].

-spec do_insert_expanded([{index_n(), binary(), binary()}], proplist(),
                         state()) -> state().
do_insert_expanded([], _Opts, State) ->
    State;
do_insert_expanded([{Id, Key, Hash}|Rest], Opts, State=#state{trees=Trees}) ->
    State2 =
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            handle_unexpected_key(Id, Key, State)
    end,
    do_insert_expanded(Rest, Opts, State2).

do_delete(Items, State=#state{trees=Trees}) ->
    HasIndex = has_index_tree(Trees),
    do_delete_expanded(expand_delete_items(HasIndex, Items), State).

expand_delete_items(HasIndex, Items) ->
    lists:foldl(fun(I, Acc) ->
                        expand_delete_item(HasIndex, I, Acc)
                end, [], Items).

expand_delete_item(Has2ITree, {object, BKey}, Others) ->
    IndexN = riak_kv_util:get_index_n(BKey),
    BinKey = term_to_binary(BKey),
    Item0 = {IndexN, BinKey},
    case Has2ITree of
        false ->
            [Item0 | Others];
        true ->
            [Item0, {?INDEX_2I_N, BinKey} | Others]
    end.

-spec do_delete_expanded(list(), state()) -> state().
do_delete_expanded([], State) ->
    State;
do_delete_expanded([{Id, Key}|Rest], State=#state{trees=Trees}) ->
    State2 =
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:delete(Key, Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            handle_unexpected_key(Id, Key, State)
    end,
    do_delete_expanded(Rest, State2).

-spec responsible_preflists(#state{}) -> [index_n()].
responsible_preflists(#state{index=Partition, use_2i=Use2i}) ->
    RP = riak_kv_util:responsible_preflists(Partition) ++
    [?INDEX_2I_N || Use2i],
    RP.

-spec handle_unexpected_key(index_n(), binary(), state()) -> state().
handle_unexpected_key(Id, Key, State=#state{index=Partition}) ->
    RP = responsible_preflists(State),
    case lists:member(Id, RP) of
        false ->
            %% The encountered object does not belong to any preflists that
            %% this partition is associated with. Under normal Riak operation,
            %% this should only happen when the `n_val' for an object is
            %% reduced. For example, write an object with N=3, then change N to
            %% 2. There will be an extra replica of the object that is no
            %% longer needed. We should probably just delete these objects, but
            %% to be safe rather than sorry, the first version of AAE simply
            %% ignores these objects.
            %%
            %% TODO: We should probably remove these warnings before final
            %%       release, as reducing N will result in a ton of log/console
            %%       spam.
            %% ?LOG_WARNING("Object ~p encountered during fold over partition "
            %%               "~p, but key does not hash to an index handled by "
            %%               "this partition", [Key, Partition]),
            State;
        true ->
            %% The encountered object belongs to a preflist that is currently
            %% associated with this partition, but was not when the
            %% index_hashtree process was created. This occurs when increasing
            %% the `n_val' for an object. For example, write an object with N=3
            %% and it will map to the index/n preflist `{<index>, 3}'. Increase
            %% N to 4, and the object now maps to preflist '{<index>, 4}' which
            %% may not have an existing hashtree if there were previously no
            %% objects with N=4.
            ?LOG_INFO("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            case State#state.built of
                true ->
                    %% If the tree is already built, clear the tree to trigger
                    %% a rebuild that will re-distribute objects into the
                    %% proper hashtrees based on current N values.
                    ?LOG_INFO("Clearing tree to trigger future rebuild"),
                    clear_tree(State);
                _ ->
                    %% Initialize a new index_n tree to prevent future errors.
                    %% The various hashtrees will likely be inconsistent, with
                    %% some trees containing key/hash pairs that should be in
                    %% other trees (eg. due to a change in N value). This will
                    %% be resolved whenever trees are eventually rebuilt, either
                    %% after normal expiration or after a future unexpected value
                    %% triggers the alternate case clause above.
                    State2 = do_new_tree(Id, State, mark_open),
                    State2
            end
    end.

-spec tree_id(index_n()) -> hashtree:tree_id_bin().
tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

-spec do_compare(index_n(), hashtree:remote_fun(), hashtree:acc_fun(any()),
                 any(), term(), state()) -> ok.
do_compare(Id, Remote, AccFun, Acc, From, State) ->
    case orddict:find(Id, State#state.trees) of
        error ->
            %% This case shouldn't happen, but might as well safely handle it.
            ?LOG_WARNING("Tried to compare nonexistent tree "
                          "(vnode)=~p (preflist)=~p", [State#state.index, Id]),
            gen_server:reply(From, []);
        {ok, Tree} ->
            spawn_link(fun() ->
                               Remote(init, self()),
                               Result = hashtree:compare2(Tree, Remote,
                                                         AccFun, Acc),
                               Remote(final, self()),
                               gen_server:reply(From, Result)
                       end)
    end,
    ok.

-spec do_poke(state()) -> state().
do_poke(State) ->
    % If we need to upgrade - do this.  If there's a rebuild, the effort will
    % be wasted, as the next task will be to upgrade and this will wipe the
    % rebult store clear anyway (straight after going to all that effort of
    % rebuilding it).
    State1 = maybe_upgrade(State),
    State2 = maybe_rebuild(maybe_expire(State1)),
    State3 = maybe_build(State2),
    State3.

-spec maybe_upgrade(state()) -> state().
maybe_upgrade(State=#state{lock=undefined, built=true, version=legacy, index=Index}) ->
    case riak_kv_entropy_manager:get_pending_version() of
        legacy ->
            State;
        0 ->
            case get_all_locks(upgrade, Index, self()) of
                true ->
                    riak_kv_vnode:upgrade_hashtree(Index),
                    State;
                _ ->
                    riak_kv_entropy_manager:requeue_poke(State#state.index),
                    State
            end
    end;
maybe_upgrade(State) ->
    State.

-spec maybe_expire(state()) -> state().
maybe_expire(State=#state{lock=undefined, built=true, expired=false}) ->
    Diff = timer:now_diff(os:timestamp(), State#state.build_time),
    Expire = app_helper:get_env(riak_kv,
                                anti_entropy_expire,
                                ?DEFAULT_EXPIRE),
    %% Need to convert from millsec to microsec
    case (Expire =/= never) andalso (Diff > (Expire * 1000)) of
        true ->
            ?LOG_DEBUG("Tree expired: ~p", [State#state.index]),
            State#state{expired=true};
        false ->
            State
    end;
maybe_expire(State) ->
    State.

-spec clear_tree(state()) -> state().
clear_tree(State=#state{index=Index}) ->
    ?LOG_INFO("Clearing AAE tree: ~p", [Index]),
    IndexNs = responsible_preflists(State),
    State2 = destroy_trees(State),
    ?LOG_INFO("Completed destroy of AAE tree: ~p", [Index]),
    State3 = init_trees(IndexNs, true, State2#state{trees=orddict:new()}),
    ?LOG_INFO("Completed init of AAE tree: ~p", [Index]),
    State3#state{built=false, expired=false}.

destroy_trees(State) ->
    State2 = close_trees(State, true),
    {_,Tree0} = hd(State#state.trees), % deliberately using state with live db ref
    _ = hashtree:destroy(Tree0),
    State2.

-spec maybe_build(state()) -> state().
maybe_build(State=#state{built=false}) ->
    HashtreePid = self(),
    Pid = spawn_link(fun() ->
                             build_or_rehash(HashtreePid, State)
                     end),
    State#state{built=Pid};
maybe_build(State) ->
    %% Already built or build in progress
    State.

%% If the on-disk data is not marked as previously being built, then trigger
%% a fold/build. Otherwise, trigger a rehash to ensure the hashtrees match the
%% current on-disk segments.
-spec build_or_rehash(pid(), state()) -> ok.
build_or_rehash(HashtreePid, State = #state{index =Index}) ->
    BuildOrRehash = determine_build_or_rehash(State),
    Locked = get_all_locks(BuildOrRehash, Index, self()),
    build_or_rehash(HashtreePid, Locked, BuildOrRehash, State).

determine_build_or_rehash(State) ->
    case load_built(State) of
        false -> build;
        true -> rehash
    end.

build_or_rehash(HashtreePid, true, build, #state{index =Index, trees =Trees}) ->
    ?LOG_INFO("Starting AAE tree build: ~p", [Index]),
    fold_keys(Index, HashtreePid, Index, has_index_tree(Trees));
build_or_rehash(HashtreePid, true, rehash, #state{index=Index, trees=Trees}) ->
    ?LOG_DEBUG("Starting AAE tree rehash: ~p", [Index]),
    _ = [hashtree:rehash_tree(T) || {_,T} <- Trees],
    ?LOG_DEBUG("Finished AAE tree rehash: ~p", [Index]),
    gen_server:cast(HashtreePid, build_finished);
build_or_rehash(HashtreePid, false, _Type, _State) ->
    gen_server:cast(HashtreePid, build_failed).

-spec maybe_rebuild(state()) -> state().
maybe_rebuild(State=#state{lock=undefined, built=true, expired=true, index=Index}) ->
    Self = self(),
    Pid = spawn_link(fun() ->
                             receive
                                 {lock, Locked, State2} ->
                                     build_or_rehash(Self, Locked, build, State2);
                                 stop ->
                                     ok
                             end
                     end),
    Locked = get_all_locks(build, Index, Pid),
    case Locked of
        true ->
            State2 = clear_tree(State),
            ?LOG_INFO("Informing process to trigger rebuild of tree: ~p", [Index]),
            Pid ! {lock, Locked, State2},
            State2#state{built=Pid};
        _ ->
            Pid ! stop,
            State
    end;
maybe_rebuild(State) ->
    State.

%% Check if the trees contain the magic index tree
-spec has_index_tree(orddict()) -> boolean().
has_index_tree(Trees) ->
    orddict:is_key(?INDEX_2I_N, Trees).

close_trees(State) ->
    close_trees(State, false).

close_trees(State=#state{trees=undefined}, _WillDestroy) ->
    State;
close_trees(State=#state{trees=Trees}, false) ->
    Trees2 = [begin
                  NewTree = try
                                case hashtree:next_rebuild(Tree) of
                                    %% Not marking close cleanly to avoid the
                                    %% cost of a full rebuild on shutdown.
                                    full ->
                                        ?LOG_INFO("Deliberately marking KV hashtree ~p"
                                                   ++ " for full rebuild on next restart",
                                                   [IdxN]),
                                        hashtree:flush_buffer(Tree);
                                    incremental ->
                                        HT = hashtree:update_tree(Tree),
                                        hashtree:mark_clean_close(IdxN, HT)
                                end
                            catch _:Err ->
                                    ?LOG_WARNING("Failed to flush/update trees"
                                                  ++ " during close | Error: ~p", [Err]),
                                    Tree
                            end,
                  {IdxN, NewTree}
              end || {IdxN, Tree} <- Trees],
    really_close_trees(Trees2, State);

close_trees(#state{trees=Trees} = State, true) ->
    really_close_trees(Trees, State).

really_close_trees(Trees, State) ->
    lists:foreach(fun really_close_tree/1, Trees),
    State#state{trees = undefined}.

really_close_tree({_IdxN, Tree}) -> hashtree:close(Tree).

-spec get_all_locks(build | rehash | upgrade, index(), pid()) -> boolean().
get_all_locks(Type, Index, Pid) ->
    try riak_kv_entropy_manager:get_lock(Type, Pid) of
        ok ->
            case maybe_get_vnode_lock(Type, Index, Pid) of
                ok ->
                    true;
                _ ->
                    false
            end;
        Other ->
            ?LOG_DEBUG("Could not get lock: ~p", [Other]),
            false
    catch exit:{timeout,_} ->
        riak_kv_entropy_manager:release_lock(Pid),
        ?LOG_DEBUG("Could not get lock due to timeout."),
        false
    end.

maybe_get_vnode_lock(rehash, _Partition, _Pid) ->
    %% rehash operations do not need a vnode lock
    ok;
maybe_get_vnode_lock(upgrade, _Partition, _Pid) ->
    %% upgrade operations do not need a vnode lock
    %% The subsequent build following the hashtree
    %% restart will trigger a build and get a vnode_lock
    ok;
maybe_get_vnode_lock(build, Partition, Pid) ->
    maybe_get_vnode_lock(Partition, Pid).

%% @private
%% @doc Unless skipping the background manager, try to acquire the per-vnode lock.
%%      Sets our task meta-data in the lock as `aae_rebuild', which is useful for
%%      seeing what's holding the lock via {@link riak_core_background_mgr:ps/0}.
-spec maybe_get_vnode_lock(SrcPartition::index(), pid()) -> ok | max_concurrency.
maybe_get_vnode_lock(SrcPartition, Pid) ->
    case riak_core_bg_manager:use_bg_mgr(riak_kv, aae_use_background_manager) of
        true  ->
            Lock = ?KV_VNODE_LOCK(SrcPartition),
            case riak_core_bg_manager:get_lock(Lock, Pid, [{task, aae_rebuild}]) of
                {ok, _Ref} -> ok;
                max_concurrency -> max_concurrency
            end;
        false ->
            ok
    end.

snapshot_and_async_update_tree(Tree, Id, From, Callback) ->
    {SnapTree, Tree2} = hashtree:update_snapshot(Tree),
    Tree3 = hashtree:set_next_rebuild(Tree2, full),
    Self = self(),
    spawn_link(
        fun() ->
            try maybe_callback(Callback)
            catch
                _:E ->
                    ?LOG_ERROR(
                        "An error occurred in update callback: ~p.  "
                        "Ignoring error and proceeding with update.", [E])
            end,
            _ = hashtree:update_perform(SnapTree),
            gen_server:cast(Self, {updated, Id}),
            gen_server:reply(From, ok)
        end),
    Tree3.

maybe_callback(undefined) ->
    ok;
maybe_callback(Callback) ->
    Callback().
