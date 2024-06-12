%% -------------------------------------------------------------------
%%
%% riak_kv_token_manager: Process for granting access to tokens
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

%% @doc Grant requests for tokens, if no other request is active.
%% 
%% riak_kv_token_session process may reveive the grant, and those processes
%% should be monitored so that the grant is revoked should the session
%% terminate
%% 
%% This is to provide "stronger" but not "strong" consistency.  The aim is to
%% have a system whereby in healthy clusters and in common failure scenarios
%% tokens can be requested without conflict - in the first case to allow for
%% conditional logic on PUTs to be reliable in these circumstances.
%% 
%% It is accepted that there will be partition scenarios, and scenarios where
%% rapid changes in up/down state where guarantees cannot be met. The intention
%% is that eventual consistency will be the fallback.  The application/operator
%% may have to deal with siblings to protect against data loss - but the
%% frequency with which those siblings occur should be manageable.
%% 
%% The riak_kv_token_manager is intended to work in conjunction with
%% riak_kv_token_session processes.  Each node should have a single
%% riak_kv_token_manager.  When a token is requested the request will be made
%% by a riak_kv_token_session process, and the token is granted to that
%% process.  The session identifier may be returned to the requester (not the
%% token).  The identifier can then be used to route riak_client commands to
%% the session process, and each command will prompt the renewal of the token.
%% 
%% The session process exists to manage a single session, the lifecycle of a
%% single grant.
%% 
%% In every situation, the riak_kv_token_manager granting the token, and the
%% riak_kv_token_session process requesting the token must be on the same node.
%% 
%% The riak_kv_token_manager process should be monitored by each
%% riak_kv_token_session process which has requested a token, or been granted a
%% token - and the riak_kv_token_session process should terminate if the
%% riak_kv_token_manager should go down.  The riak_kv_token_manager will
%% monitor every riak_kv_token_session process to which it has made a grant,
%% and release that grant on the process terminating for any reason.  There
%% is no monitoring of remote processes.
%% 
%% The riak_kv_token_manager has no awareness of other ndoes in the cluster.
%% The riak_kv_token_session request logic should be aware of which
%% riak_kv_token_manager is responsible for granting a given token.  In Riak
%% this is done using the preflist based on the hash of the token key.  The
%% node at the head of the peflist is the node responsible for granting that
%% token.  The next two nodes in the preflist (that are UP), are responsible
%% for verifying that the grant can be made (i.e. to confirm that they have not
%% granted a token while the head node was recently unavailable). 
%% 
%% Verification of the request in downstream (from the perspective of the
%% preflist) nodes is a loose process.  The aim is to handle obvious failure
%% at a low cost, and to avoid deadlocks - rather than maintaining absolute
%% guarantees across all scenarios.

-module(riak_kv_token_manager).

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        start_link/0,
        request_token/2,
        is_downstream_clear/2,
        is_granted_remotely/2,
        not_granted/2,
        clear_downstream/2,
        renew_downstream/2,
        stats/0,
        grants/0
    ]
).

-export(
    [
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
    ]
).

-record(state,
        {
            queues = maps:new() :: request_queues(),
            grants = maps:new() :: grant_map(),
            associations = maps:new() :: #{pid() => token_id()}
        }
    ).

-type granted_session()
    :: {local, pid(), verify_list()}| {upstream, {node(), pid()}}.
-type grant_map() :: #{token_id() => granted_session()}.
-type request_queues()
    :: #{token_id() => list({pid(), verify_list()})}.
-type token_id() :: {token, binary()}|{riak_object:bucket(), riak_object:key()}.
-type verify_list() :: [node()].

-export_type([token_id/0, verify_list/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec request_token(token_id(), [node()]) -> ok.
request_token(TokenID, VerifyList) ->
    gen_server:cast(
        ?MODULE, {request, TokenID, VerifyList, self()}). 

-spec is_downstream_clear(node(), token_id()) -> boolean().
is_downstream_clear(ToNode, TokenID) ->
    gen_server:call(
        {?MODULE, ToNode}, {is_clear, TokenID, {node(), self()}}, infinity
    ).

-spec is_granted_remotely(node(), token_id()) -> ok.
is_granted_remotely(ToNode, TokenID) ->
    gen_server:cast({?MODULE, ToNode}, {is_granted_locally, TokenID, node()}).

-spec not_granted(node(), token_id()) -> ok.
not_granted(Node, TokenID) ->
    gen_server:cast({?MODULE, Node}, {not_granted, TokenID, node()}).

-spec clear_downstream(token_id(), {node(), pid()}) -> ok.
clear_downstream(TokenID, Upstream) ->
    gen_server:cast(?MODULE, {clear_downstream, TokenID, Upstream}).

-spec renew_downstream(token_id(), {node(), pid()}) -> ok.
renew_downstream(TokenID, Upstream) ->
    gen_server:cast(?MODULE, {renew_downstream, TokenID, Upstream}).

-spec stats() -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
stats() ->
    gen_server:call(?MODULE, stats).

-spec grants() -> grant_map().
grants() ->
    gen_server:call(?MODULE, grants).


%%%============================================================================
%%% Callback functions
%%%============================================================================

init(_Args) ->
    {ok, #state{}}.

handle_call({is_clear, TokenID, {Node, Pid}}, _From, State) ->
    case free_to_grant(TokenID, State#state.grants) of
        {true, none, UpdGrants} ->
            {reply,
                true,
                State#state{
                    grants = 
                        maps:put(TokenID, {upstream, {Node, Pid}}, UpdGrants)
                }
            };
        {false, _, UpdGrants} ->
            {reply, false, State#state{grants = UpdGrants}}
    end;
handle_call(stats, _From, State) ->
    Grants = maps:size(State#state.grants),
    QueuedRequests =
        maps:fold(
            fun(_T, Q, Acc) -> length(Q) + Acc end,
            0,
            State#state.queues
        ),
    Associations = maps:size(State#state.associations),
    {reply, {Grants, QueuedRequests, Associations}, State};
handle_call(grants, _From, State) ->
    {reply, State#state.grants, State}.

handle_cast({request, TokenID, VerifyList, Session}, State) ->
    case free_to_grant(TokenID, State#state.grants) of
        {false, local, UpdGrants} ->
            TokenQueue = maps:get(TokenID, State#state.queues, []),
            UpdQueue =
                maps:put(
                    TokenID,
                    [{Session, VerifyList}|TokenQueue],
                    State#state.queues
                ),
            _Ref = erlang:monitor(process, Session),
            UpdAssocs = maps:put(Session, TokenID, State#state.associations),
            {noreply,
                State#state{
                    grants = UpdGrants,
                    queues = UpdQueue,
                    associations = UpdAssocs}
                };
        {false, upstream, UpdGrants} ->
            Session ! refused,
            {noreply, State#state{grants = UpdGrants}};
        {true, none, UpdGrants0} ->
            case check_downstream_clear(VerifyList, TokenID) of
                true ->
                    UpdGrants =
                        maps:put(
                            TokenID, {local, Session, VerifyList}, UpdGrants0
                        ),
                    UpdAssocs =
                        maps:put(Session, TokenID, State#state.associations),
                    Session ! granted,
                    _Ref = erlang:monitor(process, Session),
                    {noreply,
                        State#state{
                            grants = UpdGrants, associations = UpdAssocs
                        }};
                false ->
                    Session ! refused,
                    {noreply, State}
            end
    end;
handle_cast({clear_downstream, TokenID, Upstream}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {upstream, Upstream} ->
            {noreply,
                State#state{
                    grants = maps:remove(TokenID, State#state.grants)
                }
            };
        _ ->
            {noreply, State}
    end;
handle_cast({renew_downstream, TokenID, Upstream}, State) ->
    case maps:is_key(TokenID, State#state.grants) of
        false ->
            UpdGrants =
                maps:put(TokenID, {upstream, Upstream}, State#state.grants),
            {noreply, State#state{grants = UpdGrants}};
        true ->
            ExistingGrant = maps:get(TokenID, State#state.grants),
            ?LOG_WARNING(
                "Potential conflict ignored on token ~w between ~w and ~w",
                [TokenID, ExistingGrant, Upstream]
            ),
            {noreply, State}
    end;
handle_cast({is_granted_locally, TokenID, FromNode}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, _Session, _VL} ->
            ok;
        _ ->
            not_granted(FromNode, TokenID)
    end,
    {noreply, State};
handle_cast({not_granted, TokenID, FromNode}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {upstream, {FromNode, _FromPid}} ->
            {noreply,
                State#state{
                    grants = maps:remove(TokenID, State#state.grants)
                }
            };
        _ ->
            {noreply, State}
    end.

handle_info({'DOWN', _Ref, process, Session, _Reason}, State) ->
    {TokenID,  UpdAssocs} = maps:take(Session, State#state.associations),
    Queues = clear_session_from_queues(State#state.queues, TokenID, Session),
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, Session, VerifyList} ->
            case return_session_from_queues(Queues, TokenID) of
                {none, UpdQueues} ->
                    ok = clear_downstream(VerifyList, TokenID, self()),
                    {noreply,
                        State#state{
                            associations = UpdAssocs,
                            queues = UpdQueues,
                            grants = maps:remove(TokenID, State#state.grants)
                        }
                    };
                {{NextSession, NextVerifyList}, UpdQueues} ->
                    UpdGrants =
                        maps:put(
                            TokenID,
                            {local, NextSession, NextVerifyList},
                            State#state.grants
                        ),
                    NextSession ! granted,
                    ok = renew_downstream(VerifyList, TokenID, self()),
                    {noreply,
                        State#state{
                            associations = UpdAssocs,
                            queues = UpdQueues,
                            grants = UpdGrants
                        }
                    }
            end;
        _ ->
            {noreply,
                State#state{associations = UpdAssocs, queues = Queues}
            }
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec free_to_grant(
    token_id(), grant_map()) -> {boolean(), local|upstream|none, grant_map()}.
free_to_grant(TokenID, GrantMap) ->
    case maps:get(TokenID, GrantMap, not_found) of
        {local, _Session, _VerifyList} ->
            {false, local, GrantMap};
        {upstream, {Node, Pid}} ->
            case check_upstream(TokenID, Node, Pid) of
                true ->
                    {false, upstream, GrantMap};
                _ ->
                    {true, none, maps:remove(TokenID, GrantMap)}
            end;
        not_found ->
            {true, none, GrantMap}
    end.

check_upstream(TokenID, Node, UpstreamMgr) ->
    is_granted_remotely(Node, TokenID),
    CurrentTMGR = erpc:call(Node, erlang, whereis, [?MODULE]),
    UpstreamMgr == CurrentTMGR.


-spec clear_downstream(verify_list(), token_id(), pid()) -> ok.
clear_downstream([], _TokenID, _Pid) ->
    ok;
clear_downstream([N|Rest], TokenID, Pid) ->
    gen_server:cast(
        {riak_kv_token_manager, N},
        {clear_downstream, TokenID, {node(), Pid}}
    ),
    clear_downstream(Rest, TokenID, Pid).

-spec renew_downstream(verify_list(), token_id(), pid()) -> ok.
renew_downstream([], _TokenID, _Pid) ->
    ok;
renew_downstream([N|Rest], TokenID, Pid) ->
    gen_server:cast(
        {riak_kv_token_manager, N},
        {renew_downstream, TokenID, {node(), Pid}}
    ),
    renew_downstream(Rest, TokenID, Pid).

-spec check_downstream_clear(verify_list(), token_id()) -> boolean().
check_downstream_clear(VerifyList, TokenID) ->
    case lists:member(node(), VerifyList) of
        true ->
            false;
        false ->
            downstream_clear(VerifyList, TokenID, true)
    end.

downstream_clear([], _TokenID, AreAllClear) ->
    AreAllClear;
downstream_clear([Node|Rest], TokenID, true) ->
    Clear = is_downstream_clear(Node, TokenID),
    downstream_clear(Rest, TokenID, Clear == true);
downstream_clear(_VerifyList, _TokenID, false)  ->
    false.

-spec clear_session_from_queues(
    request_queues(), token_id(), pid()) -> request_queues().
clear_session_from_queues(Queues, TokenID, Session) ->
    Queue = maps:get(TokenID, Queues, []),
    case lists:keytake(Session, 1, Queue) of
        {value, {Session, _VL}, UpdTQueue} ->
            maps:put(TokenID, UpdTQueue, Queues);
        false ->
            Queues
    end.

-spec return_session_from_queues(
    request_queues(), token_id()) -> 
        {{pid(), verify_list()}|none, request_queues()}.
return_session_from_queues(Queues, TokenID) ->
    case maps:get(TokenID, Queues, []) of
        [] ->
            {none, Queues};
        QueuedRequests ->
            [{NextSession, VerifyList}|QueueRem] =
                lists:reverse(QueuedRequests),
            case QueueRem of
                [] ->
                    {{NextSession, VerifyList}, maps:remove(TokenID, Queues)};
                _ ->
                    {{NextSession, VerifyList},
                        maps:put(TokenID, lists:reverse(QueueRem), Queues)}
            end
    end.