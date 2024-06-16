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
%% riak_kv_token_session process may request a grant.  The session process
%% should monnitor the token_manager to which it makes a request, and fail with
%% the fialure of that token_manager.  A request may be granted immediately, or
%% potentially queued to be granted when the token is next released.
%% 
%% The grant may be refused if the a previous token has been granted and now
%% the preflist has changed - either the primary is new, or the downstreams are
%% different.  In this case, the session should back-off and retry, once any
%% tokens requested under different conditions have been released new tokens
%% may be granted.
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
%% and release that grant on the process terminating for any reason.  
%% 
%% The riak_kv_token_manager has no awareness of other ndoes in the cluster.
%% The riak_kv_token_session request logic should be aware of which
%% riak_kv_token_manager is responsible for granting a given token.  In Riak
%% this is done using the preflist based on the hash of the token key.  The
%% node at the head of the peflist is the node responsible for granting that
%% token.  The next two nodes in the preflist (that are UP), are responsible
%% for verifying that the grant can be made (i.e. to confirm that they have not
%% granted a token while the head node was recently unavailable).  

-module(riak_kv_token_manager).

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

-export(
    [
        start_link/0,
        request_token/2,
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
            grants = maps:new() :: grant_map(),
            queues = maps:new() :: request_queues(),
            associations = maps:new() :: #{session_pid() => token_id()}
        }
    ).

-type verify_count() :: non_neg_integer().
-type granted_session()
    :: {local, session_pid(), verify_list(), verify_count()}|
        {upstream, manager_pid(), session_pid()}.
-type grant_map() :: #{token_id() => granted_session()}.
-type request_queues()
    :: #{token_id() => list({pid(), verify_list()})}.
-type token_id() :: {token, binary()}|{riak_object:bucket(), riak_object:key()}.
-type verify_list() :: [downstream_node()].
-type session_pid() :: pid().
-type manager_pid() :: pid().
-type downstream_node() :: node()|pid().
    % in tests will be a pid() not a node()

-export_type([token_id/0, verify_list/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec start_link() -> {ok, manager_pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc request_token/2
%% Request a token (TokenID) from this node's Token Manager, requiring the
%% request to be verified in the nodes provided in the VerifyList.
%% 
%% An async response message of either `granted` or `refused` will be returned.
%% Any session process making a request should monitor the riak_kv_token_manager
%% and terminate should the manager terminate.
%% 
%% A token is released when the riak_kv_token_manager receives a 'DOWN' message
%% from a session process associated with a grant.
-spec request_token(token_id(), [node()]) -> ok.
request_token(TokenID, VerifyList) ->
    gen_server:cast(
        ?MODULE, {request, TokenID, VerifyList, self()}). 


%%%============================================================================
%%% API Remote - Downstream/Upstream messages between token managers
%%%============================================================================

%% @doc downstream_check/3
%% Call to a remote node to confirm that it does not currently have a
%% information of a grant from another node.  If it does have such a grant the
%% token request will be refused (return false).
%% 
%% If there is no grant registered, or if the grant that is registered is from
%% the same remote_manager, then the downstream_check will pass and the grant
%% information will be updated
-spec downstream_check(downstream_node(), token_id(), pid()) -> ok.
downstream_check(TestPid, TokenID, SessionPid) when is_pid(TestPid) ->
    gen_server:cast(
        TestPid, {downstream_check, TokenID, {self(), SessionPid}});
downstream_check(ToNode, TokenID, SessionPid) ->
    gen_server:cast(
        {?MODULE, ToNode}, {downstream_check, TokenID, {self(), SessionPid}}).

%% @doc renew_downstream
%% If a token has been released, and a queued token is now to be granted the
%% granting is not blocked by the use of is_downstream_check/3, it is assumed
%% that a notification is present due to the original grant.
-spec downstream_renew(verify_list(), token_id(), pid()) -> ok.
downstream_renew([], _TokenID, _SessionPid) ->
    ok;
downstream_renew([TestPid|Rest], TokenID, SessionPid) when is_pid(TestPid) ->
    gen_server:cast(
        TestPid,
        {downstream_renew, TokenID, {self(), SessionPid}}
    ),
    downstream_renew(Rest, TokenID, SessionPid);
downstream_renew([N|Rest], TokenID, SessionPid) ->
    gen_server:cast(
        {?MODULE, N}, {downstream_renew, TokenID, {self(), SessionPid}}),
    downstream_renew(Rest, TokenID, SessionPid).


%%%============================================================================
%%% API - Operations (helper functions)
%%%============================================================================

%% @doc stats/0
%% Return three counts - the count of grants, the count of queued requests and
%% the count of associations (whenever a grant is made or queued an association
%% is retain to map the PID to the token as a helper should the PID go down).
-spec stats() -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
stats() ->
    gen_server:call(?MODULE, stats).

%% @doc grants/0
%% A map of the current grants that have bee made by this token_manager
-spec grants() -> grant_map().
grants() ->
    gen_server:call(?MODULE, grants).

%%%============================================================================
%%% Callback functions
%%%============================================================================

init(_Args) ->
    {ok, #state{}}.
    
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
    case maps:get(TokenID, State#state.grants, not_found) of
        not_found ->
            lists:foreach(
                fun(N) -> downstream_check(N, TokenID, Session) end,
                VerifyList
            ),
            UpdAssocs =
                maps:put(Session, TokenID, State#state.associations),
            _SidRef = monitor(process, Session),
            UpdGrants =
                maps:put(
                    TokenID,
                    {local, Session, VerifyList, 0},
                    State#state.grants
                ),
            case VerifyList of
                [] ->
                    Session ! granted;
                _ ->
                    %% Need to wait for downstream replies
                    ok
            end,
            {noreply,
                State#state{
                    grants = UpdGrants, associations = UpdAssocs
                }
            };
        {local, _OtherSession, VerifyList, _VerifyCount} ->
            %% Can queue this session, it has the same verifylist as the
            %% existing grant
            UpdAssocs =
                maps:put(Session, TokenID, State#state.associations),
            _SidRef = monitor(process, Session),
            TokenQueue = maps:get(TokenID, State#state.queues, []),
            UpdQueue =
                maps:put(
                    TokenID,
                    [{Session, VerifyList}|TokenQueue],
                    State#state.queues
                ),
            {noreply,
                State#state{
                    queues = UpdQueue,
                    associations = UpdAssocs
                }
            };
        _ ->
            Session ! refused,
            {noreply, State}
    end;

handle_cast({downstream_renew, TokenID, {Manager, Session}}, State) ->
    case maps:is_key(TokenID, State#state.grants) of
        false ->
            UpdAssocs = maps:put(Session, TokenID, State#state.associations),
            _MgrRef = monitor(process, Manager),
            _SidRef = monitor(process, Session),
            UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, Manager, Session},
                    State#state.grants
                ),
            
            {noreply,
                State#state{grants = UpdGrants, associations = UpdAssocs}};
        true ->
            ExistingGrant = maps:get(TokenID, State#state.grants),
            ?LOG_WARNING(
                "Potential conflict ignored on token ~w between ~w and ~w",
                [TokenID, ExistingGrant, {Manager, Session}]
            ),
            {noreply, State}
    end;
handle_cast({downstream_check, TokenID, {Manager, Session}}, State) ->
    case maps:is_key(TokenID, State#state.grants) of
        false ->
            gen_server:cast(
                Manager, {downstream_reply, TokenID, Session, true}
            ),
            UpdAssocs = maps:put(Session, TokenID, State#state.associations),
            _MgrRef = monitor(process, Manager),
            _SidRef = monitor(process, Session),
            UpdGrants =
                maps:put(
                    TokenID,
                    {upstream, Manager, Session},
                    State#state.grants
                ),
            {noreply,
                State#state{grants = UpdGrants, associations = UpdAssocs}};
        true ->
            gen_server:cast(
                Manager, {downstream_reply, TokenID, Session, false}
            ),
            {noreply, State}
        end;
handle_cast({downstream_reply, TokenID, Session, true}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, Session, VerifyList, VerifyCount} ->
            case length(VerifyList) of
                VL when VL == (VerifyCount + 1) ->
                    Session ! granted;
                _ ->
                    ok
            end,
            {noreply,
                State#state{
                    grants = 
                        maps:put(
                            TokenID,
                            {local, Session, VerifyList, VerifyCount + 1},
                            State#state.grants
                        )
                    }
                };
        _ ->
            {noreply, State}
    end;
handle_cast({downstream_reply, TokenID, Session, false}, State) ->
    case maps:get(TokenID, State#state.grants, not_found) of
        {local, Session, _VerifyList, _VerifyCount} ->
            Session ! refused,
            {noreply,
                State#state{grants = maps:remove(TokenID, State#state.grants)}
            };
        _ ->
            {noreply, State}
    end.


handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    case maps:take(Pid, State#state.associations) of
        {TokenID,  UpdAssocs} ->
            %% An association exists, so this is assumed to be a session
            %% process which has gone down.  This might be in a queue, or
            %% in receipt of a grant
            Queues =
                clear_session_from_queues(
                    State#state.queues,
                    TokenID,
                    Pid
                ),
            case maps:get(TokenID, State#state.grants, not_found) of
                {local, Pid, VerifyList, _VerifyCount} ->
                    %% There is a grant, is there a queued request for that
                    %% same token that we can now grant
                    case return_session_from_queues(Queues, TokenID) of
                        {none, UpdQueues} ->
                            {noreply,
                                State#state{
                                    associations = UpdAssocs,
                                    queues = UpdQueues,
                                    grants =
                                        maps:remove(
                                            TokenID,
                                            State#state.grants
                                        )
                                }
                            };
                        {{NextSession, VerifyList}, UpdQueues} ->
                            %% Only requests with the same VerifyList should be
                            %% queued
                            UpdGrants =
                                maps:put(
                                    TokenID,
                                    {local, NextSession, VerifyList, 0},
                                    State#state.grants
                                ),
                            NextSession ! granted,
                            %% Check that the downstream nodes are still aware
                            %% of this grant (in case, for example, they have
                            %% restarted in between)
                            ok =
                                downstream_renew(
                                    VerifyList,
                                    TokenID,
                                    NextSession
                                ),
                            {noreply,
                                State#state{
                                    associations = UpdAssocs,
                                    queues = UpdQueues,
                                    grants = UpdGrants
                                }
                            }
                    end;
                {upstream, _Manager, Pid} ->
                    %% It is possible that this is a remote PID that has failed
                    %% No need to promote form queue in this case, just remove
                    {noreply,
                        State#state{
                            associations = UpdAssocs,
                            queues = Queues,
                            grants =
                                maps:remove(
                                    TokenID,
                                    State#state.grants
                                )
                        }
                    };
                _ ->
                    %% The session may have been queued and not had a grant
                    {noreply,
                        State#state{associations = UpdAssocs, queues = Queues}
                    }
            end;
        _ ->
            %% No assocition is assumed to be a token manager
            ?LOG_WARNING(
                "Remote Token Manager ~w reported down due to ~p",
                [Pid, Reason]
            ),
            FilterFun =
                fun({_Tid, G}) ->
                    case G of
                        {upstream, Pid, _Session} ->
                            false;
                        _ ->
                            true
                    end
                end,
            UpdGrants =
                maps:from_list(
                    lists:filter(
                        FilterFun,
                        maps:to_list(State#state.grants)
                    )
                ),
            {noreply, State#state{grants =  UpdGrants}}
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


%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

manager_simple_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr2, [Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    S3 = spawn(Req1),
    S4 = spawn(Req2),
    receive Rf1 -> ?assertMatch({refused, S4}, Rf1) end,
    S1 ! terminate,
    receive Gr2 -> ?assertMatch({granted, S2}, Gr2) end,
    S2 ! terminate,
    receive Gr3 -> ?assertMatch({granted, S3}, Gr3) end,
    S3 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).


manager_downstream_failure_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    S3 = spawn(Req1),
    ok = gen_server:stop(Mgr2),
    {ok, Mgr2A} = gen_server:start(?MODULE, [], []),
    ?assert(is_process_alive(S1)),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2A, Mgr3]),
        % Will not queue a request if the verify list has changed
    S4 = spawn(Req2),
    receive Rf1 -> ?assertMatch({refused, S4}, Rf1) end,
    S1 ! terminate,
    receive Gr2 -> ?assertMatch({granted, S2}, Gr2) end,
    S2 ! terminate,
    receive Gr3 -> ?assertMatch({granted, S3}, Gr3) end,
    S3 ! terminate,
    S5 = spawn(Req2),
    receive Gr4 -> ?assertMatch({granted, S5}, Gr4) end,
    S5 ! terminate,
    ?assert(receive _ -> false after 10 -> true end),
    gen_server:stop(Mgr1),
    gen_server:stop(Mgr2A),
    gen_server:stop(Mgr3).

manager_primary_failure_test() ->
    {ok, Mgr1} = gen_server:start(?MODULE, [], []),
    {ok, Mgr2} = gen_server:start(?MODULE, [], []),
    {ok, Mgr3} = gen_server:start(?MODULE, [], []),
    Req1 = requestor_fun(self(), <<"T1">>, Mgr1, [Mgr2, Mgr3]),
    S1 = spawn(Req1),
    receive Gr1 -> ?assertMatch({granted, S1}, Gr1) end,
    S2 = spawn(Req1),
    S3 = spawn(Req1),
    gen_server:stop(Mgr1),
    Req2 = requestor_fun(self(), <<"T1">>, Mgr2, [Mgr3]),
    S4 = spawn(Req2),
    receive Gr2 -> ?assertMatch({granted, S4}, Gr2) end,
    {ok, Mgr1A} = gen_server:start(?MODULE, [], []),
    Req3 = requestor_fun(self(), <<"T1">>, Mgr1A, [Mgr2, Mgr3]),
    S5 = spawn(Req3),
    receive Rf3 -> ?assertMatch({refused, S5}, Rf3) end,
    S4 ! terminate,
    S6 = spawn(Req3),
    receive Gr4 -> ?assertMatch({granted, S6}, Gr4) end,
    lists:foreach(fun(P) -> P ! terminate end, [S1, S2, S3, S6]),
    gen_server:stop(Mgr1A),
    gen_server:stop(Mgr2),
    gen_server:stop(Mgr3).
    

requestor_fun(ReturnPid, Token, Mgr, VerifyList) ->
    fun() ->
        gen_server:cast(Mgr, {request, Token, VerifyList, self()}),
        requestor_receive_loop(ReturnPid)
    end.


requestor_receive_loop(ReturnPid) ->
    receive
        granted ->
            ReturnPid ! {granted, self()},
            requestor_receive_loop(ReturnPid);
        refused ->
            ReturnPid ! {refused, self()},
            requestor_receive_loop(ReturnPid);
        _ ->
            ok
    end.

-endif.