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

%% @doc Grant requests for tokens, if and only if, no other request is
%% active.
%% 
%% riak_kv_token_session process may reveive the grant, and those processes
%% should be monitored so that the grant is revoked should the session
%% terminate

-module(riak_kv_token_manager).

-behavior(gen_server).

-export(
    [
        start_link/0,
        request_token/2,
        is_downstream_clear/1,
        stats/0
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
            queues = maps:new()
                :: request_queues(),
            grants = maps:new()
                :: #{token_id() => pid()},
            associations = maps:new()
                :: #{pid() => token_id()}
        }
    ).

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

-spec is_downstream_clear(token_id()) -> boolean().
is_downstream_clear(TokenID) ->
    gen_server:call(?MODULE, {is_clear, TokenID}, infinity).

-spec stats() -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
stats() ->
    gen_server:call(?MODULE, stats).

%%%============================================================================
%%% Callback functions
%%%============================================================================

init(_Args) ->
    {ok, #state{}}.

handle_call({is_clear, TokenID}, _From, State) ->
    {reply, not maps:is_key(TokenID, State#state.grants), State};
handle_call(stats, _From, State) ->
    Grants = maps:size(State#state.grants),
    QueuedRequests =
        maps:fold(
            fun(_T, Q, Acc) -> length(Q) + Acc end,
            0,
            State#state.queues
        ),
    Associations = maps:size(State#state.associations),
    {reply, {Grants, QueuedRequests, Associations}, State}.

handle_cast({request, TokenID, VerifyList, Session}, State) ->
    case maps:is_key(TokenID, State#state.grants) of
        true ->
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
                State#state{queues = UpdQueue, associations = UpdAssocs}};
        false ->
            case downstream_clear(VerifyList, TokenID) of
                true ->
                    UpdGrants =
                        maps:put(TokenID, Session, State#state.grants),
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
    end.

handle_info({'DOWN', _Ref, process, Session, _Reason}, State) ->
    {TokenID,  UpdAssocs} = maps:take(Session, State#state.associations),
    Queues = clear_session_from_queues(State#state.queues, TokenID, Session),
    case maps:get(TokenID, State#state.grants, not_found) of
        Session ->
            case return_session_from_queues(Queues, TokenID) of
                {none, UpdQueues} ->
                    {noreply,
                        State#state{
                            associations = UpdAssocs,
                            queues = UpdQueues,
                            grants = maps:remove(TokenID, State#state.grants)
                        }
                    };
                {{NextSession, _VerifyList}, UpdQueues} ->
                    UpdGrants =
                        maps:put(TokenID, NextSession, State#state.grants),
                    NextSession ! granted,
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

-spec downstream_clear(verify_list(), token_id()) -> boolean().
downstream_clear(VerifyList, TokenID) ->
    case lists:member(node(), VerifyList) of
        true ->
            false;
        false ->
            downstream_clear(VerifyList, TokenID, true)
    end.

downstream_clear([], _TokenID, AreAllClear) ->
    AreAllClear;
downstream_clear([Node|Rest], TokenID, true) ->
    Clear = erpc:call(Node, ?MODULE, is_downstream_clear, [TokenID]),
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
            {{NextSession, VerifyList},
                maps:put(TokenID, lists:reverse(QueueRem), Queues)}
    end.


