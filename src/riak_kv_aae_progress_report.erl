%% -------------------------------------------------------------------
%%
%% riak_kv_aae_progress_report: additional details of AAE tree rebuilds
%%
%% Copyright (c) 2024 TI Tokyo.  All Rights Reserved.
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

-module(riak_kv_aae_progress_report).

-define(TIMEOUT_GET_VNODES, 30000).
-define(TIMEOUT_GET_AAECONTROLLER_STATE, 30000).
-define(TIMEOUT_GET_KEYSTORE_STATE, 30000).
-define(TIMEOUT_GET_TREECACHE_STATE, 30000).

-export([produce/0,
         produce/1,
         produce/4,
         print/0,
         print/1]).

-spec produce() -> [{atom(), term()}].
-spec produce(timeout()) -> [{atom(), term()}].
-spec produce(timeout(), timeout(), timeout(), timeout()) -> [{atom(), term()}].

produce() ->
    produce(?TIMEOUT_GET_VNODES,
            ?TIMEOUT_GET_AAECONTROLLER_STATE,
            ?TIMEOUT_GET_KEYSTORE_STATE,
            ?TIMEOUT_GET_TREECACHE_STATE).

produce(Timeout) ->
    produce(Timeout, Timeout, Timeout, Timeout).

produce(T1, T2, T3, T4) ->
    case application:get_env(riak_kv, tictacaae_active) of
        {ok, active} ->
            {ok, produce2(T1, T2, T3, T4)};
        _ ->
            {error, tictacaae_not_active}
    end.
produce2(TimeoutGetVNodes,
        TimeoutGetAAEControllerState,
        TimeoutGetKeyStoreState,
        TimeoutGetTreeCacheState) ->
    {AllNodeResponses, _} = rpc:multicall(
                              supervisor, which_children, [riak_core_vnode_sup],
                              TimeoutGetVNodes),
    VVSS = lists:append(
             [case sys:get_state(P) of
                  {active, {state, Idx, riak_kv_vnode, VNodeState, _, _, _, _, _, _, _, _}} ->
                      [{Idx, VNodeState}];
                  _ ->
                      []
              end || {_, P, _, _} <- lists:flatten(AllNodeResponses)]),
    [begin
         AAECntrl = element(24, VNState),
         TictacRebuilding = element(30, VNState),
         AAECntrlState = sys:get_state(AAECntrl, TimeoutGetAAEControllerState),
         KeyStore = element(2, AAECntrlState),
         KeyStoreCurrentStatus = if is_pid(KeyStore) ->
                                         aae_keystore:store_currentstatus(KeyStore);
                                    el/=se ->
                                         not_running
                                 end,
         {_, KeyStoreState} = sys:get_state(KeyStore, TimeoutGetKeyStoreState),
         %% in 2.9.10, last_rebuild is at pos 11; in 3.2.x, it is 12
         LastRebuild = case element(12, KeyStoreState) of
                           never ->
                               never;
                           TS ->
                               calendar:now_to_local_time(TS)
                       end,

         NextRebuild = calendar:now_to_local_time(element(8, AAECntrlState)),
         {RebuildWait, RebuildDelay} = element(9, AAECntrlState),
         TreeCaches = element(3, AAECntrlState),
         TCStates = [sys:get_state(P, TimeoutGetTreeCacheState) || {_, P} <- TreeCaches],
         TotalDirtySegments = [element(8, S) || S <- TCStates],
         [{partition, Idx},
          {key_store_current_status, KeyStoreCurrentStatus},
          {last_rebuild, LastRebuild},
          {next_rebuild, NextRebuild},
          {total_dirty_segments, length(lists:append(TotalDirtySegments))},
          {rebuild_inprogress, TictacRebuilding /= false},
          {controller_pid, AAECntrl},
          {rebuild_wait, RebuildWait},
          {rebuild_delay, RebuildDelay}
         ]
     end || {Idx, VNState} <- VVSS].

print() ->
    print([{omit, "built"}]).
print(Options) ->
    case produce() of
        {ok, AA} ->
            print2(AA, Options);
        {error, tictacaae_not_active} ->
            io:format("tictacaae_active is set to passive\n")
    end.
print2(AA, Options) ->
    Omit = proplists:get_value(omit, Options, "nothing"),
    io:format("~22s  ~52s  ~10s  ~21s  ~5s  ~10s  ~21s  ~15s\n", ["Node Name", "Partition ID", "Status", "Last Rebuild Date", "Delay", "Wait", "Next Rebuild Date", "Controller PID"]),
    io:format("~22s  ~52s  ~10s  ~21s  ~5s  ~10s  ~21s  ~15s\n", ["----------------------", "----------------------------------------------------", "----------", "---------------------", "-----", "----------", "---------------------", "----------------"]),
    [begin
         Idx = proplists:get_value(partition, M),
         LastRebuild = proplists:get_value(last_rebuild, M),
         InProgress = proplists:get_value(rebuild_inprogress, M),
         NextRebuild = proplists:get_value(next_rebuild, M),
         ControllerPid = proplists:get_value(controller_pid, M),
         RebuildWait = proplists:get_value(rebuild_wait, M),
         RebuildDelay = proplists:get_value(rebuild_delay, M),
         ControllerNodeName = node(ControllerPid),
         LastRebuildDate =
             case LastRebuild of
                 never ->
                     "never";
                 {{LRY, LRMo, LRD}, {LRH, LRMi, LRS}} ->
                     io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B", [LRY, LRMo, LRD, LRH, LRMi, LRS])
             end,
         NextRebuildDate =
             case NextRebuild of
                 never ->
                     "never";
                 {{NRY, NRMo, NRD}, {NRH, NRMi, NRS}} ->
                     io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B", [NRY, NRMo, NRD, NRH, NRMi, NRS])
             end,
         Status =
             case {LastRebuild, InProgress, NextRebuild} of
                 {never, false, Scheduled} when Scheduled /= undefined ->
                     "unbuilt";
                 {Built, false, _} when Built /= never ->
                     "built";
                 {Built, true, _} when Built /= never ->
                     "rebuilding";
                 {never, true, _} ->
                     "building"
             end,
         if Status /= Omit ->
                 io:format("~22s  ~52b  ~10s  ~21s  ~5b  ~10b  ~21s  ~15s\n", [ControllerNodeName, Idx, Status, LastRebuildDate, RebuildWait, RebuildDelay, NextRebuildDate, pid_to_list(ControllerPid)]);
            el/=se ->
                 skip
         end
     end || M <- AA],
    ok.
