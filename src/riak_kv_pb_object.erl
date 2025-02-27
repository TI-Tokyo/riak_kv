%% -------------------------------------------------------------------
%%
%% riak_kv_pb_object: Expose KV functionality to Protocol Buffers
%%
%% Copyright (c) 2012-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>The Object/Key PB service for Riak KV. This covers the
%% following request messages in the original protocol:</p>
%%
%% <pre>
%%  3 - RpbGetClientIdReq
%%  5 - RpbSetClientIdReq
%%  9 - RpbGetReq
%% 11 - RpbPutReq
%% 13 - RpbDelReq
%% 202 - RpbFetchReq
%% 204 - RpbPushReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  4 - RpbGetClientIdResp
%%  6 - RpbSetClientIdResp
%% 10 - RpbGetResp
%% 12 - RpbPutResp - 0 length
%% 14 - RpbDelResp
%% 203 - RpbFetchResp
%% 205 - RpbPushResp
%% </pre>
%%
%% <p>The semantics are unchanged from their original
%% implementations.</p>
%% @end

-module(riak_kv_pb_object).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-ifdef(TEST).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx,   % context to go along with request (partial results, request ids etc)
                is_consistent = false :: boolean(),
                client_id = <<0,0,0,0>>, % emulate legacy API when vnode_vclocks is true
                repl_compress = false :: boolean()}).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    ToCompress = app_helper:get_env(riak_kv, replrtq_compressonwire, false),
    #state{client=C, repl_compress = ToCompress}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbgetreq{} ->
            {ok, Msg, {"riak_kv.get", bucket_type(Msg#rpbgetreq.type,
                                                        Msg#rpbgetreq.bucket)}};
        #rpbputreq{} ->
            {ok, Msg, {"riak_kv.put", bucket_type(Msg#rpbputreq.type,
                                                        Msg#rpbputreq.bucket)}};
        #rpbdelreq{} ->
            {ok, Msg, {"riak_kv.delete", bucket_type(Msg#rpbdelreq.type,
                                                           Msg#rpbdelreq.bucket)}};
        _ ->
            {ok, Msg}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(rpbgetclientidreq, #state{client=C, client_id=CID} = State) ->
    ClientId =
        case riak_core_capability:get({riak_kv, vnode_vclocks}) of
            true ->
                CID;
            false ->
                riak_client:get_client_id(C)
        end,
    Resp = #rpbgetclientidresp{client_id = ClientId},
    {reply, Resp, State};

process(#rpbsetclientidreq{client_id = ClientId}, State) ->
    NewState = case riak_core_capability:get({riak_kv, vnode_vclocks}) of
                   true -> State#state{client_id=ClientId};
                   false ->
                       {ok, C} = riak:local_client(ClientId),
                       State#state{client = C}
               end,
    {reply, rpbsetclientidresp, NewState};

process(#rpbgetreq{bucket = <<>>}, State) ->
    {error, "Bucket cannot be zero-length", State};
process(#rpbgetreq{key = <<>>}, State) ->
    {error, "Key cannot be zero-length", State};
process(#rpbgetreq{type = <<>>}, State) ->
    {error, "Type cannot be zero-length", State};
process(#rpbgetreq{bucket=B0, type=T, key=K, r=R0, pr=PR0,
                    notfound_ok=NFOk, node_confirms=NC,
                    basic_quorum=BQ, if_modified=VClock,
                    head=Head, deletedvclock=DeletedVClock,
                    n_val=N_val, sloppy_quorum=SloppyQuorum,
                    timeout=Timeout},
            #state{client=C} = State) ->
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    B = maybe_bucket_type(T, B0),
    Options =
        make_option(deletedvclock, DeletedVClock) ++
        make_option(r, R) ++
        make_option(pr, PR) ++
        make_option(timeout, Timeout) ++
        make_option(notfound_ok, NFOk) ++
        make_option(node_confirms, NC) ++
        make_option(basic_quorum, BQ) ++
        make_option(n_val, N_val) ++
        make_option(sloppy_quorum, SloppyQuorum),
    case riak_client:get(B, K, Options, C) of
        {ok, O} ->
            case erlify_rpbvc(VClock) == riak_object:vclock(O) of
                true ->
                    {reply, #rpbgetresp{unchanged = true}, State};
                _ ->
                    Contents = riak_object:get_contents(O),
                    PbContent = case Head of
                                    true ->
                                        %% Remove all the 'value' fields from the contents
                                        %% This is a rough equivalent of a REST HEAD
                                        %% request
                                        BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                                        riak_pb_kv_codec:encode_contents(BlankContents);
                                    _ ->
                                        riak_pb_kv_codec:encode_contents(Contents)
                                end,
                    {reply, #rpbgetresp{content = PbContent,
                                        vclock = pbify_rpbvc(riak_object:vclock(O))}, State}
            end;
        {error, {deleted, TombstoneVClock}} ->
            %% Found a tombstone - return its vector clock so it can
            %% be properly overwritten
            {reply, #rpbgetresp{vclock = pbify_rpbvc(TombstoneVClock)}, State};
        {error, notfound} ->
            {reply, #rpbgetresp{}, State};
        {error, Reason} ->
            {error, {format,Reason}, State}
    end;

process(#rpbfetchreq{queuename = QueueName, encoding = EncodingBin},
        #state{client=C, repl_compress=ToCompress} = State) ->
    Result = 
        try
            riak_client:fetch(binary_to_existing_atom(QueueName, utf8), C)
        catch _:badarg ->
            {error, queue_not_defined}
        end,
    Encoding =
        case EncodingBin of
            undefined ->
                internal;
            <<"internal">> ->
                internal;
            <<"internal_aaehash">> ->
                internal_aaehash
        end,
    case Result of
        {ok, queue_empty} ->
            {reply, #rpbfetchresp{queue_empty = true}, State};
        {ok, {reap, {B, K, TC, LMD}}} ->
            EncObj =
                riak_object:nextgenrepl_encode(
                    repl_v1, {reap, {B, K, TC, LMD}}, false),
            CRC32 = erlang:crc32(EncObj),
            Resp =
                #rpbfetchresp{
                    queue_empty = false,
                    replencoded_object = EncObj,
                    crc_check = CRC32},
            {reply,
                encode_nextgenrepl_response(Encoding, Resp, {B, K, TC}),
                State};
        {ok, {deleted, TombClock, RObj}} ->
            % Never bother compressing tombstones, they're practically empty
            EncObj = riak_object:nextgenrepl_encode(repl_v1, RObj, false),
            CRC32 = erlang:crc32(EncObj),
            Resp =
                #rpbfetchresp{queue_empty = false,
                                deleted = true,
                                replencoded_object = EncObj,
                                crc_check = CRC32,
                                deleted_vclock = pbify_rpbvc(TombClock)},
            {reply, encode_nextgenrepl_response(Encoding, Resp, RObj), State};
        {ok, RObj} ->
            EncObj = riak_object:nextgenrepl_encode(repl_v1, RObj, ToCompress),
            CRC32 = erlang:crc32(EncObj),
            Resp =
                #rpbfetchresp{queue_empty = false,
                                deleted = false,
                                replencoded_object = EncObj,
                                crc_check = CRC32},
            {reply, encode_nextgenrepl_response(Encoding, Resp, RObj), State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

process(#rpbpushreq{queuename = QueueNameBin, keys_value = KVL}, State) ->
    QueueName = binary_to_existing_atom(QueueNameBin, utf8),
    KeyClockList = lists:map(fun unpack_keyclock_fun/1, KVL),
    ok = riak_kv_replrtq_src:replrtq_ttaaefs(QueueName, KeyClockList),
    case riak_kv_replrtq_src:length_rtq(QueueName) of
        {QueueName, {FL, FSL, RTL}} ->
            {reply,
                #rpbpushresp{queue_exists = true,
                                queuename = QueueNameBin,
                                foldq_length = FL,
                                fsync_length = FSL,
                                realt_length = RTL},
                State};
        _ ->
            {reply,
                #rpbpushresp{queue_exists = false, queuename = QueueNameBin},
                State}
    end;

process(rpbmembershipreq, State) ->
    MembershipList =
        lists:map(
            fun({IP, Port}) ->
                #rpbclustermemberentry{ip = IP, port = Port}
            end,
            riak_client:membership_request(pb)),
    {reply, #rpbmembershipresp{up_nodes = MembershipList}, State};

process(#rpbputreq{bucket = <<>>}, State) ->
    {error, "Bucket cannot be zero-length", State};
process(#rpbputreq{key = <<>>}, State) ->
    {error, "Key cannot be zero-length", State};
process(#rpbputreq{type = <<>>}, State) ->
    {error, "Type cannot be zero-length", State};
process(#rpbputreq{bucket=B0, type=T, key=K, vclock=PbVC,
                   if_not_modified=NotMod, if_none_match=NoneMatch,
                   n_val=N_val, sloppy_quorum=SloppyQuorum} = Req,
        #state{client=C} = State) when NotMod; NoneMatch ->
    GetOpts = make_option(n_val, N_val) ++
              make_option(sloppy_quorum, SloppyQuorum),
    B = maybe_bucket_type(T, B0),
    Result =
        case riak_kv_util:consistent_object(B) of
            true ->
                consistent;
            false ->
                riak_client:get(B, K, GetOpts, C)
        end,
    case Result of
        consistent ->
            process(Req#rpbputreq{if_not_modified=undefined,
                                  if_none_match=undefined},
                    State#state{is_consistent = true});
        {ok, _} when NoneMatch ->
            {error, "match_found", State};
        {ok, O} when NotMod ->
            case erlify_rpbvc(PbVC) == riak_object:vclock(O) of
                true ->
                    process(Req#rpbputreq{if_not_modified=undefined,
                                          if_none_match=undefined},
                            State);
                _ ->
                    {error, "modified", State}
            end;
        {error, _} when NoneMatch ->
            process(Req#rpbputreq{if_not_modified=undefined,
                                  if_none_match=undefined},
                    State);
        {error, notfound} when NotMod ->
            {error, "notfound", State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

process(#rpbputreq{bucket=B0, type=T, key=K, vclock=PbVC, content=RpbContent,
                   w=W0, dw=DW0, pw=PW0, return_body=ReturnBody,
                   return_head=ReturnHead, timeout=Timeout, asis=AsIs,
                   n_val=N_val, sloppy_quorum=SloppyQuorum,
                   node_confirms=NodeConfirms0},
        #state{client=C} = State0) ->

    case K of
        undefined ->
            %% Generate a key, the user didn't supply one
            Key = list_to_binary(riak_core_util:unique_id_62()),
            ReturnKey = Key;
        _ ->
            Key = K,
            %% Don't return the key since we're not generating one
            ReturnKey = undefined
    end,
    B = maybe_bucket_type(T, B0),
    O0 = riak_object:new(B, Key, <<>>),
    O1 = update_rpbcontent(O0, RpbContent),
    O  = update_pbvc(O1, PbVC),
    %% erlang_protobuffs encodes as 1/0/undefined
    W = decode_quorum(W0),
    DW = decode_quorum(DW0),
    PW = decode_quorum(PW0),
    NodeConfirms = decode_quorum(NodeConfirms0),
    B = maybe_bucket_type(T, B0),
    Options = case ReturnBody of
                  1 -> [returnbody];
                  true -> [returnbody];
                  _ ->
                      case ReturnHead of
                          true -> [returnbody];
                          _ -> []
                      end
              end,
    {Options2, State} = 
        case State0#state.is_consistent of
            true ->
                {[{if_none_match, true}|Options],
                    State0#state{is_consistent = false}};
            _ ->
                {Options, State0}
        end,
    Opts =
        make_options([{w, W}, {dw, DW}, {pw, PW},
                        {node_confirms, NodeConfirms},
                        {timeout, Timeout}, {asis, AsIs},
                        {n_val, N_val},
                        {sloppy_quorum, SloppyQuorum}]) ++ Options2,
    case riak_client:put(O, Opts, C) of
        ok when is_binary(ReturnKey) ->
            PutResp = #rpbputresp{key = ReturnKey},
            {reply, PutResp, State};
        ok ->
            {reply, #rpbputresp{}, State};
        {ok, Obj} ->
            Contents = riak_object:get_contents(Obj),
            PbContents = case ReturnHead of
                             true ->
                                 %% Remove all the 'value' fields from the contents
                                 %% This is a rough equivalent of a REST HEAD
                                 %% request
                                 BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                                 riak_pb_kv_codec:encode_contents(BlankContents);
                             _ ->
                                 riak_pb_kv_codec:encode_contents(Contents)
                         end,
            PutResp = #rpbputresp{content = PbContents,
                                  vclock = pbify_rpbvc(riak_object:vclock(Obj)),
                                  key = ReturnKey
                                 },
            {reply, PutResp, State};
        {error, notfound} ->
            {reply, #rpbputresp{}, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end;

process(#rpbdelreq{bucket=B0, type=T, key=K, vclock=PbVc,
                   r=R0, w=W0, pr=PR0, pw=PW0, dw=DW0, rw=RW0,
                   timeout=Timeout, n_val=N_val, sloppy_quorum=SloppyQuorum},
        #state{client=C} = State) ->
    W = decode_quorum(W0),
    PW = decode_quorum(PW0),
    DW = decode_quorum(DW0),
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    RW = decode_quorum(RW0),

    B = maybe_bucket_type(T, B0),
    Options = make_options([{r, R}, {w, W}, {rw, RW}, {pr, PR}, {pw, PW}, 
                            {dw, DW}, {timeout, Timeout}, {n_val, N_val},
                            {sloppy_quorum, SloppyQuorum}]),
    Result =
        case PbVc of
            undefined ->
                riak_client:delete(B, K, Options, C);
            _ ->
                VClock = erlify_rpbvc(PbVc),
                riak_client:delete_vclock(B, K, VClock, Options, C)
        end,
    case Result of
        ok ->
            {reply, rpbdelresp, State};
        {error, notfound} ->  %% delete succeeds if already deleted
            {reply, rpbdelresp, State};
        {error, Reason} ->
            {error, {format, Reason}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec encode_nextgenrepl_response(
    intenal|internal_aaehash,
    #rpbfetchresp{},
    riak_object:riak_object()|
        {riak_object:bucket(), riak_object:key(), vclock:vclock()})
        -> #rpbfetchresp{}.
encode_nextgenrepl_response(Encoding, Resp, RObj) ->
    case Encoding of
        internal ->
            Resp;
        internal_aaehash ->
            {SegID, SegHash} =
                case RObj of
                    {B, K, TC} ->
                        BK = make_binarykey({B, K}),
                        leveled_tictac:tictac_hash(BK, lists:sort(TC));
                    RObj ->
                        BK = make_binarykey(RObj),
                        leveled_tictac:tictac_hash(
                            BK, lists:sort(riak_object:vclock(RObj)))
                    end,
            Resp#rpbfetchresp{segment_id = SegID, segment_hash = SegHash}
    end.

-spec make_binarykey(
    riak_object:riak_object()|{riak_object:bucket(), riak_object:key()})
        -> binary().
make_binarykey({B, K}) ->
    make_binarykey(B, K);
make_binarykey(RObj) ->
    make_binarykey(riak_object:bucket(RObj), riak_object:key(RObj)).

-spec unpack_keyclock_fun(#rpbkeysvalue{}) ->
        {riak_object:bucket(), riak_object:key(), vclock:vclock(), to_fetch}.
unpack_keyclock_fun(RpbKeysClock) ->
    Bucket =
        case RpbKeysClock#rpbkeysvalue.type of
            undefined ->
                RpbKeysClock#rpbkeysvalue.bucket;
            T ->
                {T, RpbKeysClock#rpbkeysvalue.bucket}
        end,
    {Bucket,
        RpbKeysClock#rpbkeysvalue.key,
        riak_object:decode_vclock(RpbKeysClock#rpbkeysvalue.value),
        to_fetch}.

-spec make_binarykey(riak_object:bucket(), riak_object:key()) -> binary().
%% @doc
%% Convert Bucket and Key into a single binary 
make_binarykey({Type, Bucket}, Key)
                    when is_binary(Type), is_binary(Bucket), is_binary(Key) ->
    <<Type/binary, Bucket/binary, Key/binary>>;
make_binarykey(Bucket, Key) when is_binary(Bucket), is_binary(Key) ->
    <<Bucket/binary, Key/binary>>.


%% Update riak_object with the pbcontent provided
update_rpbcontent(O0, RpbContent) ->
    {MetaData, Value} = riak_pb_kv_codec:decode_content(RpbContent),
    O1 = riak_object:update_metadata(O0, MetaData),
    riak_object:update_value(O1, Value).

%% Update riak_object with vector clock
update_pbvc(O0, PbVc) ->
    Vclock = erlify_rpbvc(PbVc),
    riak_object:set_vclock(O0, Vclock).

make_options(List) ->
    lists:flatmap(fun({K,V}) -> make_option(K,V) end, List).

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

%% Convert a vector clock to erlang
erlify_rpbvc(undefined) ->
    vclock:fresh();
erlify_rpbvc(<<>>) ->
    vclock:fresh();
erlify_rpbvc(PbVc) ->
    riak_object:decode_vclock(PbVc).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    riak_object:encode_vclock(Vc).

%% Construct a {Type, Bucket} tuple, if not working with the default bucket
maybe_bucket_type(undefined, B) ->
    B;
maybe_bucket_type(<<"default">>, B) ->
    B;
maybe_bucket_type(T, B) ->
    {T, B}.

%% always construct {Type, Bucket} tuple, filling in default type if needed
bucket_type(undefined, B) ->
    {<<"default">>, B};
bucket_type(T, B) ->
    {T, B}.

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

-define(CODE(Msg), riak_pb_codec:msg_code(Msg)).
-define(PAYLOAD(Msg), riak_kv_pb:encode_msg(Msg)).

empty_bucket_key_test_() ->
    Name = "empty_bucket_key_test",
    SetupFun =  fun (load) ->
                        application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
                        application:set_env(riak_api, pb_ip, "127.0.0.1"),
                        application:set_env(riak_api, pb_port, 32767);
                    (_) -> ok end,
    {setup,
     riak_kv_test_util:common_setup(Name, SetupFun),
     riak_kv_test_util:common_cleanup(Name, SetupFun),
     [{"RpbPutReq with empty key is disallowed",
       ?_assertMatch([0|_], request(#rpbputreq{bucket = <<"foo">>,
                                               key = <<>>,
                                               content=#rpbcontent{value = <<"dummy">>}}))},
      {"RpbPutReq with empty bucket is disallowed",
       ?_assertMatch([0|_], request(#rpbputreq{bucket = <<>>,
                                               key = <<"foo">>,
                                               content=#rpbcontent{value = <<"dummy">>}}))},
      {"RpbGetReq with empty key is disallowed",
       ?_assertMatch([0|_], request(#rpbgetreq{bucket = <<"foo">>,
                                               key = <<>>}))},
      {"RpbGetReq with empty bucket is disallowed",
       ?_assertMatch([0|_], request(#rpbgetreq{bucket = <<>>,
                                               key = <<"foo">>}))}]}.

%% Utility funcs copied from riak_api/test/pb_service_test.erl

request(Msg) when is_tuple(Msg) andalso is_atom(element(1, Msg)) ->
    request(?CODE(element(1,Msg)), iolist_to_binary(?PAYLOAD(Msg))).

request(Code, Payload) when is_binary(Payload), is_integer(Code) ->
    Connection = new_connection(),
    ?assertMatch({ok, _}, Connection),
    {ok, Socket} = Connection,
    request(Code, Payload, Socket).

request(Code, Payload, Socket) when is_binary(Payload), is_integer(Code) ->
    ?assertEqual(ok, gen_tcp:send(Socket, <<Code:8, Payload/binary>>)),
    Result = gen_tcp:recv(Socket, 0),
    ?assertMatch({ok, _}, Result),
    {ok, Response} = Result,
    Response.

new_connection() ->
    new_connection([{packet,4}, {header, 1}]).

new_connection(Options) ->
    Host = app_helper:get_env(riak_api, pb_ip),
    Port = app_helper:get_env(riak_api, pb_port),
    gen_tcp:connect(Host, Port, [binary, {active, false},{nodelay, true}|Options]).

-endif.
