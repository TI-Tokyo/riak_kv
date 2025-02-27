%% -------------------------------------------------------------------
%%
%% riak_kv_wm_object: Webmachine resource for KV object level operations.
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Resource for serving Riak objects over HTTP.
%%
%% URLs that begin with `/types' are necessary for the new bucket
%% types implementation in Riak 2.0, those that begin with `/buckets'
%% are for the default bucket type, and `/riak' is an old URL style,
%% also only works for the default bucket type.
%%
%% It is possible to reconfigure the `/riak' prefix but that seems to
%% be rarely if ever used.
%%
%% ```
%% POST /types/Type/buckets/Bucket/keys
%% POST /buckets/Bucket/keys
%% POST /riak/Bucket'''
%%
%%   Allow the server to choose a key for the data.
%%
%% ```
%% GET /types/Type/buckets/Bucket/keys/Key
%% GET /buckets/Bucket/keys/Key
%% GET /riak/Bucket/Key'''
%%
%%   Get the data stored in the named Bucket under the named Key.
%%
%%   Content-type of the response will be taken from the
%%   Content-type was used in the request that stored the data.
%%
%%   Additional headers will include:
%% <ul>
%%     <li>`X-Riak-Vclock': The vclock of the object</li>
%%     <li>`Link': The links the object has</li>
%%     <li>`Etag': The Riak "vtag" metadata of the object</li>
%%     <li>`Last-Modified': The last-modified time of the object</li>
%%     <li>`Encoding': The value of the incoming Encoding header from
%%       the request that stored the data.</li>
%%     <li>`X-Riak-Meta-': Any headers prefixed by X-Riak-Meta- supplied
%%       on PUT are returned verbatim</li>
%% </ul>
%%
%%   Specifying the query param `r=R', where `R' is an integer will
%%   cause Riak to use `R' as the r-value for the read request. A
%%   default r-value of 2 will be used if none is specified.
%%
%%   If the object is found to have siblings (only possible if the
%%   bucket property `allow_mult' is true), then
%%   Content-type will be `text/plain'; `Link', `Etag', and `Last-Modified'
%%   headers will be omitted; and the body of the response will
%%   be a list of the vtags of each sibling.  To request a specific
%%   sibling, include the query param `vtag=V', where `V' is the vtag
%%   of the sibling you want.
%%
%% ```
%% PUT /types/Type/buckets/Bucket/keys/Key
%% PUT /buckets/Bucket/keys/Key
%% PUT /riak/Bucket/Key'''
%%
%%   Store new data in the named Bucket under the named Key.
%%
%%   A Content-type header *must* be included in the request.  The
%%   value of this header will be used in the response to subsequent
%%   GET requests.
%%
%%   The body of the request will be stored literally as the value
%%   of the riak_object, and will be served literally as the body of
%%   the response to subsequent GET requests.
%%
%%   Include an X-Riak-Vclock header to modify data without creating
%%   siblings.
%%
%%   Include a Link header to set the links of the object.
%%
%%   Include an Encoding header if you would like an Encoding header
%%   to be included in the response to subsequent GET requests.
%%
%%   Include custom metadata using headers prefixed with X-Riak-Meta-.
%%   They will be returned verbatim on subsequent GET requests.
%%
%%   Specifying the query param `w=W', where W is an integer will
%%   cause Riak to use W as the w-value for the write request. A
%%   default w-value of 2 will be used if none is specified.
%%
%%   Specifying the query param `dw=DW', where DW is an integer will
%%   cause Riak to use DW as the dw-value for the write request. A
%%   default dw-value of 2 will be used if none is specified.
%%
%%   Specifying the query param `r=R', where R is an integer will
%%   cause Riak to use R as the r-value for the read request (used
%%   to determine whether or not the resource exists). A default
%%   r-value of 2 will be used if none is specified.
%%
%% ```
%% POST /types/Type/buckets/Bucket/keys/Key
%% POST /buckets/Bucket/keys/Key
%% POST /riak/Bucket/Key'''
%%
%%   Equivalent to `PUT /riak/Bucket/Key' (useful for clients that
%%   do not support the PUT method).
%%
%% ```
%% DELETE /types/Type/buckets/Bucket/keys/Key (with bucket-type)
%% DELETE /buckets/Bucket/keys/Key (NEW)
%% DELETE /riak/Bucket/Key (OLD)'''
%%
%%   Delete the data stored in the named Bucket under the named Key.

-module(riak_kv_wm_object).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         is_authorized/2,
         forbidden/2,
         allowed_methods/2,
         allow_missing_post/2,
         malformed_request/2,
         resource_exists/2,
         is_conflict/2,
         last_modified/2,
         generate_etag/2,
         content_types_provided/2,
         charsets_provided/2,
         encodings_provided/2,
         content_types_accepted/2,
         post_is_create/2,
         create_path/2,
         process_post/2,
         produce_doc_body/2,
         accept_doc_body/2,
         produce_sibling_message_body/2,
         produce_multipart_body/2,
         multiple_choices/2,
         delete_resource/2
        ]).

-record(ctx, {api_version,  %% integer() - Determine which version of the API to use.
              bucket_type,  %% binary() - Bucket type (from uri)
              type_exists,  %% bool() - Type exists as riak_core_bucket_type
              bucket,       %% binary() - Bucket name (from uri)
              key,          %% binary() - Key (from uri)
              client,       %% riak_client() - the store client
              r,            %% integer() - r-value for reads
              w,            %% integer() - w-value for writes
              dw,           %% integer() - dw-value for writes
              rw,           %% integer() - rw-value for deletes
              pr,           %% integer() - number of primary nodes required in preflist on read
              pw,           %% integer() - number of primary nodes required in preflist on write
              node_confirms,%% integer() - number of physically diverse nodes required in preflist on write
              basic_quorum, %% boolean() - whether to use basic_quorum
              notfound_ok,  %% boolean() - whether to treat notfounds as successes
              asis,         %% boolean() - whether to send the put without modifying the vclock
              sync_on_write,%% string() - sync on write behaviour to pass to backend
              prefix,       %% string() - prefix for resource uris
              riak,         %% local | {node(), atom()} - params for riak client
              doc,          %% {ok, riak_object()}|{error, term()} - the object found
              vtag,         %% string() - vtag the user asked for
              bucketprops,  %% proplist() - properties of the bucket
              links,        %% [link()] - links of the object
              index_fields, %% [index_field()]
              method,       %% atom() - HTTP method for the request
              ctype,        %% string() - extracted content-type provided
              charset,      %% string() | undefined - extracted character set provided
              timeout,      %% integer() - passed-in timeout value in ms
              security      %% security context
             }).

-ifdef(namespaced_types).
-type riak_kv_wm_object_dict() :: dict:dict().
-else.
-type riak_kv_wm_object_dict() :: dict().
-endif.

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

-type context() :: #ctx{}.
-type request_data() :: #wm_reqdata{}.

-type validation_function() ::
    fun((request_data(), context()) ->
        {boolean()|{halt, pos_integer()}, request_data(), context()}).

-type link() :: {{Bucket::binary(), Key::binary()}, Tag::binary()}.

-define(DEFAULT_TIMEOUT, 60000).
-define(V1_BUCKET_REGEX, "/([^/]+)>; ?rel=\"([^\"]+)\"").
-define(V1_KEY_REGEX, "/([^/]+)/([^/]+)>; ?riaktag=\"([^\"]+)\"").
-define(V2_BUCKET_REGEX, "</buckets/([^/]+)>; ?rel=\"([^\"]+)\"").
-define(V2_KEY_REGEX,
            "</buckets/([^/]+)/keys/([^/]+)>; ?riaktag=\"([^\"]+)\"").

-spec init(proplists:proplist()) -> {ok, context()}.
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' and 'riak' properties from the dispatch args.
init(Props) ->
    {ok, #ctx{api_version=proplists:get_value(api_version, Props),
              prefix=proplists:get_value(prefix, Props),
              riak=proplists:get_value(riak, Props),
              bucket_type=proplists:get_value(bucket_type, Props)}}.

-spec service_available(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not a connection to Riak
%%      can be established.  This function also takes this
%%      opportunity to extract the 'bucket' and 'key' path
%%      bindings from the dispatch, as well as any vtag
%%      query parameter.
service_available(RD, Ctx0=#ctx{riak=RiakProps}) ->
    Ctx = ensure_bucket_type(RD, Ctx0),
    ClientID = riak_kv_wm_utils:get_client_id(RD),
    case riak_kv_wm_utils:get_riak_client(RiakProps, ClientID) of
        {ok, C} ->
            Bucket =
                case wrq:path_info(bucket, RD) of
                    undefined ->
                        undefined;
                    B ->
                        list_to_binary(
                            riak_kv_wm_utils:maybe_decode_uri(RD, B))
                end,
            Key =
                case wrq:path_info(key, RD) of
                    undefined ->
                        undefined;
                    K ->
                        list_to_binary(
                            riak_kv_wm_utils:maybe_decode_uri(RD, K))
                end,
            {true,
                RD,
                Ctx#ctx{
                    method=wrq:method(RD),
                    client=C,
                    bucket=Bucket,
                    key=Key,
                    vtag=wrq:get_qs_value(?Q_VTAG, RD)}};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

is_authorized(ReqData, Ctx) ->
    case riak_api_web_security:is_authorized(ReqData) of
        false ->
            {"Basic realm=\"Riak\"", ReqData, Ctx};
        {true, SecContext} ->
            {true, ReqData, Ctx#ctx{security=SecContext}};
        insecure ->
            %% XXX 301 may be more appropriate here, but since the http and
            %% https port are different and configurable, it is hard to figure
            %% out the redirect URL to serve.
            {{halt, 426}, wrq:append_to_resp_body(<<"Security is enabled and "
                    "Riak does not accept credentials over HTTP. Try HTTPS "
                    "instead.">>, ReqData), Ctx}
    end.

-spec forbidden(#wm_reqdata{}, context()) -> term().
forbidden(RD, Ctx) ->
    case riak_kv_wm_utils:is_forbidden(RD) of
        true ->
            {true, RD, Ctx};
        false ->
            validate(RD, Ctx)
    end.

-spec validate(#wm_reqdata{}, context()) -> term().
validate(RD, Ctx=#ctx{security=undefined}) ->
    validate_resource(
        RD, Ctx, riak_kv_wm_utils:method_to_perm(Ctx#ctx.method));
validate(RD, Ctx=#ctx{security=Security}) ->
    Perm = riak_kv_wm_utils:method_to_perm(Ctx#ctx.method),
    Res = riak_core_security:check_permission({Perm,
                                              {Ctx#ctx.bucket_type,
                                              Ctx#ctx.bucket}},
                                              Security),
    maybe_validate_resource(Res, RD, Ctx, Perm).

-spec maybe_validate_resource(
        term(), #wm_reqdata{}, context(), string()) -> term().
maybe_validate_resource({false, Error, _}, RD, Ctx, _Perm) ->
    RD1 = wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD),
    {true, wrq:append_to_resp_body(
             unicode:characters_to_binary(Error, utf8, utf8),
             RD1), Ctx};
maybe_validate_resource({true, _}, RD, Ctx, Perm) ->
    validate_resource(RD, Ctx, Perm).

-spec validate_resource(#wm_reqdata{}, context(), string()) -> term().
validate_resource(RD, Ctx, Perm) when Perm == "riak_kv.get" ->
    %% Ensure the key is here, otherwise 404
    %% we do this early as it used to be done in the
    %% malformed check, so the rest of the resource
    %% assumes that the key is present.
    validate_doc(RD, Ctx);
validate_resource(RD, Ctx, _Perm) ->
    %% Ensure the bucket type exists, otherwise 404 early.
    validate_bucket_type(RD, Ctx).

%% @doc Detects whether fetching the requested object results in an
%% error.
validate_doc(RD, Ctx) ->
    DocCtx = ensure_doc(Ctx),
    case DocCtx#ctx.doc of
        {error, Reason} ->
            handle_common_error(Reason, RD, DocCtx);
        _ ->
            {false, RD, DocCtx}
    end.

%% @doc Detects whether the requested object's bucket-type exists.
validate_bucket_type(RD, Ctx) ->
    case Ctx#ctx.type_exists of
        true ->
            {false, RD, Ctx};
        false ->
            handle_common_error(bucket_type_unknown, RD, Ctx)
    end.

-spec allowed_methods(#wm_reqdata{}, context()) ->
    {[atom()], #wm_reqdata{}, context()}.
%% @doc Get the list of methods this resource supports.
allowed_methods(RD, Ctx) ->
    {['HEAD', 'GET', 'POST', 'PUT', 'DELETE'], RD, Ctx}.

-spec allow_missing_post(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Makes POST and PUT equivalent for creating new
%%      bucket entries.
allow_missing_post(RD, Ctx) ->
    {true, RD, Ctx}.

-spec malformed_request(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format is checked to be valid JSON, including
%%      a "props" object for a bucket-PUT.  Body format
%%      is not tested for a key-level request (since the
%%      body may be any content the client desires).
%%      Query parameters r, w, dw, and rw are checked to
%%      be valid integers.  Their values are stored in
%%      the context() at this time.
%%      Link headers are checked for the form:
%%        &lt;/Prefix/Bucket/Key&gt;; riaktag="Tag",...
%%      The parsed links are stored in the context()
%%      at this time.
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'POST'
                                orelse Ctx#ctx.method =:= 'PUT' ->
    malformed_request([fun malformed_content_type/2,
                       fun malformed_timeout_param/2,
                       fun malformed_rw_params/2,
                       fun malformed_link_headers/2,
                       fun malformed_index_headers/2],
                      RD, Ctx);
malformed_request(RD, Ctx) ->
    malformed_request([fun malformed_timeout_param/2,
                       fun malformed_rw_params/2], RD, Ctx).

%% @doc Given a list of 2-arity funs, threads through the request data
%% and context, returning as soon as a single fun discovers a
%% malformed request or halts.
-spec malformed_request(
        list(validation_function()), request_data(), context()) ->
            {boolean() | {halt, pos_integer()}, request_data(), context()}.
malformed_request([], RD, Ctx) ->
    {false, RD, Ctx};
malformed_request([H|T], RD, Ctx) ->
    case H(RD, Ctx) of
        {true, _, _} = Result -> Result;
        {{halt,_}, _, _} = Halt -> Halt;
        {false, RD1, Ctx1} ->
            malformed_request(T, RD1, Ctx1)
    end.

%% @doc Detects whether the Content-Type header is missing on
%% PUT/POST.
%% This should probably result in a 415 using the known_content_type callback
malformed_content_type(RD, Ctx) ->
    case wrq:get_req_header(?HEAD_CTYPE, RD) of
        undefined ->
            {true, missing_content_type(RD), Ctx};
        RawCType ->
            [ContentType|RawParams] = string:tokens(RawCType, "; "),
            Params = [ list_to_tuple(string:tokens(P, "=")) || P <- RawParams],
            Charset = proplists:get_value("charset", Params),
            {false, RD, Ctx#ctx{ctype = ContentType, charset = Charset}}
    end.

-spec malformed_timeout_param(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the timeout parameter is are a
%%      string-encoded integer.  Store the integer value
%%      in context() if so.
malformed_timeout_param(RD, Ctx) ->
    case wrq:get_qs_value("timeout", RD) of
        undefined ->
            {false, RD, Ctx};
        TimeoutStr ->
            try
                Timeout = list_to_integer(TimeoutStr),
                {false, RD, Ctx#ctx{timeout=Timeout}}
            catch
                _:_ ->
                    {true,
                        wrq:append_to_resp_body(
                            io_lib:format("Bad timeout "
                                            "value ~p~n",
                                            [TimeoutStr]),
                        wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
                     Ctx}
            end
    end.

-spec malformed_rw_params(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that r, w, dw, and rw query parameters are
%%      string-encoded integers.  Store the integer values
%%      in context() if so.
malformed_rw_params(RD, Ctx) ->
    Res =
    lists:foldl(fun malformed_rw_param/2,
                {false, RD, Ctx},
                [{#ctx.r, "r", "default"},
                 {#ctx.w, "w", "default"},
                 {#ctx.dw, "dw", "default"},
                 {#ctx.rw, "rw", "default"},
                 {#ctx.pw, "pw", "default"},
                 {#ctx.node_confirms, "node_confirms", "default"},
                 {#ctx.pr, "pr", "default"}]),
    Res2 =
    lists:foldl(fun malformed_custom_param/2,
                 Res,
                 [{#ctx.sync_on_write,
                     "sync_on_write",
                     "default",
                     [default, backend, one, all]}]),
    lists:foldl(fun malformed_boolean_param/2,
                Res2,
                [{#ctx.basic_quorum, "basic_quorum", "default"},
                 {#ctx.notfound_ok, "notfound_ok", "default"},
                 {#ctx.asis, "asis", "false"}]).

-spec malformed_rw_param({Idx::integer(), Name::string(), Default::string()},
                         {boolean(), #wm_reqdata{}, context()}) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that a specific r, w, dw, or rw query param is a
%%      string-encoded integer.  Store its result in context() if it
%%      is, or print an error message in #wm_reqdata{} if it is not.
malformed_rw_param({Idx, Name, Default}, {Result, RD, Ctx}) ->
    case catch normalize_rw_param(wrq:get_qs_value(Name, Default, RD)) of
        P when (is_atom(P) orelse is_integer(P)) ->
            {Result, RD, setelement(Idx, Ctx, P)};
        _ ->
            {true,
             wrq:append_to_resp_body(
               io_lib:format("~s query parameter must be an integer or "
                   "one of the following words: 'one', 'quorum' or 'all'~n",
                             [Name]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

-spec malformed_custom_param({Idx::integer(),
                                    Name::string(),
                                    Default::string(),
                                    AllowedValues::[atom()]},
                                {boolean(), #wm_reqdata{}, context()}) ->
   {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that a custom parameter is one of the AllowedValues
%% Store its result in context() if it is, or print an error message
%% in #wm_reqdata{} if it is not.
malformed_custom_param({Idx, Name, Default, AllowedValues}, {Result, RD, Ctx}) ->
    AllowedValueTuples = [{V} || V <- AllowedValues],
    Option=
        lists:keyfind(
            list_to_atom(
                string:to_lower(
                    wrq:get_qs_value(Name, Default, RD))),
                1,
                AllowedValueTuples),
    case Option of
        false ->
            ErrorText =
                "~s query parameter must be one of the following words: ~p~n",
            {true,
             wrq:append_to_resp_body(
               io_lib:format(ErrorText, [Name, AllowedValues]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        _ ->
            {Value} = Option,
            {Result, RD, setelement(Idx, Ctx, Value)}
    end.

%% @doc Check that a specific query param is a
%%      string-encoded boolean.  Store its result in context() if it
%%      is, or print an error message in #wm_reqdata{} if it is not.
malformed_boolean_param({Idx, Name, Default}, {Result, RD, Ctx}) ->
    case string:to_lower(wrq:get_qs_value(Name, Default, RD)) of
        "true" ->
            {Result, RD, setelement(Idx, Ctx, true)};
        "false" ->
            {Result, RD, setelement(Idx, Ctx, false)};
        "default" ->
            {Result, RD, setelement(Idx, Ctx, default)};
        _ ->
            {true,
            wrq:append_to_resp_body(
              io_lib:format("~s query parameter must be true or false~n",
                            [Name]),
              wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

normalize_rw_param("backend") -> backend;
normalize_rw_param("default") -> default;
normalize_rw_param("one") -> one;
normalize_rw_param("quorum") -> quorum;
normalize_rw_param("all") -> all;
normalize_rw_param(V) -> list_to_integer(V).

-spec malformed_link_headers(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the Link header in the request() is valid.
%%      Store the parsed links in context() if the header is valid,
%%      or print an error in #wm_reqdata{} if it is not.
%%      A link header should be of the form:
%%        &lt;/Prefix/Bucket/Key&gt;; riaktag="Tag",...
malformed_link_headers(RD, Ctx) ->
    case catch get_link_heads(RD, Ctx) of
        Links when is_list(Links) ->
            {false, RD, Ctx#ctx{links=Links}};
        _Error when Ctx#ctx.api_version == 1->
            {true,
             wrq:append_to_resp_body(
               io_lib:format("Invalid Link header. Links must be of the form~n"
                             "</~s/BUCKET/KEY>; riaktag=\"TAG\"~n",
                             [Ctx#ctx.prefix]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        _Error when Ctx#ctx.api_version == 2 ->
            {true,
             wrq:append_to_resp_body(
               io_lib:format("Invalid Link header. Links must be of the form~n"
                             "</buckets/BUCKET/keys/KEY>; riaktag=\"TAG\"~n", []),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}

    end.

-spec malformed_index_headers(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Check that the Index headers (HTTP headers prefixed with index_")
%%      are valid. Store the parsed headers in context() if valid,
%%      or print an error in #wm_reqdata{} if not.
%%      An index field should be of the form "index_fieldname_type"
malformed_index_headers(RD, Ctx) ->
    %% Get a list of index_headers...
    IndexFields1 = extract_index_fields(RD),

    %% Validate the fields. If validation passes, then the index
    %% headers are correctly formed.
    case riak_index:parse_fields(IndexFields1) of
        {ok, IndexFields2} ->
            {false, RD, Ctx#ctx { index_fields=IndexFields2 }};
        {error, Reasons} ->
            {true,
             wrq:append_to_resp_body(
               [riak_index:format_failure_reason(X) || X <- Reasons],
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

-spec extract_index_fields(#wm_reqdata{}) -> proplists:proplist().
%% @doc Extract fields from headers prefixed by "x-riak-index-" in the
%%      client's PUT request, to be indexed at write time.
extract_index_fields(RD) ->
    PrefixSize = length(?HEAD_INDEX_PREFIX),
    {ok, RE} = re:compile(",\\s"),
    F =
        fun({K,V}, Acc) ->
            KList = riak_kv_wm_utils:any_to_list(K),
            case lists:prefix(?HEAD_INDEX_PREFIX, string:to_lower(KList)) of
                true ->
                    %% Isolate the name of the index field.
                    IndexField =
                        list_to_binary(
                            element(2, lists:split(PrefixSize, KList))),

                    %% HACK ALERT: Split values on comma. The HTTP
                    %% spec allows for comma separated tokens
                    %% where the tokens can be quoted strings. We
                    %% don't currently support quoted strings.
                    %% (http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html)
                    Values = re:split(V, RE, [{return, binary}]),
                    [{IndexField, X} || X <- Values] ++ Acc;
                false ->
                    Acc
            end
        end,
    lists:foldl(F, [], mochiweb_headers:to_list(wrq:req_headers(RD))).

-spec content_types_provided(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Producer::atom()}], #wm_reqdata{}, context()}.
%% @doc List the content types available for representing this resource.
%%      The content-type for a key-level request is the content-type that
%%      was used in the PUT request that stored the document in Riak.
content_types_provided(RD, Ctx=#ctx{method=Method, ctype=ContentType})
            when Method =:= 'PUT'; Method =:= 'POST' ->
    {[{ContentType, produce_doc_body}], RD, Ctx};
content_types_provided(RD, Ctx=#ctx{method=Method})
            when Method =:= 'DELETE' ->
    {[{"text/html", to_html}], RD, Ctx};
content_types_provided(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    %% we can assume DocCtx#ctx.doc is {ok,Doc} because of malformed_request
    case select_doc(DocCtx) of
        {MD, V} ->
            {[{get_ctype(MD,V), produce_doc_body}], RD, DocCtx};
        multiple_choices ->
            {[{"text/plain", produce_sibling_message_body},
              {"multipart/mixed", produce_multipart_body}], RD, DocCtx}
    end.

-spec charsets_provided(#wm_reqdata{}, context()) ->
    {no_charset|[{Charset::string(), Producer::function()}],
     #wm_reqdata{}, context()}.
%% @doc List the charsets available for representing this resource.
%%      The charset for a key-level request is the charset that was used
%%      in the PUT request that stored the document in Riak (none if
%%      no charset was specified at PUT-time).
charsets_provided(RD, Ctx=#ctx{method=Method})
            when Method =:= 'PUT'; Method =:= 'POST' ->
    case Ctx#ctx.charset of
        undefined ->
            {no_charset, RD, Ctx};
        Charset ->
            {[{Charset, fun(X) -> X end}], RD, Ctx}
    end;
charsets_provided(RD, Ctx=#ctx{method=Method})
            when Method =:= 'DELETE' ->
    {no_charset, RD, Ctx};
charsets_provided(RD, Ctx0) ->
    DocCtx = ensure_doc(Ctx0),
    case DocCtx#ctx.doc of
        {ok, _} ->
            case select_doc(DocCtx) of
                {MD, _} ->
                    case dict:find(?MD_CHARSET, MD) of
                        {ok, CS} ->
                            {[{CS, fun(X) -> X end}], RD, DocCtx};
                        error ->
                            {no_charset, RD, DocCtx}
                    end;
                multiple_choices ->
                    {no_charset, RD, DocCtx}
            end;
        {error, _} ->
            {no_charset, RD, DocCtx}
    end.

-spec encodings_provided(#wm_reqdata{}, context()) ->
    {[{Encoding::string(), Producer::function()}], #wm_reqdata{}, context()}.
%% @doc List the encodings available for representing this resource.
%%      The encoding for a key-level request is the encoding that was
%%      used in the PUT request that stored the document in Riak, or
%%      "identity" and "gzip" if no encoding was specified at PUT-time.
encodings_provided(RD, Ctx0) ->
    DocCtx =
        case Ctx0#ctx.method of
            UpdM when UpdM =:= 'PUT'; UpdM =:= 'POST'; UpdM =:= 'DELETE' ->
                Ctx0;
            _ ->
                ensure_doc(Ctx0)
        end,
    case DocCtx#ctx.doc of
        {ok, _} ->
            case select_doc(DocCtx) of
                {MD, _} ->
                    case dict:find(?MD_ENCODING, MD) of
                        {ok, Enc} ->
                            {[{Enc, fun(X) -> X end}], RD, DocCtx};
                        error ->
                            {riak_kv_wm_utils:default_encodings(), RD, DocCtx}
                    end;
                multiple_choices ->
                    {riak_kv_wm_utils:default_encodings(), RD, DocCtx}
            end;
        _ ->
            {riak_kv_wm_utils:default_encodings(), RD, DocCtx}
    end.

-spec content_types_accepted(#wm_reqdata{}, context()) ->
    {[{ContentType::string(), Acceptor::atom()}],
     #wm_reqdata{}, context()}.
%% @doc Get the list of content types this resource will accept.
%%      Whatever content type is specified by the Content-Type header
%%      of a key-level PUT request will be accepted by this resource.
%%      (A key-level put *must* include a Content-Type header.)
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header(?HEAD_CTYPE, RD) of
        undefined ->
            %% user must specify content type of the data
            {[], RD, Ctx};
        CType ->
            Media = hd(string:tokens(CType, ";")),
            case string:tokens(Media, "/") of
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    {[{Media, accept_doc_body}], RD, Ctx};
                _ ->
                    {[],
                     wrq:set_resp_header(
                       ?HEAD_CTYPE,
                       "text/plain",
                       wrq:set_resp_body(
                         ["\"", Media, "\""
                          " is not a valid media type"
                          " for the Content-type header.\n"],
                         RD)),
                     Ctx}
            end
    end.

-spec resource_exists(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether or not the requested item exists.
%%      Documents exists if a read request to Riak returns {ok, riak_object()},
%%      and either no vtag query parameter was specified, or the value of the
%%      vtag param matches the vtag of some value of the Riak object.
resource_exists(RD, Ctx0) ->
    Method = Ctx0#ctx.method,
    ToFetch =
        case Method of
            UpdM when UpdM =:= 'PUT'; UpdM =:= 'POST'; UpdM =:= 'DELETE' ->
                conditional_headers_present(RD) == true;
            _ ->
                true
        end,
    case ToFetch of
        true ->
            DocCtx = ensure_doc(Ctx0),
            case DocCtx#ctx.doc of
                {ok, Doc} ->
                    case DocCtx#ctx.vtag of
                        undefined ->
                            {true, RD, DocCtx};
                        Vtag ->
                            MDs = riak_object:get_metadatas(Doc),
                            {lists:any(
                                    fun(M) ->
                                        dict:fetch(?MD_VTAG, M) =:= Vtag
                                    end,
                                    MDs),
                                RD,
                                DocCtx#ctx{vtag=Vtag}}
                    end;
                {error, _} ->
                    %% This should never actually be reached because all the
                    %% error conditions from ensure_doc are handled up in
                    %% malformed_request.
                    {false, RD, DocCtx}
            end;
        false ->
            % Fake it - rather than fetch to see.  If we're deleting we assume
            % it does exist, and if PUT/POST, assume it doesn't
            case Method of
                'DELETE' ->
                    {true, RD, Ctx0};
                _ ->
                    {false, RD, Ctx0}
            end
    end.

-spec is_conflict(request_data(), context()) ->
        {boolean(), request_data(), context()}.
is_conflict(RD, Ctx) ->
    case {Ctx#ctx.method, wrq:get_req_header(?HEAD_IF_NOT_MODIFIED, RD)} of
        {_ , undefined} ->
            {false, RD, Ctx};
        {UpdM, NotModifiedClock} when UpdM =:= 'PUT'; UpdM =:= 'POST' ->
            case Ctx#ctx.doc of
                {ok, Obj} ->
                    InClock =
                        riak_object:decode_vclock(
                            base64:decode(NotModifiedClock)),
                    CurrentClock =
                        riak_object:vclock(Obj),
                    {not vclock:equal(InClock, CurrentClock), RD, Ctx};
                _ ->
                    {true, RD, Ctx}
            end;
        _ ->
            {false, RD, Ctx}
    end.

-spec conditional_headers_present(request_data()) -> boolean().
conditional_headers_present(RD) ->
    NoneMatch =
        (wrq:get_req_header("If-None-Match", RD) =/= undefined),
    Match =
        (wrq:get_req_header("If-Match", RD) =/= undefined),
    UnModifiedSince =
        (wrq:get_req_header("If-Unmodified-Since", RD) =/= undefined),
    NotModified =
        (wrq:get_req_header(?HEAD_IF_NOT_MODIFIED, RD) =/= undefined),
    (NoneMatch or Match or UnModifiedSince or NotModified).


-spec post_is_create(#wm_reqdata{}, context()) ->
    {boolean(), #wm_reqdata{}, context()}.
%% @doc POST is considered a document-creation operation for bucket-level
%%      requests (this makes webmachine call create_path/2, where the key
%%      for the created document will be chosen).
post_is_create(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-POST is create
    {true, RD, Ctx};
post_is_create(RD, Ctx) ->
    %% key-POST is not create
    {false, RD, Ctx}.

-spec create_path(#wm_reqdata{}, context()) ->
    {string(), #wm_reqdata{}, context()}.
%% @doc Choose the Key for the document created during a bucket-level POST.
%%      This function also sets the Location header to generate a
%%      201 Created response.
create_path(RD, Ctx=#ctx{prefix=P, bucket_type=T, bucket=B, api_version=V}) ->
    K = riak_core_util:unique_id_62(),
    {K,
     wrq:set_resp_header("Location",
                         riak_kv_wm_utils:format_uri(T, B, K, P, V),
                         RD),
     Ctx#ctx{key=list_to_binary(K)}}.

-spec process_post(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Pass-through for key-level requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) -> accept_doc_body(RD, Ctx).

-spec accept_doc_body(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Store the data the client is PUTing in the document.
%%      This function translates the headers and body of the HTTP request
%%      into their final riak_object() form, and executes the Riak put.
accept_doc_body(
        RD,
        Ctx=#ctx{
            bucket_type=T, bucket=B, key=K, client=C,
            links=L, ctype=CType, charset=Charset,
            index_fields=IF}) ->
    Doc0 = riak_object:new(riak_kv_wm_utils:maybe_bucket_type(T,B), K, <<>>),
    VclockDoc = riak_object:set_vclock(Doc0, decode_vclock_header(RD)),
    UserMeta = extract_user_meta(RD),
    CTypeMD = dict:store(?MD_CTYPE, CType, dict:new()),
    CharsetMD =
        if Charset /= undefined ->
                dict:store(?MD_CHARSET, Charset, CTypeMD);
            true ->
                CTypeMD
        end,
    EncMD =
        case wrq:get_req_header(?HEAD_ENCODING, RD) of
            undefined -> CharsetMD;
            E -> dict:store(?MD_ENCODING, E, CharsetMD)
        end,
    LinkMD = dict:store(?MD_LINKS, L, EncMD),
    UserMetaMD = dict:store(?MD_USERMETA, UserMeta, LinkMD),
    IndexMD = dict:store(?MD_INDEX, IF, UserMetaMD),
    MDDoc = riak_object:update_metadata(VclockDoc, IndexMD),
    Doc =
        riak_object:update_value(
            MDDoc, riak_kv_wm_utils:accept_value(CType, wrq:req_body(RD))),
    Options0 =
        case wrq:get_qs_value(?Q_RETURNBODY, RD) of
            ?Q_TRUE -> [returnbody];
            _ -> []
        end,
    Options = make_options(Options0, Ctx),
    NoneMatch = (wrq:get_req_header("If-None-Match", RD) =/= undefined),
    Options2 = case riak_kv_util:consistent_object(B) and NoneMatch of
                   true ->
                       [{if_none_match, true}|Options];
                   false ->
                       Options
               end,
    case riak_client:put(Doc, Options2, C) of
        {error, Reason} ->
            handle_common_error(Reason, RD, Ctx);
        ok ->
            {true, RD, Ctx#ctx{doc={ok, Doc}}};
        {ok, RObj} ->
            DocCtx = Ctx#ctx{doc={ok, RObj}},
            HasSiblings = (select_doc(DocCtx) == multiple_choices),
            send_returnbody(RD, DocCtx, HasSiblings)
    end.

%% Handle the no-sibling case. Just send the object.
send_returnbody(RD, DocCtx, _HasSiblings = false) ->
    {Body, DocRD, DocCtx2} = produce_doc_body(RD, DocCtx),
    {DocRD2, DocCtx3} = add_conditional_headers(DocRD, DocCtx2),
    {true, wrq:append_to_response_body(Body, DocRD2), DocCtx3};

%% Handle the sibling case. Send either the sibling message body, or a
%% multipart body, depending on what the client accepts.
send_returnbody(RD, DocCtx, _HasSiblings = true) ->
    AcceptHdr = wrq:get_req_header("Accept", RD),
    PossibleTypes = ["multipart/mixed", "text/plain"],
    case webmachine_util:choose_media_type(PossibleTypes, AcceptHdr) of
        "multipart/mixed"  ->
            {Body, DocRD, DocCtx2} = produce_multipart_body(RD, DocCtx),
            {DocRD2, DocCtx3} = add_conditional_headers(DocRD, DocCtx2),
            {true, wrq:append_to_response_body(Body, DocRD2), DocCtx3};
        _ ->
            {Body, DocRD, DocCtx2} = produce_sibling_message_body(RD, DocCtx),
            {DocRD2, DocCtx3} = add_conditional_headers(DocRD, DocCtx2),
            {true, wrq:append_to_response_body(Body, DocRD2), DocCtx3}
    end.

%% Add ETag and Last-Modified headers to responses that might not
%% necessarily include them, specifically when the client requests
%% returnbody on a PUT or POST.
add_conditional_headers(RD, Ctx) ->
    {ETag, RD2, Ctx2} = generate_etag(RD, Ctx),
    {LM, RD3, Ctx3} = last_modified(RD2, Ctx2),
    RD4 =
        wrq:set_resp_header(
            "ETag", webmachine_util:quoted_string(ETag), RD3),
    RD5 =
        wrq:set_resp_header(
            "Last-Modified",
            httpd_util:rfc1123_date(
                calendar:universal_time_to_local_time(LM)), RD4),
    {RD5,Ctx3}.

-spec extract_user_meta(#wm_reqdata{}) -> proplists:proplist().
%% @doc Extract headers prefixed by X-Riak-Meta- in the client's PUT request
%%      to be returned by subsequent GET requests.
extract_user_meta(RD) ->
    lists:filter(fun({K,_V}) ->
                    lists:prefix(
                        ?HEAD_USERMETA_PREFIX,
                        string:to_lower(riak_kv_wm_utils:any_to_list(K)))
                end,
                mochiweb_headers:to_list(wrq:req_headers(RD))).

-spec multiple_choices(#wm_reqdata{}, context()) ->
          {boolean(), #wm_reqdata{}, context()}.
%% @doc Determine whether a document has siblings.  If the user has
%%      specified a specific vtag, the document is considered not to
%%      have sibling versions.  This is a safe assumption, because
%%      resource_exists will have filtered out requests earlier for
%%      vtags that are invalid for this version of the document.
multiple_choices(RD, Ctx=#ctx{vtag=undefined, doc={ok, Doc}}) ->
    %% user didn't specify a vtag, so there better not be siblings
    case riak_object:get_update_value(Doc) of
        undefined ->
            case riak_object:value_count(Doc) of
                1 -> {false, RD, Ctx};
                _ -> {true, RD, Ctx}
            end;
        _ ->
            %% just updated can't have multiple
            {false, RD, Ctx}
    end;
multiple_choices(RD, Ctx) ->
    %% specific vtag was specified
    %% if it's a tombstone add the X-Riak-Deleted header
    case select_doc(Ctx) of
        {M, _} ->
            case dict:find(?MD_DELETED, M) of
                {ok, "true"} ->
                    {false,
                        wrq:set_resp_header(?HEAD_DELETED, "true", RD),
                        Ctx};
                error ->
                    {false, RD, Ctx}
            end;
        multiple_choices ->
            throw(
                {unexpected_code_path,
                ?MODULE,
                multiple_choices,
                multiple_choices})
    end.

-spec produce_doc_body(#wm_reqdata{}, context()) ->
    {binary(), #wm_reqdata{}, context()}.
%% @doc Extract the value of the document, and place it in the
%%      response body of the request.  This function also adds the
%%      Link, X-Riak-Meta- headers, and X-Riak-Index- headers to the
%%      response.  One link will point to the bucket, with the
%%      property "rel=container".  The rest of the links will be
%%      constructed from the links of the document.
produce_doc_body(RD, Ctx) ->
    Prefix = Ctx#ctx.prefix,
    Bucket = Ctx#ctx.bucket,
    APIVersion = Ctx#ctx.api_version,
    case select_doc(Ctx) of
        {MD, Doc} ->
            %% Add links to response...
            Links1 = case dict:find(?MD_LINKS, MD) of
                        {ok, L} -> L;
                        error -> []
                    end,
            Links2 =
                riak_kv_wm_utils:format_links(
                    [{Bucket, "up"}|Links1], Prefix, APIVersion),
            LinkRD = wrq:merge_resp_headers(Links2, RD),

            %% Add user metadata to response...
            UserMetaRD = case dict:find(?MD_USERMETA, MD) of
                        {ok, UserMeta} ->
                            lists:foldl(
                                fun({K,V},Acc) ->
                                    wrq:merge_resp_headers([{K,V}],Acc)
                                end,
                                LinkRD, UserMeta);
                        error -> LinkRD
                    end,

            %% Add index metadata to response...
            IndexRD =
                case dict:find(?MD_INDEX, MD) of
                    {ok, IndexMeta} ->
                        lists:foldl(
                        fun({K,V}, Acc) ->
                            K1 = riak_kv_wm_utils:any_to_list(K),
                            V1 = riak_kv_wm_utils:any_to_list(V),
                            wrq:merge_resp_headers(
                                [{?HEAD_INDEX_PREFIX ++ K1, V1}], Acc)
                        end,
                        UserMetaRD, IndexMeta);
                    error ->
                        UserMetaRD
                end,
            {riak_kv_wm_utils:encode_value(Doc),
                encode_vclock_header(IndexRD, Ctx), Ctx};
        multiple_choices ->
            throw(
                {unexpected_code_path,
                ?MODULE,
                produce_doc_body,
                multiple_choices})
    end.

-spec produce_sibling_message_body(#wm_reqdata{}, context()) ->
    {iolist(), #wm_reqdata{}, context()}.
%% @doc Produce the text message informing the user that there are multiple
%%      values for this document, and giving that user the vtags of those
%%      values so they can get to them with the vtag query param.
produce_sibling_message_body(RD, Ctx=#ctx{doc={ok, Doc}}) ->
    Vtags = [ dict:fetch(?MD_VTAG, M)
              || M <- riak_object:get_metadatas(Doc) ],
    {[<<"Siblings:\n">>, [ [V,<<"\n">>] || V <- Vtags]],
     wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                         encode_vclock_header(RD, Ctx)),
     Ctx}.

-spec produce_multipart_body(#wm_reqdata{}, context()) ->
    {iolist(), #wm_reqdata{}, context()}.
%% @doc Produce a multipart body representation of an object with multiple
%%      values (siblings), each sibling being one part of the larger
%%      document.
produce_multipart_body(RD, Ctx=#ctx{doc={ok, Doc}, bucket=B, prefix=P}) ->
    APIVersion = Ctx#ctx.api_version,
    Boundary = riak_core_util:unique_id_62(),
    {[[["\r\n--",Boundary,"\r\n",
        riak_kv_wm_utils:multipart_encode_body(P, B, Content, APIVersion)]
       || Content <- riak_object:get_contents(Doc)],
      "\r\n--",Boundary,"--\r\n"],
     wrq:set_resp_header(?HEAD_CTYPE,
                         "multipart/mixed; boundary="++Boundary,
                         encode_vclock_header(RD, Ctx)),
     Ctx}.


-spec select_doc(context()) ->
    {Metadata :: term(), Value :: term()}|multiple_choices.
%% @doc Selects the "proper" document:
%%  - chooses update-value/metadata if update-value is set
%%  - chooses only val/md if only one exists
%%  - chooses val/md matching given Vtag if multiple contents exist
%%      (assumes a vtag has been specified)
select_doc(#ctx{doc={ok, Doc}, vtag=Vtag}) ->
    case riak_object:get_update_value(Doc) of
        undefined ->
            case riak_object:get_contents(Doc) of
                [Single] -> Single;
                Mult ->
                    case lists:dropwhile(
                           fun({M,_}) ->
                                dict:fetch(?MD_VTAG, M) /= Vtag
                           end,
                           Mult) of
                        [Match|_] -> Match;
                        [] -> multiple_choices
                    end
            end;
        UpdateValue ->
            {riak_object:get_update_metadata(Doc), UpdateValue}
    end.

-spec encode_vclock_header(#wm_reqdata{}, context()) -> #wm_reqdata{}.
%% @doc Add the X-Riak-Vclock header to the response.
encode_vclock_header(RD, #ctx{doc={ok, Doc}}) ->
    {Head, Val} = riak_object:vclock_header(Doc),
    wrq:set_resp_header(Head, Val, RD);
encode_vclock_header(RD, #ctx{doc={error, {deleted, VClock}}}) ->
    BinVClock = riak_object:encode_vclock(VClock),
    wrq:set_resp_header(
        ?HEAD_VCLOCK, binary_to_list(base64:encode(BinVClock)), RD).

-spec decode_vclock_header(#wm_reqdata{}) -> vclock:vclock().
%% @doc Translate the X-Riak-Vclock header value from the request into
%%      its Erlang representation.  If no vclock header exists, a fresh
%%      vclock is returned.
decode_vclock_header(RD) ->
    case wrq:get_req_header(?HEAD_VCLOCK, RD) of
        undefined -> vclock:fresh();
             Head -> riak_object:decode_vclock(base64:decode(Head))
    end.

-spec ensure_doc(context()) -> context().
%% @doc Ensure that the 'doc' field of the context() has been filled
%%      with the result of a riak_client:get request.  This is a
%%      convenience for memoizing the result of a get so it can be
%%      used in multiple places in this resource, without having to
%%      worry about the order of executing of those places.
ensure_doc(Ctx=#ctx{doc=undefined, key=undefined}) ->
    Ctx#ctx{doc={error, notfound}};
ensure_doc(Ctx=#ctx{doc=undefined, bucket_type=T, bucket=B, key=K, client=C,
                    basic_quorum=Quorum, notfound_ok=NotFoundOK}) ->
    case Ctx#ctx.type_exists of
        true ->
            Options0 =
                [deletedvclock,
                {basic_quorum, Quorum},
                {notfound_ok, NotFoundOK}],
            Options = make_options(Options0, Ctx),
            BT = riak_kv_wm_utils:maybe_bucket_type(T,B),
            Ctx#ctx{doc=riak_client:get(BT, K, Options, C)};
        false ->
            Ctx#ctx{doc={error, bucket_type_unknown}}
    end;
ensure_doc(Ctx) -> Ctx.

-spec delete_resource(#wm_reqdata{}, context()) ->
    {true, #wm_reqdata{}, context()}.
%% @doc Delete the document specified.
delete_resource(RD, Ctx=#ctx{bucket_type=T, bucket=B, key=K, client=C}) ->
    Options = make_options([], Ctx),
    BT = riak_kv_wm_utils:maybe_bucket_type(T,B),
    Result =
        case wrq:get_req_header(?HEAD_VCLOCK, RD) of
            undefined ->
                riak_client:delete(BT, K, Options, C);
            _ ->
                VC = decode_vclock_header(RD),
                riak_client:delete_vclock(BT, K, VC, Options, C)
        end,
    case Result of
        ok ->
            {true, RD, Ctx};
        {error, Reason} ->
            handle_common_error(Reason, RD, Ctx)
    end.

-ifndef(old_hash).
md5(Bin) ->
    crypto:hash(md5, Bin).
-else.
md5(Bin) ->
    crypto:md5(Bin).
-endif.

-spec generate_etag(#wm_reqdata{}, context()) ->
    {undefined|string(), #wm_reqdata{}, context()}.
%% @doc Get the etag for this resource.
%%      Documents will have an etag equal to their vtag. For documents with
%%      siblings when no vtag is specified, this will be an etag derived from
%%      the vector clock.
generate_etag(RD, Ctx) ->
    case select_doc(Ctx) of
        {MD, _} ->
            {dict:fetch(?MD_VTAG, MD), RD, Ctx};
        multiple_choices ->
            {ok, Doc} = Ctx#ctx.doc,
            <<ETag:128/integer>> =
                md5(term_to_binary(riak_object:vclock(Doc))),
            {riak_core_util:integer_to_list(ETag, 62), RD, Ctx}
    end.

-spec last_modified(#wm_reqdata{}, context()) ->
    {undefined|calendar:datetime(), #wm_reqdata{}, context()}.
%% @doc Get the last-modified time for this resource.
%%      Documents will have the last-modified time specified by the
%%      riak_object.
%%      For documents with siblings, this is the last-modified time of the
%%      latest sibling.
last_modified(RD, Ctx) ->
    case select_doc(Ctx) of
        {MD, _} ->
            {normalize_last_modified(MD),RD, Ctx};
        multiple_choices ->
            {ok, Doc} = Ctx#ctx.doc,
            LMDates = [ normalize_last_modified(MD) ||
                          MD <- riak_object:get_metadatas(Doc) ],
            {lists:max(LMDates), RD, Ctx}
    end.

-spec normalize_last_modified(riak_kv_wm_object_dict()) -> calendar:datetime().
%% @doc Extract and convert the Last-Modified metadata into a normalized form
%%      for use in the last_modified/2 callback.
normalize_last_modified(MD) ->
    case dict:fetch(?MD_LASTMOD, MD) of
        Now={_,_,_} ->
            calendar:now_to_universal_time(Now);
        Rfc1123 when is_list(Rfc1123) ->
            httpd_util:convert_request_date(Rfc1123)
    end.

-spec get_link_heads(#wm_reqdata{}, context()) -> [link()].
%% @doc Extract the list of links from the Link request header.
%%      This function will die if an invalid link header format
%%      is found.
get_link_heads(RD, Ctx) ->
    APIVersion = Ctx#ctx.api_version,
    Prefix = Ctx#ctx.prefix,
    Bucket = Ctx#ctx.bucket,

    %% Get a list of link headers...
    LinkHeaders1 =
        case wrq:get_req_header(?HEAD_LINK, RD) of
            undefined -> [];
            Heads -> string:tokens(Heads, ",")
        end,

    %% Decode the link headers. Throw an exception if we can't
    %% properly parse any of the headers...
    {BucketLinks, KeyLinks} =
        case APIVersion of
            1 ->
                {ok, BucketRegex} =
                    re:compile("</" ++ Prefix ++ ?V1_BUCKET_REGEX),
                {ok, KeyRegex} =
                    re:compile("</" ++ Prefix ++ ?V1_KEY_REGEX),
                extract_links(LinkHeaders1, BucketRegex, KeyRegex);
            %% @todo Handle links in API Version 3?
            Two when Two >= 2 ->
                {ok, BucketRegex} =
                    re:compile(?V2_BUCKET_REGEX),
                {ok, KeyRegex} =
                    re:compile(?V2_KEY_REGEX),
                extract_links(LinkHeaders1, BucketRegex, KeyRegex)
        end,

    %% Validate that the only bucket header is pointing to the parent
    %% bucket...
    IsValid = (BucketLinks == []) orelse (BucketLinks == [{Bucket, <<"up">>}]),
    case IsValid of
        true ->
            KeyLinks;
        false ->
            throw({invalid_link_headers, LinkHeaders1})
    end.

%% Run each LinkHeader string() through the BucketRegex and
%% KeyRegex. Return {BucketLinks, KeyLinks}.
extract_links(LinkHeaders, BucketRegex, KeyRegex) ->
    %% Run each regex against each string...
    extract_links_1(LinkHeaders, BucketRegex, KeyRegex, [], []).
extract_links_1([LinkHeader|Rest], BucketRegex, KeyRegex, BucketAcc, KeyAcc) ->
    case re:run(LinkHeader, BucketRegex, [{capture, all_but_first, list}]) of
        {match, [Bucket, Tag]} ->
            Bucket1 = list_to_binary(mochiweb_util:unquote(Bucket)),
            Tag1 = list_to_binary(mochiweb_util:unquote(Tag)),
            NewBucketAcc = [{Bucket1, Tag1}|BucketAcc],
            extract_links_1(Rest, BucketRegex, KeyRegex, NewBucketAcc, KeyAcc);
        nomatch ->
            case re:run(LinkHeader, KeyRegex, [{capture, all_but_first, list}]) of
                {match, [Bucket, Key, Tag]} ->
                    Bucket1 = list_to_binary(mochiweb_util:unquote(Bucket)),
                    Key1 = list_to_binary(mochiweb_util:unquote(Key)),
                    Tag1 = list_to_binary(mochiweb_util:unquote(Tag)),
                    NewKeyAcc = [{{Bucket1, Key1}, Tag1}|KeyAcc],
                    extract_links_1(Rest, BucketRegex, KeyRegex, BucketAcc, NewKeyAcc);
                nomatch ->
                    throw({invalid_link_header, LinkHeader})
            end
    end;
extract_links_1([], _BucketRegex, _KeyRegex, BucketAcc, KeyAcc) ->
    {BucketAcc, KeyAcc}.

-spec get_ctype(riak_kv_wm_object_dict(), term()) -> string().
%% @doc Work out the content type for this object - use the metadata if provided
get_ctype(MD,V) ->
    case dict:find(?MD_CTYPE, MD) of
        {ok, Ctype} ->
            Ctype;
        error when is_binary(V) ->
            "application/octet-stream";
        error ->
            "application/x-erlang-binary"
    end.

missing_content_type(RD) ->
    RD1 = wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD),
    wrq:append_to_response_body(<<"Missing Content-Type request header">>, RD1).

send_precommit_error(RD, Reason) ->
    RD1 = wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD),
    Error = if
                Reason =:= undefined ->
                    list_to_binary([atom_to_binary(wrq:method(RD1), utf8),
                                    <<" aborted by pre-commit hook.">>]);
                true ->
                    Reason
            end,
    wrq:append_to_response_body(Error, RD1).

handle_common_error(Reason, RD, Ctx) ->
    case {error, Reason} of
        {error, precommit_fail} ->
            {{halt, 403}, send_precommit_error(RD, undefined), Ctx};
        {error, {precommit_fail, Message}} ->
            {{halt, 403}, send_precommit_error(RD, Message), Ctx};
        {error, too_many_fails} ->
            {{halt, 503}, wrq:append_to_response_body("Too Many write failures"
                    " to satisfy W/DW\n", RD), Ctx};
        {error, timeout} ->
            {{halt, 503},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("request timed out~n",[]),
                        RD)),
                Ctx};
        {error, notfound} ->
            {{halt, 404},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("not found~n",[]),
                        RD)),
                Ctx};
        {error, bucket_type_unknown} ->
            {{halt, 404},
             wrq:set_resp_body(
               io_lib:format("Unknown bucket type: ~s", [Ctx#ctx.bucket_type]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx};
        {error, {deleted, _VClock}} ->
            {{halt, 404},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:set_resp_header(?HEAD_DELETED, "true",
                        wrq:append_to_response_body(
                            io_lib:format("not found~n",[]),
                            encode_vclock_header(RD, Ctx)))),
                Ctx};
        {error, {n_val_violation, N}} ->
            Msg = io_lib:format("Specified w/dw/pw/node_confirms values invalid for bucket"
                " n value of ~p~n", [N]),
            {{halt, 400}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, {r_val_unsatisfied, Requested, Returned}} ->
            {{halt, 503},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("R-value unsatisfied: ~p/~p~n",
                            [Returned, Requested]),
                        RD)),
                Ctx};
        {error, {dw_val_unsatisfied, DW, NumDW}} ->
            {{halt, 503},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("DW-value unsatisfied: ~p/~p~n",
                            [NumDW, DW]),
                        RD)),
                Ctx};
        {error, {pr_val_unsatisfied, Requested, Returned}} ->
            {{halt, 503},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("PR-value unsatisfied: ~p/~p~n",
                            [Returned, Requested]),
                        RD)),
                Ctx};
        {error, {pw_val_unsatisfied, Requested, Returned}} ->
            Msg = io_lib:format("PW-value unsatisfied: ~p/~p~n", [Returned,
                    Requested]),
            {{halt, 503}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, {node_confirms_val_unsatisfied, Requested, Returned}} ->
            Msg = io_lib:format("node_confirms-value unsatisfied: ~p/~p~n", [Returned,
                    Requested]),
            {{halt, 503}, wrq:append_to_response_body(Msg, RD), Ctx};
        {error, failed} ->
            {{halt, 412}, RD, Ctx};
        {error, Err} ->
            {{halt, 500},
                wrq:set_resp_header(?HEAD_CTYPE, "text/plain",
                    wrq:append_to_response_body(
                        io_lib:format("Error:~n~p~n",[Err]),
                        RD)),
                Ctx}
    end.

make_options(Prev, Ctx) ->
    NewOpts0 = [{rw, Ctx#ctx.rw}, {r, Ctx#ctx.r}, {w, Ctx#ctx.w},
                {pr, Ctx#ctx.pr}, {pw, Ctx#ctx.pw}, {dw, Ctx#ctx.dw},
                {node_confirms, Ctx#ctx.node_confirms},
                {sync_on_write, Ctx#ctx.sync_on_write},
                {timeout, Ctx#ctx.timeout},
                {asis, Ctx#ctx.asis}],
    NewOpts = [ {Opt, Val} || {Opt, Val} <- NewOpts0,
                              Val /= undefined, Val /= default ],
    Prev ++ NewOpts.

ensure_bucket_type(RD, Ctx) ->
    Ctx0 = riak_kv_wm_utils:ensure_bucket_type(RD, Ctx, #ctx.bucket_type),
    Ctx0#ctx{type_exists =
               riak_kv_wm_utils:bucket_type_exists(Ctx0#ctx.bucket_type)}.
