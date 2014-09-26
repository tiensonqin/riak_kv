-module(riak_kv_pb_scan).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client,    % local client
                req,       % current request (for multi-message requests like list keys)
                req_ctx,   % context to go along with request (partial results, request ids etc)
                client_id = <<0,0,0,0>> }). % emulate legacy API when vnode_vclocks is true

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #rpbscanreq{} ->
            {ok, Msg, {"riak_kv.scan", bucket_type(Msg#rpbscanreq.type,
                                                   Msg#rpbscanreq.bucket)}};
        _ ->
            {ok, Msg}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

process(#rpbscanreq{bucket = <<>>}, State) ->
    {error, "Bucket cannot be zero-length", State};
process(#rpbscanreq{key = <<>>}, State) ->
    {error, "Key cannot be zero-length", State};
process(#rpbscanreq{type = <<>>}, State) ->
    {error, "Type cannot be zero-length", State};
process(#rpbscanreq{bucket=B0, type=T, key=K,
                    offset=Offset, len=Len, order=Order,
                    timeout=Timeout}, #state{client=C} = State) ->
    B = maybe_bucket_type(T, B0),
    case C:scan(B, K, Offset, Len, Order, [{timeout, Timeout}]) of
        {ok, Content} -> {reply, #rpbscanresp{content=Content}, State};
        {error, notfound} -> {reply, #rpbscanresp{}, State};
        {error, Reason} ->
            {error, {format,Reason}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

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
