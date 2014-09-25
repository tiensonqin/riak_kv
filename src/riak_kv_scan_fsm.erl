%% -------------------------------------------------------------------
%%
%% riak_scan_fsm: coordination of Riak SCAN requests
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

-module(riak_kv_scan_fsm).
-behaviour(gen_fsm).
-include_lib("riak_kv_vnode.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_link/7, test_link/5]).
-endif.
-export([start_link/7]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2,validate/2,execute/2,waiting_vnode_r/2]).

-type option() :: {r, pos_integer()} |         %% Minimum number of successful responses
                  {pr, non_neg_integer()} |    %% Minimum number of primary vnodes participating
                  {basic_quorum, boolean()} |  %% Whether to use basic quorum (return early
                  %% in some failure cases.
                  {notfound_ok, boolean()}  |  %% Count notfound reponses as successful.
                  {timeout, pos_integer() | infinity} | %% Timeout for vnode responses
                  {n_val, pos_integer()}.     %% default = bucket props

-type options() :: [option()].
-type req_id() :: non_neg_integer().

-export_type([options/0, option/0]).

-record(state, {from :: {raw, req_id(), pid()},
                options=[] :: options(),
                n :: pos_integer(),
                preflist2 :: riak_core_apl:preflist2(),
                req_id :: non_neg_integer(),
                timeout :: infinity | pos_integer(),
                scan_core :: riak_kv_scan_core:scancore(),
                tref    :: reference(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                offset :: non_neg_integer(),
                order :: atom(),
                len :: non_neg_integer(),
                bucket_props}).

-define(DEFAULT_TIMEOUT, 60000).
-define(DEFAULT_R, default).
-define(DEFAULT_PR, 0).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start the scan FSM - retrieve Bucket/Key with the options provided
%%
%% {r, pos_integer()}        - Minimum number of successful responses
%% {pr, non_neg_integer()}   - Minimum number of primary vnodes participating
%% {basic_quorum, boolean()} - Whether to use basic quorum (return early
%%                             in some failure cases.
%% {notfound_ok, boolean()}  - Count notfound reponses as successful.
%% {timeout, pos_integer() | infinity} -  Timeout for vnode responses
%% -spec start_link({raw, req_id(), pid()}, binary(), binary(), options()) ->
%%                         {ok, pid()} | {error, any()}.
start_link(From, Bucket, Key, Offset, Len, Order, Options) ->
    Args = [From, Bucket, Key, Offset, Len, Order, Options],
    gen_fsm:start_link(?MODULE, Args, []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, Bucket, Key, Offset, Len, Order, Options0]) ->
    Options = proplists:unfold(Options0),
    StateData = #state{from = From,
                       options = Options,
                       bkey = {Bucket, Key},
                       offset = Offset,
                       len = Len,
                       order = Order},
    {ok, prepare, StateData, 0}.

%% @private
prepare(timeout, StateData=#state{bkey=BKey={Bucket,_Key},
                                  options=Options}) ->
    {ok, DefaultProps} = application:get_env(riak_core,
                                             default_bucket_props),
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    %% typed buckets never fall back to defaults
    Props =
        case is_tuple(Bucket) of
            false ->
                lists:keymerge(1, lists:keysort(1, BucketProps),
                               lists:keysort(1, DefaultProps));
            true ->
                BucketProps
        end,
    DocIdx = riak_core_util:chash_key(BKey, BucketProps),
    Bucket_N = get_option(n_val, BucketProps),

    N = case get_option(n_val, Options) of
            undefined ->
                Bucket_N;
            N_val when is_integer(N_val), N_val > 0, N_val =< Bucket_N ->
                %% don't allow custom N to exceed bucket N
                N_val;
            Bad_N ->
                {error, {n_val_violation, Bad_N}}
        end,
    case N of
        {error, _} = Error ->
            StateData2 = client_reply(Error, StateData),
            {stop, normal, StateData2};
        _ ->

            UpNodes = riak_core_node_watcher:nodes(riak_kv),
            Preflist2 =  riak_core_apl:get_apl_ann(DocIdx, N, UpNodes),

            new_state_timeout(validate, StateData#state{n = N,
                                                        bucket_props=Props,
                                                        preflist2 = Preflist2})
    end.

%% @private
validate(timeout, StateData=#state{from = {raw, ReqId, _Pid}, options = Options,
                                   n = N, bucket_props = BucketProps, preflist2 = _PL2}) ->
    AppEnvTimeout = app_helper:get_env(riak_kv, timeout),
    Timeout = case AppEnvTimeout of
                  undefined -> get_option(timeout, Options, ?DEFAULT_TIMEOUT);
                  _ -> AppEnvTimeout
              end,

    R0 = get_option(r, Options, ?DEFAULT_R),
    R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
    ScanCore = riak_kv_scan_core:init(N, R, 2),
    new_state_timeout(execute, StateData#state{scan_core = ScanCore,
                                               timeout = Timeout,
                                               req_id = ReqId}).

%% %% @private
%% validate(timeout, StateData=#state{from = {raw, ReqId, _Pid}, options = Options,
%%                                    n = N, bucket_props = BucketProps, preflist2 = PL2,
%%                                    trace=Trace}) ->
%%     ?DTRACE(Trace, ?C_GET_FSM_VALIDATE, [], ["validate"]),
%%     AppEnvTimeout = app_helper:get_env(riak_kv, timeout),
%%     Timeout = case AppEnvTimeout of
%%                   undefined -> get_option(timeout, Options, ?DEFAULT_TIMEOUT);
%%                   _ -> AppEnvTimeout
%%               end,
%%     R0 = get_option(r, Options, ?DEFAULT_R),
%%     PR0 = get_option(pr, Options, ?DEFAULT_PR),
%%     R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
%%     PR = riak_kv_util:expand_rw_value(pr, PR0, BucketProps, N),
%%     NumVnodes = length(PL2),
%%     NumPrimaries = length([x || {_,primary} <- PL2]),
%%     IdxType = [{Part, Type} || {{Part, _Node}, Type} <- PL2],

%%     case validate_quorum(R, R0, N, PR, PR0, NumPrimaries, NumVnodes) of
%%         ok ->
%%             BQ0 = get_option(basic_quorum, Options, default),
%%             FailR = erlang:max(R, PR), %% fail fast
%%             FailThreshold =
%%                 case riak_kv_util:expand_value(basic_quorum, BQ0, BucketProps) of
%%                     true ->
%%                         erlang:min((N div 2)+1, % basic quorum, or
%%                                    (N-FailR+1)); % cannot ever get R 'ok' replies
%%                     _ElseFalse ->
%%                         N - FailR + 1 % cannot ever get R 'ok' replies
%%                 end,
%%             AllowMult = get_option(allow_mult, BucketProps),
%%             NFOk0 = get_option(notfound_ok, Options, default),
%%             NotFoundOk = riak_kv_util:expand_value(notfound_ok, NFOk0, BucketProps),
%%             DeletedVClock = get_option(deletedvclock, Options, false),
%%             GetCore = riak_kv_get_core:init(N, R, PR, FailThreshold,
%%                                             NotFoundOk, AllowMult,
%%                                             DeletedVClock, IdxType),
%%             new_state_timeout(execute, StateData#state{get_core = GetCore,
%%                                                        timeout = Timeout,
%%                                                        req_id = ReqId});
%%         Error ->
%%             StateData2 = client_reply(Error, StateData),
%%             {stop, normal, StateData2}
%%     end.

%% %% @private validate the quorum values
%% %% {error, Message} or ok
%% validate_quorum(R, ROpt, _N, _PR, _PROpt, _NumPrimaries, _NumVnodes) when R =:= error ->
%%     {error, {r_val_violation, ROpt}};
%% validate_quorum(R, _ROpt, N, _PR, _PROpt, _NumPrimaries, _NumVnodes) when R > N ->
%%     {error, {n_val_violation, N}};
%% validate_quorum(_R, _ROpt, _N, PR, PROpt, _NumPrimaries, _NumVnodes) when PR =:= error ->
%%     {error, {pr_val_violation, PROpt}};
%% validate_quorum(_R, _ROpt,  N, PR, _PROpt, _NumPrimaries, _NumVnodes) when PR > N ->
%%     {error, {n_val_violation, N}};
%% validate_quorum(_R, _ROpt, _N, PR, _PROpt, NumPrimaries, _NumVnodes) when PR > NumPrimaries ->
%%     {error, {pr_val_unsatisfied, PR, NumPrimaries}};
%% validate_quorum(R, _ROpt, _N, _PR, _PROpt, _NumPrimaries, NumVnodes) when R > NumVnodes ->
%%     {error, {insufficient_vnodes, NumVnodes, need, R}};
%% validate_quorum(_R, _ROpt, _N, _PR, _PROpt, _NumPrimaries, _NumVnodes) ->
%%     ok.

%% @private
execute(timeout, StateData0=#state{req_id=ReqId,timeout=Timeout,
                                   bkey=BKey,offset=Offset,len=Len,
                                   order=Order,
                                   preflist2 = Preflist2}) ->
    TRef = schedule_timeout(Timeout),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
    riak_kv_vnode:scan(Preflist, BKey, Offset, Len, Order, ReqId),
    StateData = StateData0#state{tref=TRef},
    new_state(waiting_vnode_r, StateData).

%% @private
waiting_vnode_r({r, VnodeResult, Idx, _ReqId}, StateData = #state{scan_core = ScanCore}) ->
    UpdScanCore = riak_kv_scan_core:add_result(Idx, VnodeResult, ScanCore),
    case riak_kv_scan_core:enough(UpdScanCore) of
        true ->
            {Reply, UpdScanCore2} = riak_kv_scan_core:response(UpdScanCore),
            client_reply(Reply, StateData#state{scan_core = UpdScanCore2});
        false ->
            %% don't use new_state/2 since we do timing per state, not per message in state
            {next_state, waiting_vnode_r,  StateData#state{scan_core = UpdScanCore}}
    end;
waiting_vnode_r(request_timeout, StateData) ->
    client_reply({error,timeout}, StateData).

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

new_state(StateName, StateData) ->
    {next_state, StateName, StateData}.

%% Move to the new state, marking the time it started and trigger an immediate
%% timeout.
new_state_timeout(StateName, StateData) ->
    {next_state, StateName, StateData, 0}.

get_option(Name, Options) ->
    get_option(Name, Options, undefined).

get_option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Val} ->
            Val;
        false ->
            Default
    end.

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, StateData = #state{from = {raw, ReqId, Pid}}) ->
    Msg = {ReqId, Reply},
    Pid ! Msg,
    StateData.
