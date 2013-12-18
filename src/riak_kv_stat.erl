%% -------------------------------------------------------------------
%%
%% riak_stat: collect, aggregate, and provide stats about the local node
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

%% @doc riak_kv_stat is a module for aggregating
%%      stats about the Riak node on which it is runing.
%%
%%      Update each stat with the exported function update/1. Add
%%      a new stat to the internal stats/0 func to register a new stat with
%%      folsom.
%%
%%      Get the latest aggregation of stats with the exported function
%%      get_stats/0. Or use folsom_metrics:get_metric_value/1,
%%      or riak_core_stat_q:get_stats/1.
%%

-module(riak_kv_stat).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0, get_stats/0,
         update/1, perform_update/1, register_stats/0, produce_stats/0,
         leveldb_read_block_errors/1, stop/0]).
-export([track_bucket/1, untrack_bucket/1]).
-export([active_gets/0, active_puts/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, monitor_loop/1]).

-record(state, {repair_mon, monitors}).

-define(SERVER, ?MODULE).
-define(APP, riak_kv).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    riak_core_stat:register_stats(?APP, stats()).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    lists:append(
      [riak_core_stat:get_stats(?APP),
       riak_kv_stat_bc:other_stats(),
       riak_core_stat:get_stats(common)]).


%% Creation of a dynamic stat _must_ be serialized.
register_stat(Name, Type) ->
    do_register_stat(Name, Type).
%% gen_server:call(?SERVER, {register, Name, Type}).

update(Arg) ->
    case erlang:module_loaded(riak_kv_stat_sj) of
        true ->
            %% Dispatch request to sidejob worker
            riak_kv_stat_worker:update(Arg);
        false ->
            perform_update(Arg)
    end.

%% @doc
%% Callback used by a {@link riak_kv_stat_worker} to perform actual update
perform_update(Arg) ->
    try do_update(Arg) of
        ok -> ok;
        {error, not_found} ->
            lager:warning("{error,not_found} updating stat ~p.", [Arg]),
            gen_server:cast(?SERVER, {re_register_stat, Arg})
    catch
        ErrClass:Err ->
            lager:warning("~p:~p updating stat ~p.", [ErrClass, Err, Arg]),
            gen_server:cast(?SERVER, {re_register_stat, Arg})
    end.

track_bucket(Bucket) when is_binary(Bucket) ->
    riak_core_bucket:set_bucket(Bucket, [{stat_tracked, true}]).

untrack_bucket(Bucket) when is_binary(Bucket) ->
    riak_core_bucket:set_bucket(Bucket, [{stat_tracked, false}]).

%% The current number of active get fsms in riak
active_gets() ->
    exometer:get_value([riak_core_stat:prefix(),
                        ?APP, node, gets, fsm, active]).

%% The current number of active put fsms in riak
active_puts() ->
    exometer:get_value([riak_core_stat:prefix(),
                        ?APP, node, puts, fsm, active]).

stop() ->
    gen_server:cast(?SERVER, stop).

%% gen_server

init([]) ->
    register_stats(),
    Me = self(),
    State = #state{monitors = [{index, spawn_link(?MODULE, monitor_loop, [index])},
                               {list, spawn_link(?MODULE, monitor_loop, [list])}],
                   repair_mon = spawn_monitor(fun() -> stat_repair_loop(Me) end)},
    {ok, State}.

handle_call({register, Name, Type}, _From, State) ->
    Rep = do_register_stat(Name, Type),
    {reply, Rep, State}.

handle_cast({re_register_stat, Arg}, State) ->
    %% To avoid massive message queues
    %% riak_kv stats are updated in the calling process
    %% @see `update/1'.
    %% The downside is that errors updating a stat don't crash
    %% the server, so broken stats stay broken.
    %% This re-creates the same behaviour as when a brokwn stat
    %% crashes the gen_server by re-registering that stat.
    #state{repair_mon={Pid, _Mon}} = State,
    Pid ! {re_register_stat, Arg},
    {noreply, State};
handle_cast({monitor, Type, Pid}, State) ->
    case proplists:get_value(Type, State#state.monitors) of
        Monitor when is_pid(Monitor) ->
            Monitor ! {add_pid, Pid};
        _ -> lager:error("Couldn't find process for ~p to add monitor", [Type])
    end,
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Cause}, State=#state{repair_mon={Pid, MonRef}}) ->
    Me = self(),
    RepairMonitor = spawn_monitor(fun() -> stat_repair_loop(Me) end),
    {noreply, State#state{repair_mon=RepairMonitor}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Update the given stat
do_update({vnode_get, Idx, USecs}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, vnode, gets], 1),
    create_or_update([P, ?APP, vnode, gets, time], USecs, histogram),
    do_per_index(gets, Idx, USecs);
do_update({vnode_put, Idx, USecs}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, vnode, puts], 1),
    create_or_update([P, ?APP, vnode, puts, time], USecs, histogram),
    do_per_index(puts, Idx, USecs);
do_update(vnode_index_read) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, vnode, index, reads], 1);
do_update({vnode_index_write, PostingsAdded, PostingsRemoved}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, vnode, index, writes], 1),
    exometer:update([P, ?APP, vnode, index, writes, postings],
		    PostingsAdded),
    exometer:update([P, ?APP, vnode, index, deletes, postings],
		    PostingsRemoved);
do_update({vnode_index_delete, Postings}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, vnode, index, deletes], Postings),
    exometer:update([P, ?APP, vnode, index, deletes, postings], Postings);
do_update({get_fsm, Bucket, Microsecs, Stages, undefined, undefined, PerBucket}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, gets], 1),
    exometer:update([P, ?APP, node, gets, time], Microsecs),
    do_stages([P, ?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, undefined, undefined});
do_update({get_fsm, Bucket, Microsecs, Stages, NumSiblings, ObjSize, PerBucket}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, gets], 1),
    exometer:update([P, ?APP, node, gets, time], Microsecs),
    exometer:update([P, ?APP, node, gets, siblings], NumSiblings),
    exometer:update([P, ?APP, node, gets, objsize], ObjSize),
    do_stages([P, ?APP, node, gets, time], Stages),
    do_get_bucket(PerBucket, {Bucket, Microsecs, Stages, NumSiblings, ObjSize});
do_update({put_fsm_time, Bucket,  Microsecs, Stages, PerBucket}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, puts], 1),
    exometer:update([P, ?APP, node, puts, time], Microsecs),
    do_stages([P, ?APP, node, puts, time], Stages),
    do_put_bucket(PerBucket, {Bucket, Microsecs, Stages});
do_update({read_repairs, Indices, Preflist}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, gets, read_repairs], 1),
    do_repairs(Indices, Preflist);
do_update(coord_redir) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, puts, coord_redirs], 1);
do_update(mapper_start) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, mapper_count], 1);
do_update(mapper_end) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, mapper_count], -1);
do_update(precommit_fail) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, precommit_fail], 1);
do_update(postcommit_fail) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, postcommit_fail], 1);
do_update({fsm_spawned, Type}) when Type =:= gets; Type =:= puts ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, Type, fsm, active], 1);
do_update({fsm_exit, Type}) when Type =:= gets; Type =:= puts  ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, node, Type, fsm, active], -1);
do_update({fsm_error, Type}) when Type =:= gets; Type =:= puts ->
    P = riak_core_stat:prefix(),
    do_update({fsm_exit, Type}),
    exometer:update([P, ?APP, node, Type, fsm, errors], 1);
do_update({index_create, Pid}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, index, fsm, create], 1),
    exometer:update([P, ?APP, index, fsm, active], 1),
    add_monitor(index, Pid),
    ok;
do_update(index_create_error) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, index, fsm, create, error], 1);
do_update({list_create, Pid}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, list, fsm, create], 1),
    exometer:update([P, ?APP, list, fsm, active], 1),
    add_monitor(list, Pid),
    ok;
do_update(list_create_error) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, list, fsm, create, error], 1);
do_update({fsm_destroy, Type}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, Type, fsm, active], -1);
do_update({Type, actor_count, Count}) ->
    P = riak_core_stat:prefix(),
    exometer:update([P, ?APP, Type, actor_count], Count);
do_update(late_put_fsm_coordinator_ack) ->
    ok = maybe_notify_existing([?APP, late_put_fsm_coordinator_ack], 1);
do_update({consistent_get, _Bucket, Microsecs, ObjSize}) ->
    ok = maybe_notify_existing([?APP, consistent, gets], 1),
    ok = maybe_notify_existing([?APP, consistent, gets, time], Microsecs),
    ok = maybe_notify_existing([?APP, consistent, gets, objsize], ObjSize);
do_update({consistent_put, _Bucket, Microsecs, ObjSize}) ->
    ok = maybe_notify_existing([?APP, consistent, puts], 1),
    ok = maybe_notify_existing([?APP, consistent, puts, time], Microsecs),
    ok = maybe_notify_existing([?APP, consistent, puts, objsize], ObjSize).

%% private

notify_existing(Name, Event) ->
    exometer:update([riak_core_stat:prefix() | Name], Event).

maybe_notify_existing(_, undefined) ->
    ok;
maybe_notify_existing(Name, Event) ->
    notify_existing(Name, Event).

add_monitor(Type, Pid) ->
    gen_server:cast(?SERVER, {monitor, Type, Pid}).

monitor_loop(Type) ->
    receive
        {add_pid, Pid} ->
            erlang:monitor(process, Pid);
        {'DOWN', _Ref, process, _Pid, _Reason} ->
            do_update({fsm_destroy, Type})
    end,
    monitor_loop(Type).

%% Per index stats (by op)
do_per_index(Op, Idx, USecs) ->
    IdxAtom = list_to_atom(integer_to_list(Idx)),
    P = riak_core_stat:prefix(),
    create_or_update([P, ?APP, vnode, Op, IdxAtom], 1, spiral),
    create_or_update([P, ?APP, vnode, Op, time, IdxAtom], USecs, histogram).

%%  per bucket get_fsm stats
do_get_bucket(false, _) ->
    ok;
do_get_bucket(true, {Bucket, Microsecs, Stages, NumSiblings, ObjSize}=Args) ->
    P = riak_core_stat:prefix(),
    case exometer:update([P, ?APP, node, gets, Bucket], 1) of
        ok ->
            [exometer:update([P, ?APP, node, gets, Dimension, Bucket], Arg)
             || {Dimension, Arg} <- [{time, Microsecs},
                                     {siblings, NumSiblings},
                                     {objsize, ObjSize}], Arg /= undefined],
            do_stages([P, ?APP, node, gets, time, Bucket], Stages);
        {error, not_found} ->
            exometer:new([P, ?APP, node, gets, Bucket], spiral),
            [register_stat([P, ?APP, node, gets, Dimension, Bucket], histogram) || Dimension <- [time,
												 siblings,
												 objsize]],
            do_get_bucket(true, Args)
    end.

%% per bucket put_fsm stats
do_put_bucket(false, _) ->
    ok;
do_put_bucket(true, {Bucket, Microsecs, Stages}=Args) ->
    P = riak_core_stat:prefix(),
    case exometer:update([P, ?APP, node, puts, Bucket], 1) of
        ok ->
            exometer:update([P, ?APP, node, puts, time, Bucket], Microsecs),
            do_stages([P, ?APP, node, puts, time, Bucket], Stages);
        {error, _} ->
            register_stat([P, ?APP, node, puts, Bucket], spiral),
            register_stat([P, ?APP, node, puts, time, Bucket], histogram),
            do_put_bucket(true, Args)
    end.

%% Path is list that provides a conceptual path to a stat
%% folsom uses the tuple as flat name
%% but some ets query magic means we can get stats by APP, Stat, DimensionX
%% Path, then is a list like [?APP, StatName]
%% Both get and put fsm have a list of {state, microseconds}
%% that they provide for stats.
%% Use the state to append to the stat "path" to create a further dimension on the stat
do_stages(_Path, []) ->
    ok;
do_stages(Path, [{Stage, Time}|Stages]) ->
    create_or_update(Path ++ [Stage], Time, histogram),
    do_stages(Path, Stages).

%% create dimensioned stats for read repairs.
%% The indexes are from get core [{Index, Reason::notfound|outofdate}]
%% preflist is a preflist of [{{Index, Node}, Type::primary|fallback}]
do_repairs(Indices, Preflist) ->
    lists:foreach(fun({{Idx, Node}, Type}) ->
                          case proplists:get_value(Idx, Indices) of
                              undefined ->
                                  ok;
                              Reason ->
                                  create_or_update([?APP, node, gets,  read_repairs, Node, Type, Reason], 1, spiral)
                          end
                  end,
                  Preflist).

%% for dynamically created / dimensioned stats
%% that can't be registered at start up
create_or_update(Name, UpdateVal, Type) ->
    case exometer:update(Name, UpdateVal) of
        ok ->
            ok;
        {error, not_found} ->
            register_stat(Name, Type),
            exometer:update(Name, UpdateVal)
    end.

%% @doc list of {Name, Type} for static
%% stats that we can register at start up
stats() ->
    [{[vnode, gets], spiral},
     {[vnode, gets, time], histogram},
     {[vnode, puts], spiral},
     {[vnode, puts, time], histogram},
     {[vnode, index, reads], spiral},
     {[vnode, index ,writes], spiral},
     {[vnode, index, writes, postings], spiral},
     {[vnode, index, deletes], spiral},
     {[vnode, index, deletes, postings], spiral},
     {[node, gets], spiral},
     {[node, gets, siblings], histogram},
     {[node, gets, objsize], histogram},
     {[node, gets, time], histogram},
     {[node, puts], spiral},
     {[node, puts, time], histogram},
     {[node, gets, read_repairs], spiral},
     {[node, puts, coord_redirs], counter},
     {[node, puts, fsm, active], counter},
     {[node, gets, fsm, active], counter},
     {[node, puts, fsm, errors], spiral},
     {[node, gets, fsm, errors], spiral},
     {[index, fsm, create], spiral},
     {[index, fsm, create, error], spiral},
     {[index, fsm, active], counter},
     {[list, fsm, create], spiral},
     {[list, fsm, create, error], spiral},
     {[list, fsm, active], counter},
     {mapper_count, counter},
     {precommit_fail, counter},
     {postcommit_fail, counter},
     {[vnode, backend, leveldb, read_block_error],
      {function, ?MODULE, leveldb_read_block_errors}},
     {[counter, actor_count], histogram},
     {[set, actor_count], histogram},
     {[map, actor_count], histogram},
     {[object, merge], spiral},
     {[object, merge, time], histogram},
     {[object, counter, merge], spiral},
     {[object, counter, merge, time], histogram},
     {[object, set, merge], spiral},
     {[object, set, merge, time], histogram},
     {[object, map, merge], spiral},
     {[object, map, merge, time], histogram},
     {late_put_fsm_coordinator_ack, counter},
     {[consistent, gets], spiral},
     {[consistent, gets, time], histogram},
     {[consistent, gets, objsize], histogram},
     {[consistent, puts], spiral},
     {[consistent, puts, time], histogram},
     {[consistent, puts, objsize], histogram}
    ].

do_register_stat(Name, histogram) ->
    %% get the global default histo type
    Opts = get_histogram_opts(Name),
    exometer:new(Name, histogram, Opts);
do_register_stat(Name, Type) ->
    %% store the function in a gauge metric
    exometer:new(Name, Type).

%% @doc the histogram sample type may be set in app.config
%% use key `stat_sample_type' in the `riak_kv' section. Or the
%% name of an `histogram' stat.
%% Check the folsom homepage for available types.
%% Defaults to `{slide_uniform, {60, 1028}}' (a uniform sliding window
%% of 60 seconds, with a uniform sample of at most 1028 entries)
get_histogram_opts(Name) ->
    SampleType0 = app_helper:get_env(riak_kv, stat_sample_type, {slide_uniform, {60, 1028}}),
    case app_helper:get_env(riak_kv, Name, SampleType0) of
        {Type, {SpanSeconds, MaxEntries}} ->
            [{type, Type},
             {time_span, SpanSeconds * 1000},
             {max_elements, MaxEntries}]
    end.

%% @doc produce the legacy blob of stats for display.
produce_stats() ->
    riak_kv_stat_bc:produce_stats().

%% @doc get the leveldb.ReadBlockErrors counter.
%% non-zero values mean it is time to consider replacing
%% this nodes disk.
leveldb_read_block_errors(_) ->
    %% level stats are per node
    %% but the way to get them is
    %% is with riak_kv_vnode:vnode_status/1
    %% for that reason just chose a partition
    %% on this node at random
    %% and ask for it's stats
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:my_indices(R) of
        [] -> undefined;
        [Idx] ->
            Status = vnode_status(Idx),
            leveldb_read_block_errors(Status);
        Indices ->
            %% technically a call to status is a vnode
            %% operation, so spread the load by picking
            %% a vnode at random.
            Nth = crypto:rand_uniform(1, length(Indices)),
            Idx = lists:nth(Nth, Indices),
            case vnode_status(Idx) of
                {backend_status, BE, St} ->
                    leveldb_read_block_errors_int(BE, St);
                _ ->
                    undefined
            end
    end.

vnode_status(Idx) ->
    PList = [{Idx, node()}],
    [{Idx, [Status]}] = riak_kv_vnode:vnode_status(PList),
    Status.

leveldb_read_block_errors_int(riak_kv_eleveldb_backend, Status) ->
    rbe_val(proplists:get_value(read_block_error, Status));
leveldb_read_block_errors_int(riak_kv_multi_backend, Statuses) ->
    multibackend_read_block_errors(Statuses, undefined);
leveldb_read_block_errors_int(_, _) ->
    undefined.

multibackend_read_block_errors([], Val) ->
    rbe_val(Val);
multibackend_read_block_errors([{_Name, Status}|Rest], undefined) ->
    RBEVal = case proplists:get_value(mod, Status) of
                 riak_kv_eleveldb_backend ->
                     proplists:get_value(read_block_error, Status);
                 _ -> undefined
             end,
    multibackend_read_block_errors(Rest, RBEVal);
multibackend_read_block_errors(_, Val) ->
    rbe_val(Val).

rbe_val(Bin) when is_binary(Bin) ->
    list_to_integer(binary_to_list(Bin));
rbe_val(_) ->
    undefined.

%% All stat creation is serialized through riak_kv_stat.
%% Some stats are created on demand as part of the call to `update/1'.
%% When a stat error is caught, the stat must be deleted and recreated.
%% Since stat updates can happen from many processes concurrently
%% a stat that throws an error may already have been deleted and
%% recreated. To protect against needlessly deleting and recreating
%% an already 'fixed stat' first retry the stat update. There is a chance
%% that the retry succeeds as the stat has been recreated, but some on
%% demand stat it uses has not yet. Since stat creates are serialized
%% in riak_kv_stat re-registering a stat could cause a deadlock.
%% This loop is spawned as a process to avoid that.
stat_repair_loop() ->
    receive
        {re_register_stat, Arg} ->
            re_register_stat(Arg),
            stat_repair_loop();
        {'DOWN', _, process, _, _} ->
            ok;
        _ ->
            stat_repair_loop()
    end.

stat_repair_loop(Dad) ->
    erlang:monitor(process, Dad),
    stat_repair_loop().

re_register_stat(Arg) ->
    case (catch do_update(Arg)) of
        {'EXIT', _} ->
            Stats = stats_from_update_arg(Arg),
            [exometer:re_register(Name, Type)
             || {Name, {metric, _, Type, _}} <- Stats];
        ok ->
            ok
    end.

%% Map from application argument used in call to `update/1' to
%% folsom stat names and types.
%% Updates that create dynamic stats must select all
%% related stats.
stats_from_update_arg({vnode_get, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, vnode, gets]);
stats_from_update_arg({vnode_put, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, vnode, puts]);
stats_from_update_arg(vnode_index_read) ->
    riak_core_stat_q:names_and_types([?APP, vnode, index, reads]);
stats_from_update_arg({vnode_index_write, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, vnode, index, writes]) ++
        riak_core_stat_q:names_and_types([?APP, vnode, index, deletes]);
stats_from_update_arg({vnode_index_delete, _}) ->
    riak_core_stat_q:names_and_types([?APP, vnode, index, deletes]);
stats_from_update_arg({get_fsm, _, _, _, _, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, node, gets]);
stats_from_update_arg({put_fsm_time, _, _, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, node, puts]);
stats_from_update_arg({read_repairs, _, _}) ->
    riak_core_stat_q:names_and_types([?APP, nodes, gets, read_repairs]);
%% continue here
stats_from_update_arg(coord_redirs) ->
    [{{?APP, node, puts, coord_redirs}, {metric,[],counter,undefined}}];
stats_from_update_arg(mapper_start) ->
    [{{?APP, mapper_count}, {metric,[],counter,undefined}}];
stats_from_update_arg(mapper_end) ->
    stats_from_update_arg(mapper_start);
stats_from_update_arg(precommit_fail) ->
    [{{?APP, precommit_fail}, {metric,[],counter,undefined}}];
stats_from_update_arg(postcommit_fail) ->
    [{{?APP, postcommit_fail}, {metric,[],counter,undefined}}];
stats_from_update_arg({fsm_spawned, Type}) ->
    [{{?APP, node, Type, fsm, active}, {metric,[],counter,undefined}}];
stats_from_update_arg({fsm_exit, Type}) ->
    stats_from_update_arg({fsm_spawned, Type});
stats_from_update_arg({fsm_error, Type}) ->
    stats_from_update_arg({fsm_spawned, Type}) ++
        [{{?APP, node, Type, fsm, errors}, {metric,[], spiral, undefined}}];
stats_from_update_arg({index_create, _Pid}) ->
    [{{?APP, index, fsm, create}, {metric, [], spiral, undefined}},
     {{?APP, index, fsm, active}, {metric, [], counter, undefined}}];
stats_from_update_arg(index_create_error) ->
    [{{?APP, index, fsm, create, error}, {metric, [], spiral, undefined}}];
stats_from_update_arg({list_create, _Pid}) ->
    [{{?APP, list, fsm, create}, {metric, [], spiral, undefined}},
     {{?APP, list, fsm, active}, {metric, [], counter, undefined}}];
stats_from_update_arg(list_create_error) ->
    [{{?APP, list, fsm, create, error}, {metric, [], spiral, undefined}}];
stats_from_update_arg({DT, actor_count, _Value}) ->
    [{{?APP, DT, actor_count}, {metric, [], histogram, undefined}}];
stats_from_update_arg(late_put_fsm_coordinator_ack) ->
    [{{?APP, late_put_fsm_coordinator_ack}, {metric,[],counter,undefined}}];
stats_from_update_arg({consistent_get, _Bucket, _Microsecs, _ObjSize}) ->
    riak_core_stat_q:names_and_types([?APP, consistent, gets]);
stats_from_update_arg({consistent_put, _Bucket, _Microsecs, _ObjSize}) ->
    riak_core_stat_q:names_and_types([?APP, consistent, puts]);
stats_from_update_arg(_) ->
    [].

-ifdef(TEST).
-define(LEVEL_STATUS(Idx, Val),  [{Idx, [{backend_status, riak_kv_eleveldb_backend,
					  [{read_block_error, Val}]}]}]).
-define(BITCASK_STATUS(Idx),  [{Idx, [{backend_status, riak_kv_bitcask_backend,
				       []}]}]).
-define(MULTI_STATUS(Idx, Val), [{Idx,  [{backend_status, riak_kv_multi_backend, Val}]}]).

leveldb_rbe_test_int() ->
    {foreach,
     fun() ->
             meck:new(riak_core_ring_manager),
             meck:new(riak_core_ring),
             meck:new(riak_kv_vnode),
             meck:expect(riak_core_ring_manager, get_my_ring, fun() -> {ok, [fake_ring]} end)
     end,
     fun(_) ->
             meck:unload(riak_kv_vnode),
             meck:unload(riak_core_ring),
             meck:unload(riak_core_ring_manager)
     end,
     [{"Zero indexes", fun zero_indexes/0},
      {"Single index", fun single_index/0},
      {"Multi indexes", fun multi_index/0},
      {"Bitcask Backend", fun bitcask_backend/0},
      {"Multi Backend", fun multi_backend/0}]
    }.

zero_indexes() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [] end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

single_index() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, <<"100">>) end),
    ?assertEqual(100, leveldb_read_block_errors()),

    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, nonsense) end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

multi_index() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?LEVEL_STATUS(Idx, <<"100">>) end),
    ?assertEqual(100, leveldb_read_block_errors()).

bitcask_backend() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) -> ?BITCASK_STATUS(Idx) end),
    ?assertEqual(undefined, leveldb_read_block_errors()).

multi_backend() ->
    meck:expect(riak_core_ring, my_indices, fun(_R) -> [index1, index2, index3] end),
    %% some backends, none level
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) ->
                                                     ?MULTI_STATUS(Idx,
								   [{name1, [{mod, bitcask}]},
								    {name2, [{mod, fired_chicked}]}]
								  )
                                             end),
    ?assertEqual(undefined, leveldb_read_block_errors()),

    %% one or movel leveldb backends (first level answer is returned)
    meck:expect(riak_kv_vnode, vnode_status, fun([{Idx, _}]) ->
						     ?MULTI_STATUS(Idx,
								   [{name1, [{mod, bitcask}]},
								    {name2, [{mod, riak_kv_eleveldb_backend},
									     {read_block_error, <<"99">>}]},
								    {name2, [{mod, riak_kv_eleveldb_backend},
									     {read_block_error, <<"1000">>}]}]
								  )
                                             end),
    ?assertEqual(99, leveldb_read_block_errors()).

-endif.
