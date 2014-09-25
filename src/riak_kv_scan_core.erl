-module(riak_kv_scan_core).

-export([add_result/3,
         enough/1,
         response/1]).

-export_type([scancore/0, result/0, reply/0]).

-type result() :: {ok, list()} |
                  {error, notfound} | % for dialyzer
                  {error, any()}.

-type reply() :: {ok, list()} |
                 {error, notfound} |
                 {error, any()}.

-type idxresult() :: {non_neg_integer(), result()}.

-record(scancore, {n :: pos_integer(),
                   r :: pos_integer(),
                   fail_threshold :: pos_integer(),
                   results = [] :: [idxresult()],
                   merged :: {notfound | ok, list() | undefined},
                   num_ok = 0 :: non_neg_integer(),
                   num_fail = 0 :: non_neg_integer()}).

-opaque scancore() :: #scancore{}.

add_result(Idx, {error, notfound} = Result, ScanCore) ->
    ScanCore#scancore{
      results = [{Idx, Result}|ScanCore#scancore.results]};
add_result(Idx, {error, _Reason} = Result, ScanCore) ->
    ScanCore#scancore{
      results = [{Idx, Result}|ScanCore#scancore.results],
      num_fail = ScanCore#scancore.num_fail + 1}.

enough(#scancore{r = R, num_ok = NumOK}) when
      NumOK >= R ->
    true;
%% too many failures
enough(#scancore{fail_threshold = FailThreshold,
                 num_fail = NumFail}) when NumFail >= FailThreshold ->
    true;
enough(_) ->
    false.

response(#scancore{r = R, num_ok = NumOK, results = Results} = ScanCore)
  when NumOK >= R ->
    {ObjState, _MObj} = Merged = merge(Results),
    Reply = case ObjState of
                ok ->
                    Merged; % {ok, MObj}
                _ -> % tombstone or notfound
                    {error, notfound}
            end,
    {Reply, ScanCore#scancore{merged = Merged}}.

%% get the scan object of largest size
larger(L1, L2) ->
    case length(L1) > length(L2) of
        true -> L1;
        false -> L2
    end.

merge(Replies) ->
    RObjs = [RObj || {_I, {ok, RObj}} <- Replies],
    case RObjs of
        [] ->
            {notfound, undefined};
        _ ->
            Merged = lists:foldl(fun larger/2, [], RObjs),
            {ok, Merged}
    end.
