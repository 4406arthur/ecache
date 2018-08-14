-module(ecache_server).

-behaviour(gen_server).

-export([start_link/1, start_link/2, start_link/3, start_link/4]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(cache, {name, datum_index,
                reaper_pid, cache_size,
                cache_policy, default_ttl, nodes}).

-record(datum, {key, data, started, ttl_reaper = nil, last_active, ttl, type = mru, remaining_ttl}).

% make 8 MB cache
start_link(Name) ->
  start_link(Name, 64).

% make 1 minute expiry cache
start_link(Name, CacheSize) ->
  start_link(Name, CacheSize, 60000).

% make MRU policy cache
start_link(Name, CacheSize, CacheTime) ->
  start_link(Name, CacheSize, CacheTime, mru).

start_link(Name, CacheSize, CacheTime, CachePolicy) ->
  gen_server:start_link({global, Name},
    ?MODULE, [Name, CacheSize, CacheTime, CachePolicy], []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init([Name, CacheSize, CacheTime, CachePolicy]) ->
  DatumIndex = ets:new(Name, [set,
                              compressed,  % yay compression
                              public,      % public because we spawn writers
                              {keypos, 2}, % use Key stored in record
                              {read_concurrency, true}]),
  case CacheSize of
    unlimited -> ReaperPid = nil, CacheSizeBytes = unlimited;
            _ -> CacheSizeBytes = CacheSize*1024*1024, 
                 {ok, ReaperPid} = ecache_reaper:start(Name, CacheSizeBytes),
                 erlang:monitor(process, ReaperPid)
  end,

  NodeList = erlang:nodes(),
  State = #cache{name = Name,
                 datum_index = DatumIndex,
                 reaper_pid = ReaperPid,
                 default_ttl = CacheTime,
                 cache_policy = CachePolicy,
                 cache_size = CacheSizeBytes,
                 nodes = NodeList
                },
  error_logger:info_msg("ecache server state: ~p~n", [State]),
  lager:start(),
  {ok, State}.

locate(Key, State) ->
  case fetch_data(Key, State) of
    {ecache, notfound} ->
      {not_found, nil};
    Data ->
      {found, Data}
  end.

handle_call({get, Key}, From, #cache{datum_index = _DatumIndex} = State) ->
  error_logger:info_msg("Requesting: (~p)~n", [Key]),
  spawn(fun() ->
          Reply = 
          case locate(Key, State) of
            {_, Data} -> Data
          end,
          gen_server:reply(From, Reply)
        end),
  {noreply, State};

% NB: total_size using ETS includes ETS overhead.  An empty table still
% has a size.
handle_call(total_size, _From, #cache{datum_index = DatumIndex} = State) ->
  TableBytes = ets:info(DatumIndex, memory) * erlang:system_info(wordsize),
  {reply, TableBytes, State};

handle_call(stats, _From, #cache{datum_index = DatumIndex} = State) ->
  EtsInfo = ets:info(DatumIndex),
  CacheName = proplists:get_value(name, EtsInfo),
  DatumCount = proplists:get_value(size, EtsInfo),
  Bytes = proplists:get_value(memory, EtsInfo) * erlang:system_info(wordsize),
  Stats = [{cache_name, CacheName},
           {memory_size_bytes, Bytes},
           {datum_count, DatumCount}],
  {reply, Stats, State};

handle_call(empty, _From, #cache{datum_index = DatumIndex} = State) ->
  ets:delete_all_objects(DatumIndex),
  {reply, ok, State};

handle_call(reap_oldest, _From, #cache{datum_index = DatumIndex} = State) ->
  LeastActive =
    ets:foldl(fun(A, Acc) when A#datum.last_active < Acc -> A;
                 (_, Acc) -> Acc
              end,
              os:timestamp(),
              DatumIndex),
  ets:delete(DatumIndex, LeastActive),
  {from, ok, State}.

handle_cast({insert, Key, Value}, #cache{datum_index = DatumIndex, default_ttl = TTL, cache_policy = CachePolicy, nodes = Nodes} = State) ->
  Datum = create_datum(Key, Value, TTL, CachePolicy),
  error_logger:info_msg("my cache data detail: ~p~n", [Datum]),
  case Nodes of
    [] ->
      launch_datum_ttl_reaper(DatumIndex, Key, Datum, erlang:node());
    Nodes when length(Nodes) > 0 ->
      Index = rand:uniform(length(Nodes)),
      Node = lists:nth(Index,Nodes),
      launch_datum_ttl_reaper(DatumIndex, Key, Datum, Node)
  end,
  ets:insert(DatumIndex, Datum),
  {noreply, State};

handle_cast({del, Key}, #cache{datum_index = DatumIndex} = State) ->
  ets:delete(DatumIndex, Key),
  {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info({destroy,_DatumPid, ok}, State) ->
  {noreply, State};

handle_info({'DOWN', _Ref, process, ReaperPid, _Reason}, 
    #cache{reaper_pid = ReaperPid, name = Name, cache_size = Size} = State) ->
  {NewReaperPid, _Mon} = ecache_reaper:start_link(Name, Size),
  {noreply, State#cache{reaper_pid = NewReaperPid}};

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
  {noreply, State};

handle_info(Info, State) ->
  io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ===================================================================
%% Private
%% ===================================================================

-compile({inline, [{create_datum, 4}]}).
create_datum(DatumKey, Data, TTL, Type) ->
  Timestamp = os:timestamp(),
  #datum{key = DatumKey, data = Data, started = Timestamp,
         ttl = TTL, remaining_ttl = TTL, type = Type,
         last_active = Timestamp}.

reap_after(EtsIndex, Key, LifeTTL) ->
  receive
    {update_ttl, NewTTL} ->
      reap_after(EtsIndex, Key, NewTTL)
  after
    LifeTTL ->
      error_logger:info_msg("time to delete the cache: ~p:~p~n", [EtsIndex,Key]),
      ets:delete(EtsIndex, Key)
      %%exit(self(), kill)
  end.

launch_datum_ttl_reaper(_, _, #datum{remaining_ttl = unlimited} = Datum, _) ->
  Datum;
launch_datum_ttl_reaper(EtsIndex, Key, #datum{remaining_ttl = TTL} = Datum, Node) ->
    Reaper = spawn(Node, fun() -> reap_after(EtsIndex, Key, TTL) end),
    Datum#datum{ttl_reaper = Reaper}.

-compile({inline, [{data_from_datum, 1}]}).
data_from_datum(#datum{data = Data}) -> Data.

fetch_data(Key, #cache{datum_index = DatumIndex}) ->
  case ets:lookup(DatumIndex, Key) of
    [Datum] ->
      data_from_datum(Datum);
    [] ->
      {ecache, notfound}
  end.

%% ===================================================================
%% Data Abstraction
%% ===================================================================

