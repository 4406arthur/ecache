-module(ecache).

-export([insert/3, get/2, empty/1, total_size/1, stats/1, del/2]).

-export([cache_ttl_sup/3]).
-define(TIMEOUT, infinity).

%% ===================================================================
%% Supervisory helpers
%% ===================================================================

cache_ttl_sup(Name, Size, TTL) ->
  {Name,
    {ecache_server, start_link, [Name, Size, TTL]},
     permanent, brutal_kill, worker, [ecache_server]}.

%% ===================================================================
%% Calls into ecache_server
%% ===================================================================\
insert(ServerName, Key, Value) ->
  gen_server:cast({global,ServerName}, {insert, Key, Value}).

get(ServerName, Key) ->
  gen_server:call({global,ServerName}, {get, Key}, ?TIMEOUT).

del(ServerName, Key) ->
  gen_server:cast({global,ServerName}, {del, Key}).

empty(ServerName) ->
  gen_server:call({global,ServerName}, empty).

total_size(ServerName) ->
  gen_server:call({global,ServerName}, total_size).

stats(ServerName) ->
  gen_server:call({global,ServerName}, stats).
