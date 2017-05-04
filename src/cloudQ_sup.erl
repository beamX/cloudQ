-module(cloudQ_sup).
-behaviour(supervisor).

-export([start_q/1
        ]).

-export([start_link/0]).
-export([init/1]).


start_q(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Procs = [],
	{ok, {{one_for_one, 1, 5}, Procs}}.


