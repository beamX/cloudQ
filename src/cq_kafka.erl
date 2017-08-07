-module(cq_kafka).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1,
         allocate_partition/2,
         handle_message/3
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).



-include_lib("brod/include/brod.hrl").

allocate_partition(PoolName, Number) ->
    poolboy:transaction(PoolName,
                        fun(Worker) ->
                                gen_server:call(Worker, {allocate_partition, Number})
                        end).

handle_message(_Partition, Msg, CbStateIn) ->
    lager:log(info, [], "~p~n", [Msg]),
    {ok, CbStateIn}.



start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    {_, Topic}      = lists:keyfind(topic, 1, Args),
    {_, Client}     = lists:keyfind(client, 1, Args),
    {_, PartFun}    = lists:keyfind(partition_fun, 1, Args),
    {_, Hosts}      = lists:keyfind(hosts, 1, Args),
    {ok, #{topic         => Topic,
           client        => Client,
           partition_fun => PartFun,
           hosts         => Hosts,
           partition     => none}}.

handle_call({allocate_partition, Number}, _From, #{hosts := _Hosts, client := Client, topic := Topic} = State) ->
    ok = brod:subscribe(Client, self(), Topic, Number, []),
    {reply, ok, State#{partition => Number}};

handle_call({send_message, Id, Msg}, _From, #{topic         := Topic,
                                              client        := Client,
                                              partition_fun := PartFun} = State) ->

    Ret = brod:produce_sync(Client, Topic, PartFun, Id, Msg),
    {reply, Ret, State};

handle_call({read_message, _Limit}, _From, #{topic     := Topic,
                                             client    := _Client,
                                             partition := Partition,
                                             hosts     := Hosts} = State) ->
    {ok, [Offset]} = brod:get_offsets(Hosts, Topic, Partition),
    lager:log(info, [], "OFFESET: ~p~n", [Offset]),
    {ok, [Msg]}    = brod:fetch(Hosts, Topic, Partition, Offset - 1),
    lager:log(info, [], "~p~n", [Msg]),
    %% This is wrong, fix this !
    %% ok = brod:consume_ack(Client, Topic, Partition, Offset),
    {reply, Msg, State};

handle_call({commit_message, Offset}, _From, #{topic      := Topic,
                                                client    := Client,
                                                partition := Partition} = State) ->
    ok = brod:consume_ack(Client, Topic, Partition, Offset),
    {reply, ok, State};


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    lager:log(info, [], "UNKNOWN: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

