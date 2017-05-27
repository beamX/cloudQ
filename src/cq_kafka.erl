-module(cq_kafka).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    {_, Topic}      = lists:keyfind(topic, 1, Args),
    {_, Client}     = lists:keyfind(client, 1, Args),
    {_, Partitions} = lists:keyfind(partitions, 1, Args),
    {_, PartFun}    = lists:keyfind(partition_fun, 1, Args),
    {ok, #{topic         => Topic,
           client        => Client,
           partition_fun => PartFun,
           partitions    => Partitions}}.


handle_call({send_message, Id, Msg}, _From, #{topic := Topic,
                                              client := Client,
                                              partition_fun := PartFun} = State) ->
    Ret = brod:produce_sync(Client, Topic, PartFun, Id, Msg),
    {reply, Ret, State};

%% round-robin read from paritions
handle_call({read_message, _Limit}, _From, #{topic      := Topic,
                                             client     := Client,
                                             partition  := Partition,
                                             hosts      := Hosts} = State) ->
    {ok, [Offset]} = brod:get_offsets(Hosts, Topic, Partition),
    {ok, [Msg]}    = brod:fetch(Hosts, Topic, Partition, Offset - 1),
    %% This is wrong, fix this !
    OffsetNew = Offset + 1,
    brod:consume_ack(Client, Topic, Partition, OffsetNew),
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

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

