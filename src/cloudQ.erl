-module(cloudQ).

-export([aws_sqs/3,
         gcp_pub_sub/5,
         kafka_q/3,
         read_message/1,
         read_message/2,
         send_message/2,
         send_message_with_id/3,
         commit_message/2,
         kafka_partition/4
        ]).

-define(POOL_EXEC_T(Pool, Msg, Timeout),
        poolboy:transaction(Pool, fun(Worker) -> gen_server:call(Worker, Msg, Timeout) end)
       ).

-define(POOL_EXEC(Pool, Msg),
        poolboy:transaction(Pool, fun(Worker) -> gen_server:call(Worker, Msg) end)
       ).

-define(L2B(X), erlang:list_to_binary(X)).

-spec aws_sqs(string(), list(), list()) -> {ok, atom()}.
aws_sqs(Queue, Opts, Args) ->
    QName     = "aws_" ++ Queue,
    WorkerNum = get_config(size, Opts, 2),
    WorkerMod = get_config(wmodule, Opts, cq_sqs),
    PoolName  = pool_name(QName),

    PoolArgs  = [{name, {local, PoolName}},
                 {worker_module, WorkerMod},
                 {size, WorkerNum},
                 {strategy, fifo},
                 {max_overflow, WorkerNum*2}],


    Args2      = [{qname, Queue} | Args],
    WorkerSpec = poolboy:child_spec(PoolName, PoolArgs, Args2),
    cloudQ_sup:start_q(WorkerSpec),
    {ok, PoolName}.

-spec gcp_pub_sub(string(), string(), string(), list(), list()) -> {ok, atom()}.
gcp_pub_sub(Project, Topic, Subscription, Opts, Args) ->
    QName     = "gcp_" ++ Project ++ Topic,
    WorkerNum = get_config(size, Opts, 2),
    WorkerMod = get_config(wmodule, Opts, cq_gcp),
    PoolName  = pool_name(QName),

    PoolArgs  = [{name, {local, PoolName}},
                 {worker_module, WorkerMod},
                 {size, WorkerNum},
                 {strategy, fifo},
                 {max_overflow, WorkerNum*2}],

    Args2      = [{project, ?L2B(Project)}, {topic, ?L2B(Topic)},
                  {subscription, ?L2B(Subscription)} | Args],
    WorkerSpec = poolboy:child_spec(PoolName, PoolArgs, Args2),
    cloudQ_sup:start_q(WorkerSpec),
    {ok, PoolName}.


kafka_q(Topic, Opts, Args) ->
    QName         = "kafka_" ++ binary_to_list(Topic),
    Client        = list_to_atom(QName),
    PoolName      = pool_name(QName),
    WorkerMod     = get_config(wmodule, Opts, cq_kafka),
    Endpoints     = get_config(endpoints, Opts, [{"localhost", 9092}]),
    {_, PartList} = lists:keyfind(assigned_partitions, 1, Opts),
    WorkerNum     = length(PartList),

    %% Consisting hashing function to map msgs with same key to the same
    %% partition for sequential processing and also ensure that each partition
    %% is equally populated
    Args2      = [{topic, Topic},
                  {partition_fun, fun ?MODULE:kafka_partition/4},
                  {client, Client} | Args],
    PoolArgs   = poolboy_args(PoolName, WorkerMod, WorkerNum),
    WorkerSpec = poolboy:child_spec(PoolName, PoolArgs, Args2),
    cloudQ_sup:start_q(WorkerSpec),

    %% assign one partition to each worker
    %% NOTE: poolboy workers will be accessed in a round-robin fashion
    %% hence we will be able to consume all the partitions equally
    lists:map(fun(Number) ->
                      ok = WorkerMod:allocate_partition(PoolName, Number)
              end, PartList),

    %% {ok, NumPartions} = brod:get_partitions_count(Client, Topic),
    ok = brod:start_client(Endpoints, Client),
    ok = brod:start_producer(Client, Topic, []),
    ok = brod:start_consumer(Client, Topic, []),

    {ok, PoolName}.




-spec kafka_partition(binary(), integer(), binary(), binary()) -> {ok, integer()}.
kafka_partition(_Topic, PartitionsCount, Key, _Value) when is_binary(Key) ->
    kafka_partition(_Topic, PartitionsCount, binary_to_list(Key), _Value);

kafka_partition(_Topic, PartitionsCount, Key, _Value) when is_list(Key) ->
    Key_I = lists:foldl(fun(K, Sum) -> K + Sum end, 0, Key),
    {ok, jch:ch(Key_I, PartitionsCount)}.



read_message(QName) -> read_message(QName, 10).
read_message(QName, Limit) ->
    ?POOL_EXEC_T(QName, {read_message, Limit}, infinity).

send_message(QName, Msg) ->
    ?POOL_EXEC(QName, {send_message, Msg}).

-spec send_message_with_id(binary(), binary(), binary()) -> ok.
send_message_with_id(QName, Id, Msg) ->
    ?POOL_EXEC(QName, {send_message, Id, Msg}).


commit_message(QName, MsgMeta) ->
    ?POOL_EXEC(QName, {commit_message, MsgMeta}).



%% =====================
%% --- internal funs ---
%% =====================
poolboy_args(PoolName, WorkerMod, WorkerNum) ->
    [{name, {local, PoolName}},
     {worker_module, WorkerMod},
     {size, WorkerNum},
     {strategy, fifo},
     {max_overflow, WorkerNum*2}].


get_config(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        false -> Default;
        {_, Value} -> Value
    end.

pool_name(Name) ->
    list_to_atom(
      lists:flatten(
        lists:map(
          fun(X) ->
                  io_lib:format("~2.16.0b", [X])
          end, binary_to_list(crypto:hash(md5, Name))
         )
       )
     ).
