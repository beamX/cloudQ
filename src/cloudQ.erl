-module(cloudQ).

-export([aws_sqs/3,
         gcp_pub_sub/5,
         read_message/1,
         read_message/2,
         send_message/2,
         commit_message/2
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

    Args2      = [{qname, QName} | Args],
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


read_message(QName) -> read_message(QName, 10).
read_message(QName, Limit) ->
    ?POOL_EXEC_T(QName, {read_message, Limit}, infinity).

send_message(QName, Msg) ->
    ?POOL_EXEC(QName, {send_message, Msg}).

commit_message(QName, MsgMeta) ->
    ?POOL_EXEC(QName, {commit_message, MsgMeta}).



%% =====================
%% --- internal funs ---
%% =====================
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
