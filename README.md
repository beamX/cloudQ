# CloudQ

CloudQ aims to provide a unified way to interact with different message queue solutions primarily to ease the interop when switching from one queue to another.


## Supports
  - AWS SQS
  - Google Pub/Sub
  - Kafka

## Usage of Kafka interface
   The Kafka interface in CloudQ uses `brod` library under the hood. Following is Kafka api

   Erlang
   ```
   {ok, Client} = cloudQ:kafka_q(TopicName, Options, Args)
   ```

   - `TopicName`: Kafka topic name to write and read from.
   - `Options`: a tuple list which specifies configration options for kafka worker pool

     - `endpoints` : list of kafka endpoints eg. `[{"localhost", 9092}]`
     - `msg_handler`: function (of arity 2) for handling messages read from kafka topic. eg `{msg_handler, fun my_module:handle_message/2}`. The first argument is the message (`{Key, Value}`) which is read from the topic and the second argument is `msg_handler_state`.
     - `msg_handler_state`: any term which will be passed to the `msg_handler` eg `%{}`
   - `Args` : additional args for the worker pool (poolboy). Should be `[]` in most cases.

   Following are example uses of the API in Erlang/Elixir.


   Erlang
   ```Erlang
   {ok, Client} = cloudQ:kafka_q(TopicName, [{msg_handler, fun my_module:handle_message/2}], [])
   ```


   Elixir
   ```Elixir
   {:ok, client} = :cloudQ.kafka_q(topic_name, [msg_handler: &MyModule.handle_message/2], [])
   ```


   The above code will start a kafka worker pool.
   
   Following is an example of `msg_handler` function
   
   
   ```Elixir
    def handle_q_message({key, msg}, msg_handler_state) do
      do_some_work(key, msg)
      :ack
    end
   ```
   
   Note that the function returns `:ack`, which is required to commit the offset. The function can also return `{:ack, new_msg_handler_state}` to update the handler's state.
   
   
   
## TODO supports
  - Rabbitmq
