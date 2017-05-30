-module(kaf_subscriber).
-include_lib("deps/brod/include/brod.hrl").

-export([start/4,
        process_msg/1]).
-export([init/2,
         handle_message/4]). %% callback api

%% brod_group_subscriber behaviour callback
init(_GroupId, Args) ->
    lager:log(info, [], "Args: ~p~n", [Args]),
    {_, MsgHandler} = lists:keyfind(msg_handler, 1, Args),
    {ok, #{msg_handler => MsgHandler}}.

%% brod_group_subscriber behaviour callback
handle_message(_Topic, _Partition, Message, #{msg_handler := MsgHandler} = State) ->
    #kafka_message{ offset = Offset
                  , key   = Key
                  , value = Value
                  } = Message,
    lager:log(info, [], "RECV: ~p, ~p, ~p, ~p~n", [Offset, Key, Value, State]),
    case MsgHandler({Key, Value}) of
        ok -> {ok, State};
        ack -> {ok, ack, State}
    end.

process_msg({Key, Value}) ->
    lager:log(info, [], "PROCESSING: ~p ~p ~n", [Key, Value]),
    ok.



start(ClientId, Topic, GroupId, MsgHandler) ->
    %% commit offsets to kafka every 5 seconds
    GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
                   {offset_commit_interval_seconds, 5}
                  ],
    ConsumerConfig = [{begin_offset, earliest}],
    brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE,
                                     [{msg_handler, MsgHandler}]).
