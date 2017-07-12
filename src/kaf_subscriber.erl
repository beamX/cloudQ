-module(kaf_subscriber).
-include_lib("deps/brod/include/brod.hrl").

-export([start/5,
        process_msg/1]).
-export([init/2,
         handle_message/4]). %% callback api

%% brod_group_subscriber behaviour callback
init(_GroupId, Args) ->
    lager:log(info, [], "Args: ~p~n", [Args]),
    {_, MsgHandler} = lists:keyfind(msg_handler, 1, Args),
    {_, MsgHandlerState} = lists:keyfind(msg_handler_state, 1, Args),
    {ok, #{msg_handler => MsgHandler, msg_handler_state => MsgHandlerState}}.

%% brod_group_subscriber behaviour callback
handle_message(_Topic, _Partition, Message, #{msg_handler := MsgHandler,
                                              msg_handler_state := MsgHandlerState} = State) ->
    #kafka_message{ offset = Offset
                  , key   = Key
                  , value = Value
                  } = Message,
    lager:log(info, [], "RECV: ~p, ~p, ~p, ~p~n", [Offset, Key, Value, State]),
    case MsgHandler({Key, Value}, MsgHandlerState) of
        ok -> {ok, State};
        ack -> {ok, ack, State};
        {ok, MsgHandlerState2} -> {ok, State#{msg_handler_state => MsgHandlerState2}};
        {ack, MsgHandlerState2} -> {ok, ack, State#{msg_handler_state => MsgHandlerState2}}
    end.

process_msg({Key, Value}) ->
    lager:log(info, [], "PROCESSING: ~p ~p ~n", [Key, Value]),
    ok.



start(ClientId, Topic, GroupId, MsgHandler, MsgHandlerState) ->
    %% commit offsets to kafka every 5 seconds
    GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
                   {offset_commit_interval_seconds, 5}
                  ],
    ConsumerConfig = [{begin_offset, earliest}],
    brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE,
                                     [{msg_handler, MsgHandler},
                                      {msg_handler_state, MsgHandlerState}]).
