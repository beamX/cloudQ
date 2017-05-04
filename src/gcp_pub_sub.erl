-module(gcp_pub_sub).

-export([create_topic/2,
         delete_topic/2,
         publish_message/3,
         subscribe_topic/3,
         unsubscribe_topic/2,
         pull_message/3,
         ack_message/3
        ]).

%% -define(GCP_PUBSUB,  <<"https://pubsub.googleapis.com/v1">>).
-define(GCP_PUBSUB(Path),  <<"http://localhost:8042/v1", Path/bitstring>>).
-define(GCP_PUBSUB_URL(Project, Topic),
        ?GCP_PUBSUB(<<"/projects/", Project/bitstring, "/topics/", Topic/bitstring>>)).

-define(GCP_PUBSUB_SUBS(Project, Subs),
        ?GCP_PUBSUB(<<"/projects/", Project/bitstring, "/subscriptions/", Subs/bitstring>>)).

-define(GCP_PUBSUB_MSG_PULL(Project, Subs),
        ?GCP_PUBSUB(<<"/projects/", Project/bitstring, "/subscriptions/", Subs/bitstring, ":pull">>)).

-define(GCP_PUBSUB_MSG_ACK(Project, Subs),
        ?GCP_PUBSUB(<<"/projects/", Project/bitstring, "/subscriptions/", Subs/bitstring, ":acknowledge">>)).

-define(AUTH(Headers), begin
                           {ok, Key} = application:get_env(cloudQ, gcp_auth),
                           [{<<"Authorization">>, <<"Bearer ", Key/bitstring>>} | Headers]
                       end).

-spec create_topic(binary(), binary()) -> {ok, success} | {error, failed}.
create_topic(Project, Topic) ->
    URL  = ?GCP_PUBSUB_URL(Project, Topic),
    Resp = http:put(URL, ?AUTH([]), [], []),
    process_resp(Resp).


-spec delete_topic(binary(), binary()) -> {ok, success} | {error, failed}.
delete_topic(Project, Topic) ->
    URL = ?GCP_PUBSUB_URL(Project, Topic),
    Resp = http:delete(URL, ?AUTH([]), [], []),
    process_resp(Resp).


-spec publish_message(binary(), binary(), list()) -> {ok, success} | {error, failed}.
publish_message(Project, Topic, MsgIO) ->
    URL     = ?GCP_PUBSUB_URL(Project, Topic),
    Data    = base64:encode(MsgIO),
    Payload = jsx:encode([{messages, [ [{data, Data}] ]}]),
    Resp    = http:post(<<URL/bitstring, ":publish">>, ?AUTH([{<<"Content-Type">>, <<"application/json">>}]), Payload, []),
    process_resp(Resp).

-spec subscribe_topic(binary(), binary(), binary()) -> {ok, success} | {error, failed}.
subscribe_topic(Project, Topic, SubsName) ->
    URL  = ?GCP_PUBSUB_SUBS(Project, SubsName),
    Payload = jsx:encode([{<<"topic">>, <<"projects/", Project/bitstring,
                                          "/topics/", Topic/bitstring>>}]),
    Resp = http:put(URL, ?AUTH([{<<"Content-Type">>, <<"application/json">>}]), Payload, []),
    process_resp(Resp).

-spec unsubscribe_topic(binary(), binary()) -> {ok, success} | {error, failed}.
unsubscribe_topic(Project, SubsName) ->
    URL  = ?GCP_PUBSUB_SUBS(Project, SubsName),
    Resp = http:delete(URL, ?AUTH([]), [], []),
    process_resp(Resp).

-spec pull_message(binary(), binary(), integer()) -> {ok, binary()} | {error, failed}.
pull_message(Project, Subs, Limit) ->
    Payload = jsx:encode([{<<"returnImmediately">>, <<"true">>},
                          {<<"maxMessages">>, integer_to_binary(Limit)}]),
    URL  = ?GCP_PUBSUB_MSG_PULL(Project, Subs),
    Resp = http:post(URL, ?AUTH([{<<"Content-Type">>, <<"application/json">>}]), Payload, []),
    case handle_resp(Resp) of
        {ok, 200, _, ClientRef} ->
            {ok, Body} = http:resp_body(ClientRef),
            {ok, jsx:decode(Body)};
        _ -> {error, failed}
    end.

-spec ack_message(binary(), binary(), integer()) -> {ok, success} | {error, failed}.
ack_message(Project, Subs, AckIds) ->
    Payload = jsx:encode([{<<"ackIds">>, AckIds}]),
    URL     = ?GCP_PUBSUB_MSG_ACK(Project, Subs),
    Resp    = http:post(URL, ?AUTH([{<<"Content-Type">>, <<"application/json">>}]), Payload, []),
    case handle_resp(Resp) of
        {ok, 200, _, _} -> {ok, success};
        _ -> {error, failed}
    end.


%% internal fun
process_resp(Resp) ->
    case handle_resp(Resp) of
        {ok, 200, _, _} -> {ok, success};
        _ -> {error, failed}
    end.

handle_resp({ok, StatusCode, RespHeaders, ClientRef}) ->
    {ok, StatusCode, RespHeaders, ClientRef};
handle_resp({error, Reason}) ->
    lager:log(error, [], "Request failed: ~p~n", [Reason]),
    {error, failed}.
