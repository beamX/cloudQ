-module(cq_gcp).

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
    {_, Project} = lists:keyfind(project, 1, Args),
    {_, Topic}   = lists:keyfind(topic, 1, Args),
    {_, Subs}    = lists:keyfind(subscription, 1, Args),
    {ok, #{project => Project, subscription => Subs, topic => Topic}}.


handle_call({read_message, Limit}, _From,
            #{project := Project, subscription := Subs} = State) ->
    Ret = case gcp_pub_sub:pull_message(Project, Subs, Limit) of
              {ok, [{}]} -> {ok, []};
              {ok, MsgList} -> {ok, process_msg_list(MsgList)};
              _ -> {error, unkown_resp}
          end,
    {reply, Ret, State};

%% Msgs = io_list
%% Msg here is expected to be json encoded string. This api call
%% is expected to push a single message to the queue
handle_call({send_message, Msg}, _From,
            #{project := Project, topic := Topic} = State) ->
    {ok, success} = gcp_pub_sub:publish_message(Project, Topic, Msg),
    {reply, ok, State};

handle_call({commit_message, MsgMeta}, _From,
            #{project := Project, subscription := Subs} = State) ->
    AckIds = lists:map(fun(Meta) ->
                               {_, AckId} = lists:keyfind(<<"ackId">>, 1, Meta),
                               AckId
                       end, MsgMeta),
    {ok, success} = gcp_pub_sub:ack_message(Project, Subs, AckIds),
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


process_msg_list(MsgList) ->
    {_, MsgList2} = lists:keyfind(<<"receivedMessages">>, 1, MsgList),
    process_msg_list(MsgList2, []).

process_msg_list([], Acc) -> lists:reverse(Acc);
process_msg_list([Packet | Rest], Acc) ->
    {_, Msg}  = lists:keyfind(<<"message">>, 1, Packet),
    {_, Body} = lists:keyfind(<<"data">>, 1, Msg),
    Packet2   = lists:keydelete(<<"message">>, 1, Packet),
    Msg2      = lists:keydelete(<<"data">>, 1, Msg),
    MsgMeta   = lists:flatten([Msg2 | Packet2]),

    PMsg = [{body, base64:decode(Body)},
            {message_meta, MsgMeta}],
    process_msg_list(Rest, [PMsg | Acc]).

