-module(cq_sqs).

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
    {_, QName} = lists:keyfind(qname, 1, Args),
    Config     = case lists:keyfind(config, 1, Args) of
                     false -> erlcloud_aws:default_config();
                     {config, Conf} -> Conf
                 end,
    {ok, #{qname => QName, config => Config}}.


handle_call({read_message, Limit}, _From, #{qname := QName, config := Config} = State) ->
    Msgs = erlcloud_sqs:receive_message(QName, [], Limit, Config),
    Ret  = case lists:keyfind(messages, 1, Msgs) of
               {messages, MsgList} -> {ok, process_msg_list(MsgList)};
               _ -> {error, unkown_resp}
           end,
    {reply, Ret, State};

%% Msgs = io_list
handle_call({send_message, Msg}, _From, #{qname := QName, config := Config} = State) ->
    Ret = erlcloud_sqs:send_message(QName, Msg, none, Config),
    {reply, Ret, State};

handle_call({commit_message, MsgMeta}, _From, #{qname := QName, config := Config} = State) ->
    lists:map(fun(Meta) ->
                      {_, RH} = lists:keyfind(receipt_handle, 1, Meta),
                      ok = erlcloud_sqs:delete_message(QName, RH, Config)
              end, MsgMeta),
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
    process_msg_list(MsgList, []).

process_msg_list([], Acc) -> lists:reverse(Acc);
process_msg_list([Msg | Rest], Acc) ->
    {body, Body} = lists:keyfind(body, 1, Msg),
    PMsg = [{body, Body},
            {message_meta, lists:keydelete(body, 1, Msg)}],
    process_msg_list(Rest, [PMsg | Acc]).
