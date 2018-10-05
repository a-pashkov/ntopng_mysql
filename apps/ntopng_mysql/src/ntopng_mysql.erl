-module(ntopng_mysql).

-behaviour(gen_server).

-compile(export_all).

-export([start_link/0]).

-export([listner_start/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-record(state, {
                lsocket, 
                socket, 
                buffer = <<"">>, 
                data = []
               }). 
-record(data, {
               ntoptimestamp, 
               srcaddr, 
               srcport, 
               dstaddr, 
               dstport, 
               inpkts, 
               inbytes, 
               outpkts, 
               outbytes, 
               firstswitched, 
               lastswitched,
               nin
              }).
-define(BUFF_SIZE, 2000). 
-define(PORT, 5510).
-define(TIMEOUT, 100).
-define(OPTIONS, [binary,{backlog, 128},{active,once},{buffer, 65536},{packet,0},{reuseaddr,true}]).

start_link()->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([]) ->
  % Подключение к MySQL

  % Слушатель
  register(listener, spawn_link(?MODULE, listner_start, [?PORT])), 
  {ok, #state{}}.

handle_cast({data,Packet}, #state{buffer=Buffer,data=Data}=State)-> 
  [NewBuffer|RcvData] = lists:reverse(binary:split(<<Buffer/binary, Packet/binary>>, <<"\n">>, [global])), 
  NewData = lists:append(RcvData, Data), 
  RestData = case length(NewData) of
    Len when Len >= ?BUFF_SIZE -> 
      {TData,HData} = lists:split(Len-?BUFF_SIZE,  NewData),
      send(HData),
      TData;
    _Len -> NewData
  end,
  {noreply, State#state{buffer=NewBuffer, data=RestData}, ?TIMEOUT};  
handle_cast(Msg, State) ->
  io:format("handle_cast: ~p\n", [{Msg, State}]),
  {noreply, State}.

handle_call(Request, From, State) -> 
  io:format("handle_call: ~p\n", [{Request, From, State}]),
  {reply, ok, State}.

handle_info(timeout, #state{data=Data}=State)-> 
  send(Data), 
  {noreply, State#state{data=[]}};
handle_info(Info, State) ->
  lager:info("handle_info: ~p\n", [{Info, State}]),
  {noreply, State}.

terminate(normal, #state{socket=Socket}) -> 
  % Закрыть MySQL
  % Закрыть сокет  
    gen_udp:close(Socket), 
    ok;
terminate(Reason, #state{socket=Socket} = State) -> 
    lager:info("terminate: ~p~n", [{Reason, State}]), %% Ненормальное завершение
    gen_tcp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

send(Data)-> 
  lager:info("Send data length: ~p~n", [length(Data)]), 
  [HData|TData] = Data, 
  lager:info("Data: ~p~n", [HData]).

prepare_data(Data)-> 
  lists:mapfoldl(fun(Elem, Acc)-> 
    case Elem of 
     {ok, #data{
                ntoptimestamp=NtopTimestamp,
                srcaddr=SrcAddr,
                srcport=SrcPort,
                dstaddr=DstAddr,
                dstport=DstPort,
                inpkts=InPkts,
                inbytes=InBytes,
                outpkts=OutPkts,
                outbytes=OutBytes,
                firstswitched=FirstSwitched,
                lastswitched=LastSwitched,
                nin=NtopngInstanceName
               }}-> 
                 {ok, [[NtopTimestamp,SrcAddr,SrcPort,DstAddr,DstPort,InPkts,InBytes,OutPkts,OutBytes,FirstSwitched,LastSwitched,NtopngInstanceName]|Acc]};
      _-> 
        {error, Acc} end end, [], Data).

bin_to_record(Data)->
  case catch jiffy:decode(Data) of 
    {[{<<"ntop_timestamp">>,NtopTimestamp},
      {<<"IN_SRC_MAC">>,_}, 
      {<<"OUT_DST_MAC">>,_}, 
      {<<"IPV4_SRC_ADDR">>,SrcAddr}, 
      {<<"IPV4_DST_ADDR">>,DstAddr}, 
      {<<"L4_SRC_PORT">>,SrcPort}, 
      {<<"L4_DST_PORT">>,DstPort}, 
      {<<"PROTOCOL">>,_}, 
      {<<"L7_PROTO">>,_}, 
      {<<"L7_PROTO_NAME">>,_}, 
      {<<"IN_PKTS">>,InPkts}, 
      {<<"IN_BYTES">>,InBytes}, 
      {<<"OUT_PKTS">>,OutPkts}, 
      {<<"OUT_BYTES">>,OutBytes}, 
      {<<"FIRST_SWITCHED">>,FirstSwitched}, 
      {<<"LAST_SWITCHED">>,LastSwitched}, 
      {<<"SRC_IP_COUNTRY">>,_}, 
      {<<"SRC_IP_LOCATION">>,_}, 
      {<<"DST_IP_COUNTRY">>,_}, 
      {<<"DST_IP_LOCATION">>,_}, 
      {<<"NTOPNG_INSTANCE_NAME">>,NtopngInstanceName}, 
      {<<"INTERFACE">>,_}]}-> 
        {ok, #data{
                   ntoptimestamp=binary_to_list(NtopTimestamp),
                   srcaddr=binary_to_list(SrcAddr),
                   srcport=integer_to_list(SrcPort),
                   dstaddr=binary_to_list(DstAddr),
                   dstport=integer_to_list(DstPort),
                   inpkts=integer_to_list(InPkts),
                   inbytes=integer_to_list(InBytes),
                   outpkts=integer_to_list(OutPkts),
                   outbytes=integer_to_list(OutBytes),
                   firstswitched=integer_to_list(FirstSwitched),
                   lastswitched=integer_to_list(LastSwitched),
                   nin=binary_to_list(NtopngInstanceName)
                  }};
      Msg -> 
        lager:info("Error: ~p~n", [Msg]),
        error
    end.


%% ====================================================================
%% Listner
%% ====================================================================

listner_start(Port)-> 
  {ok,LSocket} = gen_tcp:listen(Port, ?OPTIONS), 
  {ok, Socket} = gen_tcp:accept(LSocket), 
  lager:info("connect\n"),
  listener_loop(Socket, Port).

listener_loop(Socket, Port)->
  receive 
    {tcp, _Socket, Packet}-> 
      inet:setopts(Socket, [{active,once}]), 
      gen_server:cast(?MODULE, {data,Packet}); 
    {tcp_closed, _Socket}-> 
      gen_tcp:close(Socket), 
      listner_start(Port);
    _Msg -> 
      lager:info("unknown msg: ~p~n", [_Msg]),
      error
  end,
  listener_loop(Socket, Port).

