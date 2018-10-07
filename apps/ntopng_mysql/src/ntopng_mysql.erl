-module(ntopng_mysql).
-behaviour(gen_server).

-compile(export_all).

-export([start_link/0]).
-export([wait_connection/1]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-record(socketstate, {socket, buffer = <<"">>, data = [], timer}).
-record(state, {socketstates=[], mysqlpid}).

-define(BUFF_SIZE, 2000). 
-define(TCP_PORT, 5510).
-define(TIMEOUT, 100).
-define(MYSQL_OPTIONS, [{host, "10.1.116.42"}, {user, "ntopng"}, {password, "qwerty1"}, {database, "ntopng"}]).
-define(TCP_OPTIONS, [binary,{backlog, 128},{active,once},{buffer, 65536},{packet,0},{reuseaddr,true}]).

start_link()->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([]) ->
  {ok, MySqlPid} = mysql:start_link(?MYSQL_OPTIONS), 
  {ok,LSocket} = gen_tcp:listen(?TCP_PORT, ?TCP_OPTIONS), 
  spawn_link(?MODULE, wait_connection, [LSocket]), 
  {ok, #state{mysqlpid=MySqlPid}}.

handle_cast({data,Socket,Packet}, #state{socketstates=SocketStates,mysqlpid=MySqlPid}=State)-> 
  % Ищем сокет
  NewSocketStates = case lists:keyfind(Socket, #socketstate.socket, SocketStates) of 
    % Сокета нет (создаём новый и процессим данные)
    false->
      [packet_proc(#socketstate{socket=Socket}, Packet, MySqlPid)|SocketStates]; 
    % Сокет есть (процессим данные)
    SocketState-> 
      lists:keyreplace(Socket, #socketstate.socket, SocketStates, packet_proc(SocketState, Packet, MySqlPid)) 
  end, 
  {noreply, State#state{socketstates=NewSocketStates}};
handle_cast({closesocket, Socket}, #state{socketstates=SocketStates}=State)-> 
  {noreply, State#state{socketstates=lists:keydelete(Socket, #socketstate.socket, SocketStates)}};
handle_cast(Msg, State) ->
  lager:info("handle_cast: ~p\n", [{Msg, State}]),
  {noreply, State}.

handle_call(Request, From, State) -> 
  lager:info("handle_call: ~p\n", [{Request, From, State}]),
  {reply, ok, State}.

handle_info({send,Socket}, #state{socketstates=SocketStates,mysqlpid=MySqlPid}=State)-> 
  %lager:info("timer~n"), %%%
  NewSocketStates = case lists:keyfind(Socket, #socketstate.socket, SocketStates) of 
    % Сокета нет, оставляем как есть
    false->
      SocketStates;
    % Сокет есть - отправляем данные
    #socketstate{data=Data}=SocketState-> 
      send(Data, MySqlPid), 
      lists:keyreplace(Socket, #socketstate.socket, SocketStates, SocketState#socketstate{data=[]}) 
  end,
  {noreply, State#state{socketstates=NewSocketStates}};
handle_info(Info, State) ->
  lager:info("handle_info: ~p\n", [{Info, State}]),
  {noreply, State}.

terminate(normal, _State) -> 
  % Закрыть MySQL
  % Закрыть сокет  
  %  gen_udp:close(Socket), 
  ok;
terminate(Reason, State) -> 
  lager:info("terminate: ~p~n", [{Reason, State}]), %% Ненормальное завершение
 %  gen_tcp:close(Socket),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
packet_proc(#socketstate{socket=Socket,buffer=Buffer,data=Data,timer=Timer}=SocketState, Packet, MySqlPid)->
  % Таймер
  NewTimer = case Timer of 
    undefined-> 
      %lager:info("newtimer~n"), %%%
      erlang:send_after(?TIMEOUT, ?MODULE, {send,Socket});
    Timer-> 
      %lager:info("retimer ~p~n", [Timer]), %%%
      erlang:cancel_timer(Timer), 
      erlang:send_after(?TIMEOUT, ?MODULE, {send,Socket})
    end, 
    %lager:info("newtimer ~p~n", [NewTimer]), %%% 

  % Обработка данных
  [NewBuffer|RcvData] = lists:reverse(binary:split(<<Buffer/binary, Packet/binary>>, <<"\n">>, [global])), 
  NewData = lists:append(RcvData, Data), 
  RestData = case length(NewData) of
    Len when Len >= ?BUFF_SIZE -> 
      {TData,HData} = lists:split(Len-?BUFF_SIZE,  NewData),
      send(HData, MySqlPid),
      TData;
    _Len -> NewData
  end,
  SocketState#socketstate{buffer=NewBuffer, data=RestData, timer=NewTimer}.

send([],_MySqlPid)-> 
  lager:info("Empty data~n");
send(Data, MySqlPid)-> 
  lager:info("Send data length: ~p~n", [length(Data)]), 
  
  %lager:info("Data: ~n~p~n", [mysql_format(Data, [])]), 
  
  mysql_insert(MySqlPid, mysql_format(Data, [])).

extract(Data)->
  try
    {Data1} = jiffy:decode(Data), 
    L7Proto = case lists:keyfind(<<"L7_PROTO">>, 1, Data1) of 
      {_, L7Proto1} -> integer_to_list(L7Proto1);
      false ->  "NULL" end, 
    {_, SrcAddr} = lists:keyfind(<<"IPV4_SRC_ADDR">>, 1, Data1), 
    {_, SrcPort} = lists:keyfind(<<"L4_SRC_PORT">>, 1, Data1),   
    {_, DstAddr} = lists:keyfind(<<"IPV4_DST_ADDR">>, 1, Data1),
    {_, DstPort} = lists:keyfind(<<"L4_DST_PORT">>, 1, Data1),
    {_, Protocol} = lists:keyfind(<<"PROTOCOL">>, 1, Data1), 
    {_, InBytes} = lists:keyfind(<<"IN_BYTES">>, 1, Data1),
    {_, OutBytes} = lists:keyfind(<<"OUT_BYTES">>, 1, Data1),
    {_, InPkts} = lists:keyfind(<<"IN_PKTS">>, 1, Data1),
    {_, OutPkts} = lists:keyfind(<<"OUT_PKTS">>, 1, Data1),
    {_, FirstSwitched} = lists:keyfind(<<"FIRST_SWITCHED">>, 1, Data1),
    {_, LastSwitched} = lists:keyfind(<<"LAST_SWITCHED">>, 1, Data1),
    {_, NtopngInstanceName} = lists:keyfind(<<"NTOPNG_INSTANCE_NAME">>, 1, Data1),
    {ok, [
      L7Proto, 
      binary_to_list(<<"INET_ATON('", SrcAddr/binary, "')">>),
      integer_to_list(SrcPort),
      binary_to_list(<<"INET_ATON('", DstAddr/binary, "')">>),
      integer_to_list(DstPort),
      integer_to_list(Protocol), 
      integer_to_list(InBytes),
      integer_to_list(OutBytes),
      integer_to_list(InPkts + OutPkts),
      integer_to_list(FirstSwitched),
      integer_to_list(LastSwitched),
      binary_to_list(<<"'", NtopngInstanceName/binary, "'">>)]} 
  catch
    error:Error->
      lager:info("~p ~p~nData:~n~p~n", [Error, erlang:get_stacktrace(), Data]),
      error
  end.

mysql_format([], Acc)->
    string:join(Acc, "),(");
mysql_format([HData|TData], Acc)->
  case extract(HData) of
    {ok, Rec}->
      mysql_format(TData, [string:join(Rec, ",")|Acc]);
    _Else->
      mysql_format(TData, Acc)
  end.

mysql_insert(MySqlPid, Data)-> 
  %lager:info("~p~n", [Data]),
  Query = "INSERT INTO flowsv4 (
    L7_PROTO, 
    IP_SRC_ADDR, 
    L4_SRC_PORT, 
    IP_DST_ADDR, 
    L4_DST_PORT, 
    PROTOCOL, 
    IN_BYTES, 
    OUT_BYTES, 
    PACKETS, 
    FIRST_SWITCHED, 
    LAST_SWITCHED, 
    NTOPNG_INSTANCE_NAME) 
    VALUES (",
    ok = mysql:query(MySqlPid, Query ++ Data ++ ")").

%% ====================================================================
%% Listner
%% ====================================================================
wait_connection(LSocket)->
  {ok, Socket} = gen_tcp:accept(LSocket),
  lager:info("Accept: ~p~n", [Socket]),  
  Pid = spawn(?MODULE, get_request, [Socket]), 
  gen_tcp:controlling_process(Socket, Pid), 
  wait_connection(LSocket).
  
get_request(Socket)-> 
  receive 
    {tcp, Socket, Packet}-> 
      inet:setopts(Socket, [{active,once}]), 
      gen_server:cast(?MODULE, {data, Socket, Packet}); 
    {tcp_closed, Socket}-> 
      lager:info("Exit~n"), 
      gen_tcp:close(Socket), 
      gen_server:cast(?MODULE, {closesocket, Socket}), 
      exit(normal);
    _Msg -> 
      lager:info("unknown msg: ~p~n", [_Msg])
  end,
  get_request(Socket).

