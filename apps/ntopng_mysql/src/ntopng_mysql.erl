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
                data = [], 
                mysqlpid
               }). 
%-record(data, {
%               ntoptimestamp, 
%               srcaddr, 
%               srcport, 
%               dstaddr, 
%               dstport, 
%               inpkts, 
%               inbytes, 
%               outpkts, 
%               outbytes, 
%               firstswitched, 
%               lastswitched,
%               nin
%              }).
-define(BUFF_SIZE, 2000). 
-define(PORT, 5510).
-define(TIMEOUT, 100).
-define(OPTIONS, [binary,{backlog, 128},{active,once},{buffer, 65536},{packet,0},{reuseaddr,true}]).

start_link()->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([]) ->
  % Подключение к MySQL
  MysqlSettings = [{host, "localhost"}, {user, "ntopng"}, {password, "qwerty1"}, {database, "ntopng"}], 
  {ok, MySqlPid} = mysql:start_link(MysqlSettings), 
  % Слушатель
  register(listener, spawn_link(?MODULE, listner_start, [?PORT])), 
  {ok, #state{mysqlpid=MySqlPid}}.

handle_cast({data,Packet}, #state{buffer=Buffer,data=Data,mysqlpid=MySqlPid}=State)-> 
  [NewBuffer|RcvData] = lists:reverse(binary:split(<<Buffer/binary, Packet/binary>>, <<"\n">>, [global])), 
  NewData = lists:append(RcvData, Data), 
  RestData = case length(NewData) of
    Len when Len >= ?BUFF_SIZE -> 
      {TData,HData} = lists:split(Len-?BUFF_SIZE,  NewData),
      send(HData, MySqlPid),
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

handle_info(timeout, #state{data=Data,mysqlpid=MySqlPid}=State)-> 
  send(Data,MySqlPid), 
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

send([],_MySqlPid)-> 
  lager:info("Empty data~n");
send(Data, MySqlPid)-> 
  lager:info("Send data length: ~p~n", [length(Data)]), 
  
  %lager:info("Data: ~n~p~n", [mysql_format(Data, [])]), 
  
  mysql_insert(MySqlPid, mysql_format(Data, [])).

extract(Data)-> 
  try 
    {Data1} = jiffy:decode(Data), 
    {_, NtopTimestamp} = lists:keyfind(<<"ntop_timestamp">>, 1, Data1), 
    {_, SrcAddr} = lists:keyfind(<<"IPV4_SRC_ADDR">>, 1, Data1),
    {_, SrcPort} = lists:keyfind(<<"L4_SRC_PORT">>, 1, Data1), 
    {_, DstAddr} = lists:keyfind(<<"IPV4_DST_ADDR">>, 1, Data1), 
    {_, DstPort} = lists:keyfind(<<"L4_DST_PORT">>, 1, Data1), 
    {_, InPkts} = lists:keyfind(<<"IN_PKTS">>, 1, Data1), 
    {_, InBytes} = lists:keyfind(<<"IN_BYTES">>, 1, Data1), 
    {_, OutPkts} = lists:keyfind(<<"OUT_PKTS">>, 1, Data1), 
    {_, OutBytes} = lists:keyfind(<<"OUT_BYTES">>, 1, Data1),  
    {_, FirstSwitched} = lists:keyfind(<<"FIRST_SWITCHED">>, 1, Data1),  
    {_, LastSwitched} = lists:keyfind(<<"LAST_SWITCHED">>, 1, Data1),  
    {_, NtopngInstanceName} = lists:keyfind(<<"NTOPNG_INSTANCE_NAME">>, 1, Data1), 
    {ok, [
      binary_to_list(<<"'", NtopTimestamp/binary, "'">>),
      binary_to_list(<<"INET_ATON('", SrcAddr/binary, "')">>), 
      integer_to_list(SrcPort), 
      binary_to_list(<<"INET_ATON('", DstAddr/binary, "')">>), 
      integer_to_list(DstPort), 
      integer_to_list(InPkts), 
      integer_to_list(InBytes), 
      integer_to_list(OutPkts), 
      integer_to_list(OutBytes), 
      integer_to_list(FirstSwitched), 
      integer_to_list(LastSwitched), 
      binary_to_list(<<"'", NtopngInstanceName/binary, "'">>)]}
  catch 
    error:Error-> 
      lager:info("~p ~p~n", [Error, erlang:get_stacktrace()]), 
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
  Query = "INSERT INTO flows (
    NTOP_TIMESTAMP, 
    IP_SRC_ADDR, 
    L4_SRC_PORT, 
    IP_DST_ADDR, 
    L4_DST_PORT, 
    IN_PACKETS, 
    IN_BYTES, 
    OUT_PACKETS, 
    OUT_BYTES, 
    FIRST_SWITCHED, 
    LAST_SWITCHED, 
    NTOPNG_INSTANCE_NAME)
    VALUES (",
    ok = mysql:query(MySqlPid, Query ++ Data ++ ")").

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

