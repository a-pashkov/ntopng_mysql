-module(ntopng_mysql).
-behaviour(gen_server).

-compile(export_all).

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-record(state, {data=[], mysqlpid, timer, socket}).

-define(BUFF_SIZE, 2000). 
-define(UDP_PORT, 5510).
-define(TIMEOUT, 100).
-define(MYSQL_OPTIONS, [{host, "10.1.116.42"}, {user, "ntopng"}, {password, "qwerty1"}, {database, "ntopng"}]).
-define(UDP_OPTIONS, [binary, {active,true}, {recbuf,124928000}]).

start_link() ->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([]) ->
  {ok, MySqlPid} = mysql:start_link(?MYSQL_OPTIONS), 
  {ok,Socket} = gen_udp:open(?UDP_PORT, ?UDP_OPTIONS), 
  %controlling_process(Socket, self()),
  {ok, #state{mysqlpid=MySqlPid, socket=Socket}}.

handle_cast(Msg, State) ->
  lager:info("handle_cast: ~p\n", [{Msg, State}]),
  {noreply, State, 0}.

handle_call(Request, From, State) -> 
  lager:info("handle_call: ~p\n", [{Request, From, State}]),
  {reply, ok, State, 0}.

handle_info({udp, _Socket, _Addr, _Port, Packet}, #state{data=Data, mysqlpid=MySqlPid, timer=Timer}=State)-> 
  [_|RcvData] = lists:reverse(binary:split(Packet, <<"\n">>, [global])), 
  NewData = lists:append(RcvData, Data), 
  RestData = case length(NewData) of
    Len when Len >= ?BUFF_SIZE -> 
      NewTimer = start_timer(Timer), 
      {TData,HData} = lists:split(Len-?BUFF_SIZE,  NewData),
      send(HData, MySqlPid),
      TData;
    _Len -> 
      NewTimer = Timer, 
      NewData
  end,
  {noreply, State#state{data=RestData, timer=NewTimer}, ?TIMEOUT};
handle_info(timeout, #state{data=Data, mysqlpid=MySqlPid, timer=undefined}=State) -> 
  send(Data, MySqlPid), 
  {noreply, State#state{data=[]}};
handle_info(timeout, #state{data=Data, mysqlpid=MySqlPid, timer=Timer}=State) -> 
  send(Data, MySqlPid), 
  stop_timer(Timer),
  {noreply, State#state{data=[], timer=undefined}};
handle_info(Info, State) ->
  lager:info("handle_info: ~p\n", [{Info, State}]),
  {noreply, State, 0}.

terminate(normal, #state{data=Data, mysqlpid=MySqlPid, socket=Socket}) -> 
  send(Data, MySqlPid), 
  gen_udp:close(Socket), 
  ok;
terminate(Reason, State) -> 
  lager:info("terminate: ~p~n", [{Reason, State}]), %% Ненормальное завершение
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
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

send([],_MySqlPid)->
  lager:info("Empty data~n");
send(Data, MySqlPid)->
  lager:info("Send data length: ~p~n", [length(Data)]),
  %lager:info("Data: ~n~p~n", [mysql_format(Data, [])]),
  mysql_insert(MySqlPid, mysql_format(Data, [])).

start_timer(undefined)-> 
  lager:info("Data transfer started~n",[]), 
  erlang:now();
start_timer(Time)-> 
  Time.

stop_timer(undefined)-> 
  undefined;
stop_timer(Time)-> 
  lager:info("Data transfer took ~p seconds~n", [timer:now_diff(erlang:now(), Time) div 1000000]).

