-module(mysql_writer).
-behaviour(gen_fsm).

%% API
-export([start_link/0, stop/0]).
-export([add_data/1]).

%% gen_fsm callbacks
-export([init/1, 
         connected/2, 
         connected/3, 
         disconnected/2, 
         disconnected/3, 
         handle_event/3, 
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

-include("ntopng_mysql.hrl").

-record(state, {data=[], mysqlpid, timer, socket}).

-define(MYSQL_OPTIONS, [{name,{local,mysql}}, 
                        {host, "10.1.116.42"}, 
                        {user, "ntopng"}, 
                        {password, "qwerty1"}, 
                        {database, "ntopng"}]).
-define(RECONNECT_TIMEOUT, 1000).
-define(DATA_COLLECT_TIMEOUT, 100).
-define(BUFF_SIZE, 2000).
-define(MAX_STORE_SIZE, 300000).

-define(UNIX_EPOCH, 62167219200).
-define(DAY, 86400).

%% ====================================================================
%% API functions
%% ====================================================================

add_data(Data) -> 
  gen_fsm:send_event(?MODULE, {add, Data}).

start_link() ->
  gen_fsm:start_link({local,?MODULE},?MODULE,[],[]). 

stop() ->
  gen_fsm:send_all_state_event(?MODULE, stop).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([]) ->
  process_flag(trap_exit, true), 
  {ok, MySqlPid} = mysql:start_link(?MYSQL_OPTIONS), 
  {ok, connected, #state{mysqlpid=MySqlPid}}.


connected({add, RcvData}, #state{data=Data}=State) ->
  NewData = RcvData ++ Data,
  case length(NewData) >= ?BUFF_SIZE of
    true -> 
      {next_state, connected, State#state{data=NewData}, 0};
    _ ->
      {next_state, connected, State#state{data=NewData}, ?DATA_COLLECT_TIMEOUT}
  end;
connected(timeout, #state{data=[], timer=Timer}=State) ->
  stop_timer(Timer),
  {next_state, connected, State#state{timer=undefined}};
connected(timeout, #state{data=Data, timer=Timer}=State) ->
  NewTimer = start_timer(Timer),
  {TData,HData} = case length(Data) of 
    Len when Len >= ?BUFF_SIZE ->
      lists:split(Len-?BUFF_SIZE, Data);
    _Len -> 
      {[], Data}
  end,
  case send(HData) of
    ok ->
      {next_state, connected, State#state{data=TData, timer=NewTimer}, 0};
    {error, Data1} ->
      ?MODULE ! reconnect,
      {next_state, disconnected, State#state{data=Data1 ++ TData, timer=NewTimer}}
  end;
connected(Event, State) ->
  lager:info("Unknown connected event: ~p~n", [Event]),
  {next_state, connected, State, 0}.
  
connected(Event, From, State) ->
  lager:info("Unknown connected sync event ~p from ~p~n", [Event, From]),
  {replay, ok, connected, State, 0}.

disconnected({add, RcvData}, #state{data=Data}=State) ->
  NewData =  RcvData ++ Data,
  {_TData,HData} = case length(NewData) of
    Len when Len >= ?MAX_STORE_SIZE ->
      lager:info("The length of the lost data: ~p~n",[Len-?MAX_STORE_SIZE]),
      lists:split(Len-?MAX_STORE_SIZE, NewData);
    _Len ->
      {[], NewData}
  end,
  {next_state, disconnected, State#state{data=HData}};
disconnected(Event, State) ->
  lager:info("Unknown disconnected event: ~p~n", [Event]),
  {next_state, disconnected, State}.

disconnected(Event, From, State) ->
  lager:info("Unknown disconnected sync event ~p from ~p~n", [Event, From]),
  {replay, ok, disconnected, State}.

handle_event(stop, _StateName, _State) ->
  exit(normal);
handle_event(Event, StateName, State) ->
  lager:info("Unknown all_state event ~p current state ~p~n", [Event, StateName]),
  {next_state, StateName, State}.

handle_sync_event(Event, From, StateName, State) ->
  lager:info("Unknown all_state sync event ~p from ~p current state ~p~n", [Event, From, StateName]),
  {replay, ok, StateName, State}.

handle_info({'EXIT', MySqlPid, Reason}, _StateName, #state{mysqlpid=MySqlPid}=State) ->
  lager:info("MySQL closed with reason ~p~n",[Reason]),
  ?MODULE ! reconnect,
  {next_state, disconnected, State};
handle_info({'EXIT', Pid, Reason}, _StateName, State) ->
  lager:info("Process ~p closed with reason ~p~n",[Pid, Reason]),
  {next_state, disconnected, State};
handle_info(reconnect, disconnected, State) ->
  case mysql:start_link(?MYSQL_OPTIONS) of
    {ok, NewMySqlPid} ->
      lager:info("MySQL reconnected~n",[]),
      {next_state, connected, State#state{mysqlpid=NewMySqlPid}, 0};
    {error, Error} ->
      lager:info("MySQL not connected ~p~n",[Error]),
      erlang:send_after(?RECONNECT_TIMEOUT, ?MODULE, reconnect),
      {next_state, disconnected, State}
  end;
handle_info(Info, StateName, State) ->
  lager:info("Unknown info ~p current state ~p~n", [Info, StateName]),
  {next_state, StateName, State}.

terminate(normal, StateName, #state{mysqlpid=MySqlPid}) ->
  process_flag(trap_exit, false),
  exit(MySqlPid, normal), 
  lager:info("Terminate normal. Current state ~p~n", [StateName]),
  {normal};
terminate(Reason, StateName, _State) ->
  lager:info("Unknown terminate reason ~p current state ~p~n", [Reason, StateName]),
  {Reason}.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

send(Data) ->
  send1(lists:keysort(#packet.last_switched, Data)).

send1([]) ->
  ok;
send1([HData|_]=Data) ->
  BPeriod = localtime_start_day(HData#packet.last_switched), % Начало дня
  {InPeriod, OutPeriod} = lists:splitwith(fun(#packet{last_switched=LastSwitched})-> 
                                            (LastSwitched >= BPeriod) and (LastSwitched < BPeriod + ?DAY) end, Data),
  try mysql:query(mysql, create_mysql_query(BPeriod, InPeriod)) of
    ok ->
      lager:info("Writed data length: ~p~n", [length(InPeriod)]), 
      send1(OutPeriod)
  catch 
    exit:Error ->
      lager:info("MySQL INSERT ERROR: ~p~n", [Error]), 
      {error, Data}
  end.

create_mysql_query(BPeriod, Data) ->
  DataGroups = lists:map(fun(Packet)-> 
    #packet{
      ntop_timestamp=NtopTimestamp,
      src_addr=SrcAddr,
      src_port=SrcPort,
      dst_addr=DstAddr,
      dst_port=DstPort,
      in_pkts=InPkts,
      in_bytes=InBytes,
      out_pkts=OutPkts,
      out_bytes=OutBytes,
      first_switched=FirstSwitched,
      last_switched=LastSwitched,
      ntopng_instance_name=NtopngInstanceName,
      interface=Interface} = Packet, 
    string:join([
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
      binary_to_list(<<"'", NtopngInstanceName/binary, "'">>), 
      binary_to_list(<<"'", Interface/binary, "'">>)], ",") end, Data), 
  DataPart = string:join(DataGroups, "),("), 
  {{Year, Month, Day}, _Time} = timestamp_to_localtime(BPeriod), 
  TableName = lists:flatten(io_lib:format("flows_~4..0w~2..0w~2..0w", [Year, Month, Day])),
  "INSERT INTO " ++ TableName  ++ " (
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
    NTOPNG_INSTANCE_NAME,
    INTERFACE) VALUES (" ++ DataPart ++ ")".

% Time
start_timer(undefined)-> 
  lager:info("Data transfer started~n",[]), 
  erlang:now();
start_timer(Time)-> 
  Time.

stop_timer(undefined)-> 
  undefined;
stop_timer(Time)-> 
  lager:info("Data transfer took ~p seconds~n", [timer:now_diff(erlang:now(), Time) div 1000000]).

localtime_start_day(Timestamp) -> 
  {Date, _Time} = timestamp_to_localtime(Timestamp), 
  localtime_to_timestamp({Date, {0, 0, 0}}).

timestamp_to_localtime(Timestamp) -> 
  calendar:universal_time_to_local_time(calendar:gregorian_seconds_to_datetime(?UNIX_EPOCH + Timestamp)).

localtime_to_timestamp(Localtime) -> 
  [Utc] = calendar:local_time_to_universal_time_dst(Localtime),  
  calendar:datetime_to_gregorian_seconds(Utc) - ?UNIX_EPOCH.
