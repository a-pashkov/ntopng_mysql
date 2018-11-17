-module(listener_udp).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

-include("ntopng_mysql.hrl").

-record(state, {socket, server_ip=[]}).

-define(UDP_PORT, 5510).
-define(UDP_OPTIONS, [binary, {active,true}, {recbuf,124928000}]).

start_link() ->
  gen_server:start_link({local,?MODULE},?MODULE,[],[]).

init([]) ->
  {ok,Socket} = gen_udp:open(?UDP_PORT, ?UDP_OPTIONS), 
  ServerIP = case application:get_env(server_ip) of 
    {ok, Value} -> 
      [list_to_binary(Ip) || Ip <- Value];
    _ -> []
  end,
  {ok, #state{socket=Socket, server_ip=ServerIP}}.

handle_cast(Msg, State) ->
  lager:info("handle_cast: ~p\n", [{Msg, State}]),
  {noreply, State}.

handle_call(Request, From, State) -> 
  lager:info("handle_call: ~p\n", [{Request, From, State}]),
  {reply, ok, State}.

handle_info({udp, _Socket, _Addr, _Port, Packet}, #state{server_ip=ServerIP}=State)-> 
  [_|RcvData] = lists:reverse(binary:split(Packet, <<"\n">>, [global])), 
  Data = lists:filtermap(fun(Data)-> extract(Data, ServerIP) end, RcvData),
  mysql_writer:add_data(Data),
  {noreply, State};
handle_info(Info, State) ->
  lager:info("handle_info: ~p\n", [{Info, State}]),
  {noreply, State}.

terminate(normal, #state{socket=Socket}) -> 
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
extract(Data, ServerIP)->
  try
    {Data1} = jiffy:decode(Data), 
    {_, SrcAddr} = lists:keyfind(<<"IPV4_SRC_ADDR">>, 1, Data1), 
    {_, SrcPort} = lists:keyfind(<<"L4_SRC_PORT">>, 1, Data1),   
    {_, DstAddr} = lists:keyfind(<<"IPV4_DST_ADDR">>, 1, Data1),
    {_, DstPort} = lists:keyfind(<<"L4_DST_PORT">>, 1, Data1),
    {_, InBytes} = lists:keyfind(<<"IN_BYTES">>, 1, Data1),
    {_, OutBytes} = lists:keyfind(<<"OUT_BYTES">>, 1, Data1),
    {_, InPkts} = lists:keyfind(<<"IN_PKTS">>, 1, Data1),
    {_, OutPkts} = lists:keyfind(<<"OUT_PKTS">>, 1, Data1),
    {_, LastSwitched} = lists:keyfind(<<"LAST_SWITCHED">>, 1, Data1),
    case lists:member(SrcAddr, ServerIP) of 
      false -> 
        {true, #packet{ 
        src_addr=SrcAddr,
        src_port=SrcPort,
        dst_addr=DstAddr,
        dst_port=DstPort,
        in_pkts=InPkts,
        in_bytes=InBytes,
        out_pkts=OutPkts, 
        out_bytes=OutBytes,
        last_switched=LastSwitched}};
      _ -> 
        {true, #packet{ 
        src_addr=DstAddr,
        src_port=DstPort,
        dst_addr=SrcAddr,
        dst_port=SrcPort,
        in_pkts=OutPkts,
        in_bytes=OutBytes,
        out_pkts=InPkts, 
        out_bytes=InBytes,
        last_switched=LastSwitched,
        turn=true}}
    end
  catch
    error:Error->
      lager:info("~p ~p~nData:~n~p~n", [Error, erlang:get_stacktrace(), Data]),
      error
  end.
