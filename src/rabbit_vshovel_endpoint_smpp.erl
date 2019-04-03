%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Variable Shovel.
%%
%% The Developer of this component is Erlang Solutions, Ltd.
%% Copyright (c) 2016-2017 84codes.  All rights reserved.
%%

-module(rabbit_vshovel_endpoint_smpp).
-behaviour(rabbit_vshovel_endpoint).
-behaviour(esmpp_lib_worker).

-export([init/2,
         validate_address/1,
         validate_arguments/1,
         handle_broker_message/2,
         terminate/1]).

-include("rabbit_vshovel.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% -----------
%% Definitions
%% -----------

-record(smpp_state, {
  mode,
  host,
  port,
  system_id,
  password,
  system_type,
  callback_dr,
  source_queue,
  source_channel,
  worker_pid
}).

-define(REQUESTING_ENTITY, "RabbitMQ-vShovel").
-define(VERSION, ?VSHOVEL_VERSION).
-define(ACCEPT, "*/*").

%% -------------------
%% Behaviour callbacks
%% -------------------
validate_arguments(Args) when is_list(Args) ->
  {ok, validate_arguments([{?TO_ATOM(T), V} || {T, V} <- Args], [])};
validate_arguments(Args) -> {error, Args}.

validate_address(Address) when is_list(Address); is_binary(Address) ->
  {ok, Address};
validate_address(Other) -> {error, Other}.

init(Ch, _VState = #vshovel{queue        = QueueName,
                            destinations = #endpoint{address   = DestAddresses,
                                                     arguments = Args}}) ->

  State = parse_smpp_options(Args, #smpp_state{}),
  {ok, SMPPPid} = esmpp:start_link(#{mode => State#smpp_state.mode,
                                     host => State#smpp_state.host,
                                     port => State#smpp_state.port,
                                     system_id => State#smpp_state.system_id,
                                     password => State#smpp_state.password,
                                     system_type => State#smpp_state.system_type,
                                     callback_dr => State#smpp_state.callback_dr}),

  {ok, State#smpp_state{worker_pid     = SMPPPid,
                        source_queue   = QueueName,
                        source_channel = Ch}}.

handle_broker_message({#'basic.deliver'{exchange     = Exchange,
                                        routing_key  = RoutingKey,
                                        delivery_tag = DeliveryTag},
                       #amqp_msg{
                         props   = #'P_basic'{},
                         payload = Payload}},
                      SmppState = #smpp_state{
                        worker_pid     = Pid,
                        source_channel = Channel}) ->

  Request = {Payload},
  worker_pool:submit_async(
    fun() ->
      send(Request, Pid, Channel, DeliveryTag)
    end),
  {ok, SmppState}.

terminate(_SmppState) ->
  ok.

%% -------
%% Private
%% -------
send(Request, Pid, Channel, DeliveryTag) ->
  case catch esmpp_lib_worker:submit(Pid, [{source_addr, <<"Test">>}, {dest_addr, <<"380555222333">>}, {text, <<"Test sms">>}]) of
    ok ->
      case handle_response(ok, Channel, DeliveryTag) of
        ok -> {ok, ok};
        Error ->
          rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, Error),
          Error
      end;
    Error ->
      rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, Error),
      Error
  end.

handle_response(ok, Channel, DeliveryTag) ->
  %% Issue 'basic.ack' back to the broker.
  amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
  rabbit_event:notify(vshovel_result, [{source, ?MODULE}, {result, ok}]),
  ok;

handle_response(_, Channel, DeliveryTag) ->
  %% Issue 'basic.nack' back to the broker,
  %% indicating message delivery failure on HTTP endpoint.
  amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = DeliveryTag}),
  rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, fail),
  ok.

validate_arguments([], Acc)                 -> Acc;
validate_arguments([H = {_, _} | Rem], Acc) -> validate_arguments(Rem, [H | Acc]);
validate_arguments([_ | Rem], Acc)          -> validate_arguments(Rem, Acc).

parse_smpp_options([{mode, V} | T], State)        -> parse_smpp_options(T, State#smpp_state{mode = V});
parse_smpp_options([{host, V} | T], State)        -> parse_smpp_options(T, State#smpp_state{host = V});
parse_smpp_options([{port, V} | T], State)        -> parse_smpp_options(T, State#smpp_state{port = V});
parse_smpp_options([{system_id, V} | T], State)   -> parse_smpp_options(T, State#smpp_state{system_id = V});
parse_smpp_options([{password, V} | T], State)    -> parse_smpp_options(T, State#smpp_state{password = V});
parse_smpp_options([{system_type, V} | T], State) -> parse_smpp_options(T, State#smpp_state{system_type = V});
parse_smpp_options([_ | T], State)                -> parse_smpp_options(T, State);
parse_smpp_options([], State)                     -> State.