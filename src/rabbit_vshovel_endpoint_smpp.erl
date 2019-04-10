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

%% ------------------------------------------
%% Vshovel endpoint Callback Function Exports
%% ------------------------------------------

-export([init/2,
         validate_address/1,
         validate_arguments/1,
         handle_broker_message/2,
         terminate/1]).

%% -------------------------------
%% ESMPP Callback Function Exports
%% -------------------------------

-export([submit_sm_resp_handler/2, deliver_sm_handler/2, data_sm_handler/2,
         data_sm_resp_handler/2, query_sm_resp_handler/2, unbind_handler/1,
         outbind_handler/2, submit_error/2, network_error/2, decoder_error/2,
         sequence_number_handler/1]).

-import(rabbit_misc, [pget/2, pget/3]).

-include("rabbit_vshovel.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("esmpp_lib/include/esmpp_lib.hrl").

%% -----------
%% Definitions
%% -----------

-record(smpp_state, {channel,
                     source_queue,
                     worker_pid,
                     args}).

-define(REQUESTING_ENTITY, "RabbitMQ-vShovel").
-define(VERSION, ?VSHOVEL_VERSION).
-define(ACCEPT, "*/*").

%% ------------------------------------
%% Vshovel Endpoint Behaviour callbacks
%% ------------------------------------

validate_arguments(Args) when is_list(Args) ->
  {ok, validate_arguments([{?TO_ATOM(T), V} || {T, V} <- Args], [])};
validate_arguments(Args) -> {error, Args}.

validate_address(Address) when is_list(Address); is_binary(Address) ->
  {ok, Address};
validate_address(Other) -> {error, Other}.

init(Ch, _VState = #vshovel{queue        = QueueName,
                            destinations = #endpoint{arguments = Args}}) ->
  ets:new(dtag_to_seq_nrs, [named_table, public]),
  ets:new(seq_nr_to_dtag, [named_table, public]),
  ets:new(msg_id_to_dtag, [named_table, public]),
  ets:new(smpp_state, [named_table, public]),
  ets:insert(smpp_state, [{channel, Ch}, {ppid, self()}]),
  SMPPArguments = parse_smpp_options(Args, []),
  {ok, SMPPPid} = esmpp_lib_worker:start_link(SMPPArguments),
  {ok, #smpp_state{channel      = Ch,
                   source_queue = QueueName,
                   worker_pid   = SMPPPid,
                   args         = SMPPArguments}}.


handle_broker_message({#'basic.deliver'{delivery_tag = DeliveryTag},
                       #amqp_msg{props   = #'P_basic'{headers = Headers},
                                 payload = Payload}},
                      SmppState = #smpp_state{worker_pid = Pid,
                                              args       = SMPPArguments}) ->
  PHeaders = parse_headers(Headers),
  worker_pool:submit_async(
    fun() ->
      send_smpp(Payload, PHeaders, Pid, SMPPArguments, DeliveryTag)
    end),
  {ok, SmppState};

handle_broker_message({deliver_sm, DeliverSmArgs}, SmppState = #smpp_state{channel = Ch,
                                                                           args    = SMPPArguments}) ->
  worker_pool:submit_async(
    fun() ->
      send_amqp(DeliverSmArgs, Ch, SMPPArguments)
    end),
  {ok, SmppState}.

terminate(#smpp_state{worker_pid = SMPPPid}) ->
  esmpp_lib_worker:unbind(SMPPPid),
  ok.

%% ------------------------------------
%% ESMPP Endpoint Behaviour callbacks
%% ------------------------------------

sequence_number_handler(List) ->
  DTag = pget(delivery_tag, List),
  SeqNr = pget(sequence_number, List),
  case ets:lookup(dtag_to_seq_nrs, DTag) of
    [{DTag, SeqNrList}] -> ets:insert(dtag_to_seq_nrs, {DTag, [SeqNr | SeqNrList]});
    [] -> ets:insert(dtag_to_seq_nrs, {DTag, [SeqNr]})
  end,
  ets:insert(seq_nr_to_dtag, {SeqNr, DTag}),
  ok.

submit_sm_resp_handler(_Pid, List) ->
  SeqNr = pget(sequence_number, List),
  MsgId = pget(message_id, List),
  {ok, DTag} = handle_response(ok, SeqNr),
  ets:insert(msg_id_to_dtag, {MsgId, DTag}),
  ok.

deliver_sm_handler(_Pid, List) ->
  MsgId = pget(receipted_message_id, List),
  NewList = case ets:lookup(msg_id_to_dtag, MsgId) of
              [{MsgId, DTag}] -> [{delivery_tag, DTag} | List];
              [] -> List
            end,
  [{ppid, PPid}] = ets:lookup(smpp_state, ppid),
  PPid ! {deliver_sm, NewList},
  ok.

data_sm_handler(Pid, List) ->
  ?LOG_INFO("Data_sm pid ~p msg: ~p~n", [Pid, List]).

data_sm_resp_handler(Pid, List) ->
  ?LOG_INFO("Data_sm resp pid ~p msg: ~p~n", [Pid, List]).

query_sm_resp_handler(Pid, List) ->
  ?LOG_INFO("Query resp pid ~p msg: ~p~n", [Pid, List]).

unbind_handler(Pid) ->
  ?LOG_INFO("Link unbind ~p~n", [Pid]).

outbind_handler(Pid, Socket) ->
  ?LOG_INFO("Link pid ~p outbind ~p~n", [Pid, Socket]).

submit_error(_Pid, SeqNr) ->
  {ok, _DTag} = handle_response(fail, SeqNr),
  ok.

network_error(Pid, Error) ->
  ?LOG_INFO("Pid ~p return error tcp ~p~n", [Pid, Error]).

decoder_error(Pid, Error) ->
  ?LOG_INFO("Pid ~p return decoder error ~p~n", [Pid, Error]).

%% -------
%% Private
%% -------


send_smpp(Msg, Headers, Pid, Config, DeliveryTag) ->
  SourceAddr = get_header(source_addr, Config, Headers),
  DestAddr = get_header(dest_addr, Config, Headers),
  case catch esmpp_lib_worker:submit(Pid, [{source_addr, SourceAddr},
                                           {dest_addr, DestAddr},
                                           {text, Msg},
                                           {delivery_tag, DeliveryTag}]) of
    ok -> {ok, 0};
    Error ->
      rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, Error),
      Error
  end.

send_amqp(Args, Channel, Config) ->
  Exch = pget(deliver_sm_exchange, Config),
  Queue = pget(deliver_sm_queue, Config),
  Msg = pget(short_message, Args),
  DTag = pget(delivery_tag, Args),
  ESMClass = pget(esm_class, Args),
  Status = pget(command_status, Args),
  SrcAddr = pget(source_addr, Args),
  DestAddr = pget(destination_addr, Args),
  amqp_channel:cast(Channel,
                    #'basic.publish'{exchange    = Exch,
                                     routing_key = Queue},
                    #amqp_msg{payload = Msg,
                              props   = #'P_basic'{headers = [{<<"original_delivery_tag">>, long, DTag},
                                                              {<<"esm_class">>, long, ESMClass},
                                                              {<<"command_status">>, long, Status},
                                                              {<<"source_addr">>, longstr, SrcAddr},
                                                              {<<"dest_addr">>, longstr, DestAddr}]}}).

handle_response(Status, SeqNr) ->
  case ets:lookup(seq_nr_to_dtag, SeqNr) of
    [{SeqNr, DTag}] ->
      handle_response(Status, SeqNr, DTag);
    [] -> {ok, undefined}
  end.

handle_response(ok, SeqNr, DTag) ->
  %% Issue 'basic.ack' back to the broker.
  case ets:lookup(dtag_to_seq_nrs, DTag) of
    [{DTag, SeqNrList}] ->
      case NewSeqList = SeqNrList -- [SeqNr] of
        [] ->
          [{channel, Ch}] = ets:lookup(smpp_state, channel),
          amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
          ets:delete(dtag_to_seq_nrs, DTag),
          rabbit_event:notify(vshovel_result, [{source, ?MODULE}, {result, [{status, ok},
                                                                            {seq_nr, SeqNr},
                                                                            {dtag_nr, DTag}]}]),
          {ok, DTag};
        L when is_list(L) ->
          ets:insert(dtag_to_seq_nrs, {DTag, NewSeqList}),
          {ok, DTag}
      end;
    [] -> {ok, DTag}
  end;

handle_response(fail, SeqNr, DTag) ->
  %% Issue 'basic.nack' back to the broker,
  %% indicating message delivery failure on SMPP endpoint.
  case ets:lookup(dtag_to_seq_nrs, DTag) of
    [{DTag, _SeqNrList}] ->
      [{channel, Ch}] = ets:lookup(smpp_state, channel),
      amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag}),
      ets:delete(dtag_to_seq_nrs, {DTag, []}),
      rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, [{status, fail},
                                                             {seq_nr, SeqNr},
                                                             {dtag_nr, DTag}]),
      {ok, DTag};
    _ ->
      rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, [{status, fail},
                                                             {seq_nr, SeqNr},
                                                             {dtag_nr, DTag}]),
      {ok, DTag}
  end;


handle_response(_, SeqNr, DTag) ->
  rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, [{status, undefined},
                                                         {seq_nr, SeqNr},
                                                         {dtag_nr, DTag}]),
  ok.

validate_arguments([], Acc)                 -> Acc;
validate_arguments([H = {_, _} | Rem], Acc) -> validate_arguments(Rem, [H | Acc]);
validate_arguments([_ | Rem], Acc)          -> validate_arguments(Rem, Acc).

parse_smpp_options([], Acc)                 -> Acc;
parse_smpp_options([H = {_, _} | Rem], Acc) -> parse_smpp_options(Rem, [H | Acc]);
parse_smpp_options([_ | Rem], Acc)          -> parse_smpp_options(Rem, Acc).


parse_headers(Headers) when is_list(Headers) ->
  lists:foldl(fun(Header, Acc) ->
    case Header of
      {Key, _, Value} ->
        [{Key, Value} | Acc];
      _ -> Acc
    end
              end, [], Headers);
parse_headers(_Headers) -> [].

get_header(K, Defaults, Specifics) ->
  Default = pget(K, Defaults),
  pget(atom_to_binary(K, utf8), Specifics, Default).

