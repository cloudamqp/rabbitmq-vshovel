%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vshovel_worker).
-behaviour(gen_server2).
-behaviour(rabbit_vshovel_endpoint).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([init/2, validate_address/1, validate_arguments/1,
         handle_broker_message/2, terminate/1]).

-export([make_conn_and_chan/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_vshovel.hrl").

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

-record(state, {inbound_conn, inbound_ch, name, type, config, inbound_uri,
                unacked,
                remaining, %% [1]
                remaining_unacked}). %% [2]

%% [1] Counts down until we shut down in all modes
%% [2] Counts down until we stop publishing in on-confirm mode

-record(amqp_state, {inbound_ch,
                     outbound_conn,
                     outbound_ch,
                     outbound_uri}).

start_link(Type, Name, Config) ->
  gen_server2:start_link(?MODULE, [Type, Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------
init([Type, Name, Config]) ->
  gen_server2:cast(self(), init),
  {ok, VShovel} = parse(Type, Name, Config),
  ok = rabbit_vshovel_status:report(Name, Type, starting),
  {ok, #state{name = Name, type = Type, config = VShovel}}.

parse(static, Name, Config)  -> rabbit_vshovel_config:parse(Name, Config);
parse(dynamic, Name, Config) -> rabbit_vshovel_parameters:parse(Name, Config).

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(init, State = #state{config = VShovel0 =
                                          #vshovel{sources        = Sources,
                                                   destinations   =
                                                   #endpoint{protocol = Protocol},
                                                   prefetch_count = Prefetch,
                                                   queue          = Queue,
                                                   ack_mode       = AckMode},
                                 name   = Name}) ->

  {InboundConn, InboundChan, InboundURI} =
  make_conn_and_chan(Sources#endpoint.address),

  Mod = rabbit_vshovel_config:get_endpoint_module(Protocol),
  VShovel = VShovel0#vshovel{name = Name, dest_mod = Mod},

  {ok, DestState} = Mod:init(InboundChan, VShovel),

  %% Don't trap exits until we have established connections so that
  %% if we try to shut down while waiting for a connection to be
  %% established then we don't block
  process_flag(trap_exit, true),

  (Sources#endpoint.resource_declaration)(InboundConn, InboundChan),

  NoAck = (AckMode =:= no_ack),
  if NoAck -> ok;
    true -> #'basic.qos_ok'{} =
            amqp_channel:call(
              InboundChan, #'basic.qos'{prefetch_count = Prefetch})
  end,

  Remaining = remaining(InboundChan, VShovel),
  case Remaining of
    0 -> exit({shutdown, autodelete});
    _ -> ok
  end,

  #'basic.consume_ok'{} =
  amqp_channel:subscribe(
    InboundChan, #'basic.consume'{queue  = Queue,
                                  no_ack = NoAck},
    self()),

  State1 =
  State#state{inbound_conn      = InboundConn,
              inbound_ch        = InboundChan,
              inbound_uri       = InboundURI,
              remaining         = Remaining,
              remaining_unacked = Remaining,
              unacked           = gb_trees:empty(),
              config            = VShovel#vshovel{dest_state = DestState}},
  ok = report_running(State1),
  {noreply, State1}.

%% TODO move amqp spicific messages into amqp callback

handle_info(#'basic.consume_ok'{}, State) ->
  {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              exchange     = Exchange, routing_key = RoutingKey},
             Msg = #amqp_msg{props = Props = #'P_basic'{}}},
            State = #state{inbound_uri = InboundURI,
                           config      = #vshovel{destinations       =
                                                  #endpoint{protocol = ?AMQP_PROTOCOL},
                                                  publish_properties = PropsFun,
                                                  publish_fields     = FieldsFun,
                                                  dest_state         = DestState}}) ->
  #amqp_state{outbound_uri = OutboundURI} = DestState,
  Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
  Method1 = FieldsFun(InboundURI, OutboundURI, Method),
  Msg1 = Msg#amqp_msg{props = PropsFun(InboundURI, OutboundURI, Props)},
  {noreply, publish(Tag, Method1, Msg1, State)};
handle_info(Delivery = {#'basic.deliver'{}, #amqp_msg{}},
            State = #state{config = VShovel = #vshovel{dest_mod   = Mod,
                                                       dest_state = DestState}}) ->
  {ok, DestState0} = Mod:handle_broker_message(Delivery, DestState),
  {noreply, State#state{config = VShovel#vshovel{dest_state = DestState0}}};

handle_info(#'basic.ack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #vshovel{destinations =
                                             #endpoint{protocol = ?AMQP_PROTOCOL},
                                             ack_mode     = on_confirm}}) ->
  {noreply, confirm_to_inbound(
    fun(DTag, Multi) ->
      #'basic.ack'{delivery_tag = DTag, multiple = Multi}
    end, Seq, Multiple, State)};

handle_info(#'basic.nack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{config = #vshovel{destinations =
                                             #endpoint{protocol = ?AMQP_PROTOCOL},
                                             ack_mode     = on_confirm}}) ->
  {noreply, confirm_to_inbound(
    fun(DTag, Multi) ->
      #'basic.nack'{delivery_tag = DTag, multiple = Multi}
    end, Seq, Multiple, State)};

%% TODO make compatible as default

handle_info(Msg,
            State = #state{config = VShovel = #vshovel{dest_mod   = Mod,
                                                       dest_state = DestState}}) ->
  {ok, DestState0} = Mod:handle_broker_message(Msg, DestState),
  {noreply, State#state{config = VShovel#vshovel{dest_state = DestState0}}};

handle_info(#'basic.cancel'{},
            State = #state{name   = Name,
                           config = #vshovel{destinations =
                                             #endpoint{protocol = ?AMQP_PROTOCOL},
                                             ack_mode     = on_confirm}}) ->
  rabbit_log:warning("vShovel ~p received 'basic.cancel' from the broker~n",
                     [Name]),
  {stop, {shutdown, restart}, State};

handle_info({'EXIT', InboundConn, Reason},
            State = #state{inbound_conn = InboundConn}) ->
  {stop, {inbound_conn_died, Reason}, State};

handle_info({'EXIT', OutboundConn, Reason},
            State = #state{config = #vshovel{dest_state = #amqp_state{outbound_conn =
                                                                      OutboundConn}}}) ->
  {stop, {outbound_conn_died, Reason}, State}.

terminate(Reason, #state{inbound_conn = undefined, inbound_ch = undefined,
                         config       = #vshovel{dest_mod   = Mod,
                                                 dest_state = DestState},
                         name         = Name, type = Type}) ->
  catch Mod:terminate(DestState),
  rabbit_vshovel_status:report(Name, Type, {terminated, Reason}),
  ok;
terminate({shutdown, autodelete}, State = #state{name   = {VHost, Name},
                                                 config = #vshovel{dest_mod   = Mod,
                                                                   dest_state = DestState},
                                                 type   = dynamic}) ->
  close_connections(State),
  %% See rabbit_vshovel_dyn_worker_sup_sup:stop_child/1
  put(vshovel_worker_autodelete, true),
  rabbit_runtime_parameters:clear(VHost, <<"vshovel">>, Name, ?VSHOVEL_USER),
  rabbit_vshovel_status:remove({VHost, Name}),
  catch Mod:terminate(DestState),
  ok;
terminate(Reason, State = #state{name = Name, type = Type, config =
#vshovel{dest_mod   = Mod,
         dest_state = DestState}}) ->
  close_connections(State),
  rabbit_vshovel_status:report(Name, Type, {terminated, Reason}),
  catch Mod:terminate(DestState),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% -------------------------------
%% vShovel Endpoint Implementation
%% -------------------------------
validate_arguments(Args) ->
  {ok, Args}.

validate_address(Args) when is_list(Args);
                            is_binary(Args) ->
  {ok, Args};
validate_address(Other) -> {error, Other}.

init(InboundChan, #vshovel{ack_mode     = AckMode,
                           destinations =
                           #endpoint{address              = DestinationAddress,
                                     resource_declaration = Fun}}) ->
  {OutboundConn, OutboundChan, OutboundURI} =
  make_conn_and_chan(DestinationAddress),

  catch Fun(OutboundConn, OutboundChan),

  case AckMode of
    on_confirm ->
      #'confirm.select_ok'{} =
      amqp_channel:call(OutboundChan, #'confirm.select'{}),
      ok = amqp_channel:register_confirm_handler(OutboundChan, self());
    _ ->
      ok
  end,

  {ok, #amqp_state{inbound_ch    = InboundChan,
                   outbound_conn = OutboundConn,
                   outbound_ch   = OutboundChan,
                   outbound_uri  = OutboundURI}}.

handle_broker_message(_AMQPMessage, AMQPState = #amqp_state{}) ->
  {ok, AMQPState}.

terminate(_AMQPState = #amqp_state{outbound_ch = OutboundChan}) ->
  catch amqp_connection:close(OutboundChan, ?MAX_CONNECTION_CLOSE_TIMEOUT),
  ok.

%%---------------------------
%% Helpers
%%---------------------------

confirm_to_inbound(MsgCtr, Seq, Multiple, State =
                                          #state{inbound_ch = InboundChan, unacked = Unacked}) ->
  ok = amqp_channel:cast(
    InboundChan, MsgCtr(gb_trees:get(Seq, Unacked), Multiple)),
  {Unacked1, Removed} = remove_delivery_tags(Seq, Multiple, Unacked, 0),
  decr_remaining(Removed, State#state{unacked = Unacked1}).

remove_delivery_tags(Seq, false, Unacked, 0) ->
  {gb_trees:delete(Seq, Unacked), 1};
remove_delivery_tags(Seq, true, Unacked, Count) ->
  case gb_trees:is_empty(Unacked) of
    true -> {Unacked, Count};
    false -> {Smallest, _Val, Unacked1} = gb_trees:take_smallest(Unacked),
      case Smallest > Seq of
        true -> {Unacked, Count};
        false -> remove_delivery_tags(Seq, true, Unacked1, Count + 1)
      end
  end.

report_running(#state{name        = Name,
                      inbound_uri = InboundURI,
                      config      = #vshovel{dest_state = DestState},
                      type        = Type}) ->
  rabbit_vshovel_status:report(Name, Type, {running, [{src_uri, InboundURI},
                                                      {dest_state, DestState}]}).

publish(_Tag, _Method, _Msg, State = #state{remaining_unacked = 0}) ->
  %% We are in on-confirm mode, and are autodelete. We have
  %% published all the messages we need to; we just wait for acks to
  %% come back. So drop subsequent messages on the floor to be
  %% requeued later.
  State;

publish(Tag, Method, Msg, State = #state{inbound_ch = InboundChan,
                                         config     = #vshovel{
                                           ack_mode   = AckMode,
                                           dest_state =
                                           #amqp_state{outbound_ch =
                                                       OutboundChan}},
                                         unacked    = Unacked}) ->
  Seq = case AckMode of
          on_confirm -> amqp_channel:next_publish_seqno(OutboundChan);
          _ -> undefined
        end,
  ok = amqp_channel:call(OutboundChan, Method, Msg),
  decr_remaining_unacked(
    case AckMode of
      no_ack -> decr_remaining(1, State);
      on_confirm -> State#state{unacked = gb_trees:insert(
        Seq, Tag, Unacked)};
      on_publish -> ok = amqp_channel:cast(
        InboundChan, #'basic.ack'{delivery_tag = Tag}),
        decr_remaining(1, State)
    end).

make_conn_and_chan(URIs) ->
  URI = lists:nth(rand:uniform(length(URIs)), URIs),
  {ok, AmqpParam} = amqp_uri:parse(URI),
  {ok, Conn} = amqp_connection:start(AmqpParam),
  link(Conn),
  {ok, Chan} = amqp_connection:open_channel(Conn),
  {Conn, Chan, list_to_binary(amqp_uri:remove_credentials(URI))}.

remaining(_Ch, #vshovel{delete_after = never}) ->
  unlimited;
remaining(Ch, #vshovel{delete_after = 'queue-length', queue = Queue}) ->
  #'queue.declare_ok'{message_count = N} =
  amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                         passive = true}),
  N;
remaining(_Ch, #vshovel{delete_after = Count}) ->
  Count.

decr_remaining(_N, State = #state{remaining = unlimited}) ->
  State;
decr_remaining(N, State = #state{remaining = M}) ->
  case M > N of
    true -> State#state{remaining = M - N};
    false -> exit({shutdown, autodelete})
  end.

decr_remaining_unacked(State = #state{remaining_unacked = unlimited}) ->
  State;
decr_remaining_unacked(State = #state{remaining_unacked = 0}) ->
  State;
decr_remaining_unacked(State = #state{remaining_unacked = N}) ->
  State#state{remaining_unacked = N - 1}.

close_connections(#state{inbound_conn = InboundConn}) ->
  catch amqp_connection:close(InboundConn,
                              ?MAX_CONNECTION_CLOSE_TIMEOUT).
