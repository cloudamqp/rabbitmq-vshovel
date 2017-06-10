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

-module(rabbit_vshovel_endpoint).

-export([module/1, module/2, ensure_protocol/1, ensure_version/1,
         notify_and_maybe_log/2, notify_and_maybe_log/3]).

-include("rabbit_vshovel.hrl").

%% -------------------
%% Behaviour callbacks
%% -------------------
-callback init(pid(), vshovel_record()) -> {'ok', term()} | {'error', term()}.

-callback validate_address(iodata()) -> {'ok', term()} | vshovel_error().

-callback validate_arguments(list()) -> {'ok', term()} | vshovel_error().

-callback handle_broker_message(term(), term()) -> {'ok', term()} | vshovel_error().

-callback terminate(term()) -> 'ok'.


-spec module(vshovel_protocol(), term()) -> atom() | vshovel_error().

-spec ensure_protocol(term()) -> {ok, atom()}.

-spec ensure_version(term()) -> {ok, list()}.

-spec notify_and_maybe_log(atom(), term()) -> term().

-spec notify_and_maybe_log(atom(), term(), term()) -> term().

module(http,  _)   	-> rabbit_vshovel_endpoint_http_1_1;
module(https, _)   	-> module(http,  <<"1.1">>);
module(amqp, _)		-> rabbit_vshovel_worker;
module(Other, _)   	-> {error, io_lib:format("Unsupported protocol: ~p", [Other])}.

module(http)   		-> module(http,  <<"1.1">>);
module(https)   	-> module(http,  <<"1.1">>);
module(amqp)		-> module(amqp,  "0.9.1");
module(Other)  	 	-> {error, io_lib:format("Unsupported protocol: ~p", [Other])}.

ensure_protocol(V) when is_atom(V) -> {ok, V};
ensure_protocol(V)  -> 
    try
        {ok, ?TO_ATOM(string:to_lower(?TO_LIST(V)))}
    catch
        _:Reason -> {error, Reason}
    end.

ensure_version(V)  -> {ok, ?TO_LIST(V)}.

notify_and_maybe_log(Endpoint, Result) ->
	notify_and_maybe_log(vshovel_result, Endpoint, Result).
notify_and_maybe_log(EventName, Endpoint, Result) ->
    rabbit_event:notify(EventName, [{source, Endpoint}, {result, Result}]),
    case application:get_env(rabbitmq_vshovel, log_result) of
        true -> rabbit_log:info("vShovel result from '~p' endpoint: ~p~n",
                                [EventName, Result]);
        _    -> void
    end.
