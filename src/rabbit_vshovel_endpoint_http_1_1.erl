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

-module(rabbit_vshovel_endpoint_http_1_1).
-behaviour(rabbit_vshovel_endpoint).

-export([init/2, 
		 validate_address/1,
		 validate_arguments/1,
		 handle_broker_message/2,
		 terminate/1]).

-include("rabbit_vshovel.hrl").
-include("rabbit_vshovel_http.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% -----------
%% Definitions
%% -----------
-record(http_state, {uri,
					 method = ?HTTP_POST,
					 args,
					 http_options,
					 source_queue,
					 ack_mode,
					 source_channel,
					 result
					}).

-define(REQUESTING_ENTITY,  	"RabbitMQ-vShovel").
-define(VERSION, 		 		?VSHOVEL_VERSION).
-define(DEFAULT_CONTENT_TYPE,	"application/octet-stream").
-define(ACCEPT, 		 		"*/*").

%% -------------------
%% Behaviour callbacks
%% -------------------
validate_arguments(Args) when is_list(Args) ->
	{ok, validate_arguments([{?TO_ATOM(T), V} || {T, V} <- Args], [])};
validate_arguments(Args) -> {error, Args}.

validate_address(Address) when is_list(Address); is_binary(Address) ->
    try 
    	{ok, _} = http_uri:parse(?TO_LIST(Address)),
    	{ok, Address}
    catch
    	_:{badmatch, Error = {error, _}} -> Error;
    	_:Error                          -> Error
    end;
validate_address(Other) -> {error, Other}.

init(Ch, _VState = #vshovel{queue        = QueueName,
	    		            destinations = #endpoint{address   = DestAddresses,
						   		   			         arguments = Args}}) ->
	InitOptions = parse_options(Args, []),
	HttpOptions = parse_http_options(Args, []),
	Method      = parse_method(rabbit_misc:pget(method, Args, ?HTTP_POST)),
	ok          = httpc:set_options(InitOptions),
	{ok, #http_state{uri = hd(DestAddresses),
					 method         = Method,
					 args           = Args,
					 http_options   = HttpOptions,
					 source_queue   = QueueName,
					 source_channel = Ch}}.

handle_broker_message({#'basic.deliver'{exchange    = Exchange, 
										routing_key	= RoutingKey,
										delivery_tag=DeliveryTag},
					  #amqp_msg{props   = #'P_basic'{content_type = ContentType,
					  								 headers      = Headers0},
					  			payload = Payload}},
					  HttpState= #http_state{uri   	        = URI,
					  			  		     method         = Method,
					  			  		     http_options   = HttpOptions,
					  			  		     source_queue   = QueueName,
					  			  		     source_channel = Channel}) ->
	ContentType0= set_content_type(ContentType),
	Headers     = set_headers(QueueName, Exchange, RoutingKey, Headers0),
	Request     = {URI, Headers, ContentType0, Payload},
    %%worker_pool:submit_async(fun() ->
        send(Method, Request, HttpOptions, Channel, DeliveryTag),
    %%end),
    {ok, HttpState}.

terminate(_HttpState) ->
	ok.

%% -------
%% Private
%% -------
send(Method, Request, Options, Channel, DeliveryTag) ->
	case catch httpc:request(Method, Request, Options, []) of
        {ok, {{_HTTP, Code, _}, _Headers, _Body}} ->
            case handle_response(Code, Channel, DeliveryTag) of
                ok  	->  {ok, Code};
                Error   ->
                	rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE,
                		Error),
                	Error
            end;
        Error -> 
            rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, Error),
            Error
    end.

%% HTTP 200 Range Response Codes
handle_response(HTTPErrCode, Channel, DeliveryTag) when
											HTTPErrCode >= ?HTTP_200_RANGE,
										   	HTTPErrCode <  ?HTTP_300_RANGE ->
	%% Issue 'basic.ack' back to the broker for 2XX response codes.
	amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag=DeliveryTag }),
	ok;

%% HTTP 300 Range Response Codes
handle_response(HTTPErrCode, Channel, DeliveryTag) when 
											HTTPErrCode >= ?HTTP_300_RANGE,
										    HTTPErrCode <  ?HTTP_400_RANGE ->
	%% 'autoredirect' option = 'true' by default, hence we issue and
	%% 'basic.ack' back to the broker for 30X response codes.
	amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag=DeliveryTag }),
	rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, HTTPErrCode),
	ok;

%% HTTP 400 Range Response Codes
handle_response(HTTPErrCode, Channel, DeliveryTag) when
										   HTTPErrCode >= ?HTTP_400_RANGE,
										   HTTPErrCode <  ?HTTP_500_RANGE ->
	%% Issue 'basic.nack' back to the broker for 4XX range response codes, 
	%% indicating message delivery failure on HTTP endpoint.
	amqp_channel:cast(Channel, #'basic.nack'{ delivery_tag=DeliveryTag }),
	rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, HTTPErrCode),
	ok;

%% HTTP 500 Range Response Codes
handle_response(HTTPErrCode, Channel, DeliveryTag) when
										   HTTPErrCode >= ?HTTP_500_RANGE,
										   HTTPErrCode <  ?HTTP_600_RANGE ->
	%% Issue 'basic.nack' back to the broker for 4XX range response codes, 
	%% indicating message delivery failure on HTTP endpoint.
	amqp_channel:cast(Channel, #'basic.nack'{ delivery_tag=DeliveryTag }),
	rabbit_vshovel_endpoint:notify_and_maybe_log(?MODULE, HTTPErrCode),
	ok.

%% Requeueing options -> dead letter option.
set_headers(QueueName, Exchange, RoutingKey, Headers) ->
	parse_headers(Headers) ++ 
	[{"Accept", 			?ACCEPT},
	 {"X-Requested-With", 	?REQUESTING_ENTITY},
	 {"X-vShovel-Version",  ?VERSION},
	 {"X-Source-Queue",     binary_to_list(QueueName)},
	 {"X-Exchange",         binary_to_list(Exchange)},
	 {"X-Routing-Key",      binary_to_list(RoutingKey)}].

set_content_type(ContentType) ->
	case ContentType of
		  undefined -> ?DEFAULT_CONTENT_TYPE;
		  C -> binary_to_list(C)
	end.

parse_headers(Headers) when is_list(Headers) ->
	lists:foldl(fun (Header, Acc) ->
					case Header of
						{Key, _, Value} ->
							[{binary_to_list(Key), binary_to_list(Value)}|Acc];
						_  -> Acc 	
					end
				end, [], Headers);
parse_headers(_Headers) -> [].

%% Acqurie vShovel supported 'httpc' options for initialization
%% Ref: http://erlang.org/doc/man/httpc.html#set_options-1
parse_options([], Acc) -> Acc;
parse_options([{Tag = max_sessions, Value} | Args], Acc) -> 
	parse_options(Args, [{(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_options([{Tag = max_keep_alive_length, Value} | Args], Acc) -> 
	parse_options(Args, [{Tag, ?TO_INTEGER(Value)}|Acc]);
parse_options([{Tag = keep_alive_timeout, Value} | Args], Acc) ->
	parse_options(Args, [{?TO_ATOM(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_options([{Tag = max_pipeline_length, Value} | Args], Acc) ->
	parse_options(Args, [{?TO_ATOM(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_options([{Tag = pipeline_timeout, Value} | Args], Acc) ->
	parse_options(Args, [{?TO_ATOM(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_options([_| Args], Acc) -> parse_options(Args, Acc).

parse_http_options([], Acc) -> Acc;
parse_http_options([{Tag = timeout, Value} | Args], Acc) -> 
	parse_http_options(Args, [{(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_http_options([{Tag = connect_timeout, Value} | Args], Acc) -> 
	parse_http_options(Args, [{Tag, ?TO_INTEGER(Value)}|Acc]);
parse_http_options([{Tag = ssl, Value} | Args], Acc) ->
	parse_http_options(Args, [{?TO_ATOM(Tag), parse_ssl_options(Value, [])}|Acc]);
parse_http_options([{Tag = keep_alive_timeout, Value} | Args], Acc) ->
	parse_http_options(Args, [{?TO_ATOM(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_http_options([{Tag = autoredirect, Value} | Args], Acc) ->
	parse_http_options(Args, [{?TO_ATOM(Tag), ?TO_INTEGER(Value)}|Acc]);
parse_http_options([_ | Args], Acc) ->
	parse_http_options(Args, Acc).


parse_ssl_options([], Acc) -> Acc;
parse_ssl_options([{Tag = verify, Value} | Args], Acc) -> 
	parse_ssl_options(Args, [{Tag, Value}|Acc]);
parse_ssl_options([{Tag = verify_fun, Value} | Args], Acc) -> 
	parse_ssl_options(Args, [{Tag, Value}|Acc]);
parse_ssl_options([{Tag = fail_if_no_peer_cert, Value} | Args], Acc) -> 
	parse_ssl_options(Args, [{Tag, Value}|Acc]);
parse_ssl_options([{Tag = depth, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), parse_ssl_options(Value, [])}|Acc]);
parse_ssl_options([{Tag = cert, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = certfile, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = key, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = keyfile, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = password, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = cacerts, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = cacertfile, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = dh, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]);
parse_ssl_options([{Tag = dhfile, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = ciphers, Value} | Args], Acc) ->
 	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = user_lookup_fun, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = reuse_sessions, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = reuse_session, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = client_preferred_next_protocols, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = log_alert, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = server_name_indication, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = sni_hosts, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = sni_fun, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), ?TO_LIST(Value)}|Acc]);
parse_ssl_options([{Tag = cb_info, Value} | Args], Acc) ->
	parse_ssl_options(Args, [{?TO_ATOM(Tag), Value}|Acc]).

parse_method(V) -> to_method(V).

validate_arguments([], Acc)                  -> Acc;
validate_arguments([{T=method, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{method, validate_method(T, to_method(V))} | Acc]);
validate_arguments([{T=keep_alive_timeout, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{keep_alive_timeout, validate_numeric(T, V)} | Acc]);
validate_arguments([{T=max_sessions, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{max_sessions, validate_numeric(T, V)} | Acc]);
validate_arguments([{T=max_keep_alive_length, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{max_keep_alive_length, validate_numeric(T, V)} | Acc]);
validate_arguments([{T=ssl, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{ssl, validate_list(T, V)} | Acc]);
validate_arguments([{T=timeout, V}|Rem], Acc) -> 
	validate_arguments(Rem, [{timeout, validate_numeric(T, V)} | Acc]);
validate_arguments([H = {_, _}|Rem], Acc)  -> validate_arguments(Rem, [H|Acc]);
validate_arguments([_|Rem], Acc)        -> validate_arguments(Rem, Acc).

validate_method(_, ?HTTP_POST)   -> ?HTTP_POST;
validate_method(_, ?HTTP_PUT)    -> ?HTTP_PUT;
validate_method(_, ?HTTP_DELETE) -> ?HTTP_DELETE;
validate_method(_T, _Other)        -> ?HTTP_POST.

validate_numeric(_, V) when is_integer(V) -> V;
validate_numeric(T, V) ->
	try ?TO_INTEGER(V) catch
		_:_ -> throw({T, V})
	end.

validate_list(_, V) when is_list(V) -> V;
validate_list(T, V) ->
	try ?TO_LIST(V) catch
		_:_ -> throw({T, V})
	end.

to_method(V) when is_atom(V)  -> V; 
to_method(V) 				  -> ?TO_ATOM(string:to_lower(?TO_LIST(V))).
