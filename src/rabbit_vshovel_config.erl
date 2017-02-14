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

-module(rabbit_vshovel_config).

-export([parse/2,
         ensure_defaults/2, ensure_started/1,
         get_endpoint_module/1, get_endpoint_module/2,
         to_atom/1, to_list/1, to_integer/1, to_binary/1]).

-import(rabbit_misc, [pget/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_vshovel.hrl").

-define(IGNORE_FIELDS, [name, delete_after, dest_mod, dest_state]).
-define(EXTRA_KEYS, [add_forward_headers, add_timestamp_header]).

parse(VShovelName, Config) ->
    {ok, Defaults} = application:get_env(defaults),
    try
        {ok, parse_vshovel_config_dict(
               VShovelName, parse_vshovel_config_proplist(
                              enrich_vshovel_config(Config, Defaults)))}
    catch throw:{error, Reason} ->
            {error, {invalid_vshovel_configuration, VShovelName, Reason}}
    end.

%% ensures that any defaults that have been applied to a parsed
%% shovel, are written back to the original proplist
ensure_defaults(VShovelConfig, ParsedShovel) ->
    lists:keystore(reconnect_delay, 1,
                   VShovelConfig,
                   {reconnect_delay,
                    ParsedShovel#vshovel.reconnect_delay}).

enrich_vshovel_config(Config, Defaults) ->
    Config1 = proplists:unfold(Config),
    case [E || E <- Config1, not (is_tuple(E) andalso tuple_size(E) == 2)] of
        []      -> case duplicate_keys(Config1) of
                       []   -> return(lists:ukeysort(1, Config1 ++ Defaults));
                       Dups -> fail({duplicate_parameters, Dups})
                   end;
        Invalid -> fail({invalid_parameters, Invalid})
    end.

parse_vshovel_config_proplist(Config) ->
    Dict = dict:from_list(Config),
    Fields = record_info(fields, vshovel) -- ?IGNORE_FIELDS,
    Keys = dict:fetch_keys(Dict) -- ?EXTRA_KEYS,
    case {Keys -- Fields, Fields -- Keys} of
        {[], []}      -> {_Pos, Dict1} =
                             lists:foldl(
                               fun (FieldName, {Pos, Acc}) ->
                                       {Pos + 1,
                                        dict:update(FieldName,
                                                    fun (V) -> {V, Pos} end,
                                                    Acc)}
                               end, {2, Dict}, Fields),
                         return(Dict1);
        {[], Missing} -> fail({missing_parameters, Missing});
        {Unknown, _}  -> fail({unrecognised_parameters, Unknown})
    end.

parse_vshovel_config_dict(Name, Dict) ->
    Cfg = run_state_monad(
            [fun (Shovel) ->
                     {ok, Value} = dict:find(Key, Dict),
                     try {ParsedValue, Pos} = Fun(Value),
                          return(setelement(Pos, Shovel, ParsedValue))
                     catch throw:{error, Reason} ->
                             fail({invalid_parameter_value, Key, Reason})
                     end
             end || {Fun, Key} <-
                        [{fun parse_endpoint/1,             sources},
                         {fun parse_endpoint/1,             destinations},
                         {fun parse_non_negative_integer/1, prefetch_count},
                         {fun parse_ack_mode/1,             ack_mode},
                         {fun parse_binary/1,               queue},
                         make_parse_publish(publish_fields),
                         make_parse_publish(publish_properties),
                         {fun parse_non_negative_number/1,  reconnect_delay}]],
            #vshovel{}),
    Cfg1 = case dict:find(add_forward_headers, Dict) of
        {ok, true} -> add_forward_headers_fun(Name, Cfg);
        _          -> Cfg
    end,
    case dict:find(add_timestamp_header, Dict) of
        {ok, true} -> add_timestamp_header_fun(Cfg1);
        _          -> Cfg1
    end.

%% --=: Plain state monad implementation start :=--
run_state_monad(FunList, State) ->
    lists:foldl(fun (Fun, StateN) -> Fun(StateN) end, State, FunList).

return(V) -> V.

fail(Reason) -> throw({error, Reason}).
%% --=: end :=--

parse_endpoint({Endpoint, Pos}) when is_list(Endpoint) ->

    Protocol1 = case rabbit_misc:pget(protocol, Endpoint) of
                  undefined ->
                      rabbit_log:warning("vShovel protocol undefined. "
                                         "Default 'amqp' protocol set."),
                      ?AMQP_PROTOCOL;
                  Protocol0 ->
                      Protocol0
               end,

    Protocol = case rabbit_vshovel_endpoint:ensure_protocol(Protocol1) of
      {ok, Protocol2} -> Protocol2;
      Error  -> fail({bad_protocol_configuration_format, Error})
    end,

    if Protocol =:= ?AMQP_PROTOCOL ->

        Brokers = get_brokers(Endpoint),

        {[], Brokers1} = run_state_monad(
                           lists:duplicate(length(Brokers),
                                           fun check_uri/1),
                           {Brokers, []}),

        Arguments = case rabbit_misc:pget(arguments, Endpoint, []) of
            Arguments0 when is_list(Arguments0) ->
                Arguments0;
            Other -> fail({expected_list, arguments, Other})
        end,

        ResourceDecls =
            case rabbit_misc:pget(declarations, Arguments, []) of
                Decls when is_list(Decls) ->
                    Decls;
                Decls ->
                    fail({expected_list, declarations, Decls})
            end,
        {[], ResourceDecls1} =
            run_state_monad(
              lists:duplicate(length(ResourceDecls), fun parse_declaration/1),
              {ResourceDecls, []}),

        DeclareFun =
            fun (_Conn, Ch) ->
                    [amqp_channel:call(Ch, M) || M <- lists:reverse(ResourceDecls1)]
            end,

        return({#endpoint{protocol  = Protocol,
                          address   = Brokers1,
                          arguments = Arguments,
                          resource_declaration = DeclareFun
                          },
                Pos});
    true ->
        parse_endpoint(Protocol, {Endpoint, Pos})
    end;

parse_endpoint({Endpoint, _Pos}) ->
    fail({require_list, Endpoint}).

parse_endpoint(Protocol, {Endpoint, Pos}) when is_atom(Protocol),
                                               is_list(Endpoint) ->
    Mod = get_endpoint_module(Protocol),

    Addresses0 = get_addresses(Endpoint),
    Addresses  = validate_addresses(Mod, Addresses0),

    Arguments = case pget(arguments, Endpoint) of
        Arguments0 when is_list(Arguments0) ->
            validate_arguments(Mod, Arguments0);
        undefined -> [];
        Other     -> fail({expected_list, arguments, Other})
    end,

    return({#endpoint{protocol  = Protocol,
                      address   = Addresses,
                      arguments = Arguments},
            Pos}).

check_uri({[Uri | Uris], Acc}) ->
    case amqp_uri:parse(Uri) of
        {ok, _Params} ->
            return({Uris, [Uri | Acc]});
        {error, _} = Err ->
            throw(Err)
    end.

parse_declaration({[{Method, Props} | Rest], Acc}) when is_list(Props) ->
    FieldNames = try rabbit_framing_amqp_0_9_1:method_fieldnames(Method)
                 catch exit:Reason -> fail(Reason)
                 end,
    case proplists:get_keys(Props) -- FieldNames of
        []            -> ok;
        UnknownFields -> fail({unknown_fields, Method, UnknownFields})
    end,
    {Res, _Idx} = lists:foldl(
                    fun (K, {R, Idx}) ->
                            NewR = case proplists:get_value(K, Props) of
                                       undefined -> R;
                                       V         -> setelement(Idx, R, V)
                                   end,
                            {NewR, Idx + 1}
                    end, {rabbit_framing_amqp_0_9_1:method_record(Method), 2},
                    FieldNames),
    return({Rest, [Res | Acc]});
parse_declaration({[{Method, Props} | _Rest], _Acc}) ->
    fail({expected_method_field_list, Method, Props});
parse_declaration({[Method | Rest], Acc}) ->
    parse_declaration({[{Method, []} | Rest], Acc}).

parse_non_negative_integer({N, Pos}) when is_integer(N) andalso N >= 0 ->
    return({N, Pos});
parse_non_negative_integer({N, _Pos}) ->
    fail({require_non_negative_integer, N}).

parse_non_negative_number({N, Pos}) when is_number(N) andalso N >= 0 ->
    return({N, Pos});
parse_non_negative_number({N, _Pos}) ->
    fail({require_non_negative_number, N}).

parse_binary({Binary, Pos}) when is_binary(Binary) ->
    return({Binary, Pos});
parse_binary({NotABinary, _Pos}) ->
   fail({require_binary, NotABinary}).

parse_ack_mode({Val, Pos}) when Val =:= no_ack orelse
                                Val =:= on_publish orelse
                                Val =:= on_confirm ->
    return({Val, Pos});
parse_ack_mode({WrongVal, _Pos}) ->
    fail({ack_mode_value_requires_one_of, {no_ack, on_publish, on_confirm},
          WrongVal}).

make_parse_publish(publish_fields) ->
    {make_parse_publish1(record_info(fields, 'basic.publish')), publish_fields};
make_parse_publish(publish_properties) ->
    {make_parse_publish1(record_info(fields, 'P_basic')), publish_properties}.

make_parse_publish1(ValidFields) ->
    fun ({Fields, Pos}) when is_list(Fields) ->
            make_publish_fun(Fields, Pos, ValidFields);
        ({Fields, _Pos}) ->
            fail({require_list, Fields})
    end.

make_publish_fun(Fields, Pos, ValidFields) ->
    SuppliedFields = proplists:get_keys(Fields),
    case SuppliedFields -- ValidFields of
        [] ->
            FieldIndices = make_field_indices(ValidFields, Fields),
            Fun = fun (_SrcUri, _DestUri, Publish) ->
                          lists:foldl(fun ({Pos1, Value}, Pub) ->
                                              setelement(Pos1, Pub, Value)
                                      end, Publish, FieldIndices)
                  end,
            return({Fun, Pos});
        Unexpected ->
            fail({unexpected_fields, Unexpected, ValidFields})
    end.

make_field_indices(Valid, Fields) ->
    make_field_indices(Fields, field_map(Valid, 2), []).

make_field_indices([], _Idxs , Acc) ->
    lists:reverse(Acc);
make_field_indices([{Key, Value} | Rest], Idxs, Acc) ->
    make_field_indices(Rest, Idxs, [{dict:fetch(Key, Idxs), Value} | Acc]).

field_map(Fields, Idx0) ->
    {Dict, _IdxMax} =
        lists:foldl(fun (Field, {Dict1, Idx1}) ->
                            {dict:store(Field, Idx1, Dict1), Idx1 + 1}
                    end, {dict:new(), Idx0}, Fields),
    Dict.

duplicate_keys(PropList) ->
    proplists:get_keys(
      lists:foldl(fun (K, L) -> lists:keydelete(K, 1, L) end, PropList,
                  proplists:get_keys(PropList))).

add_forward_headers_fun(Name, #vshovel{publish_properties = PubProps} = Cfg) ->
    PubProps2 =
        fun(SrcUri, DestUri, Props) ->
                rabbit_vshovel_util:update_headers(
                  [{<<"vshovelled-by">>, rabbit_nodes:cluster_name()},
                   {<<"vshovel-type">>,  <<"static">>},
                   {<<"vshovel-name">>,  list_to_binary(atom_to_list(Name))}],
                  [], SrcUri, DestUri, PubProps(SrcUri, DestUri, Props))
        end,
    Cfg#vshovel{publish_properties = PubProps2}.

add_timestamp_header_fun(#vshovel{publish_properties = PubProps} = Cfg) ->
    PubProps2 =
        fun(SrcUri, DestUri, Props) ->
            rabbit_vshovel_util:add_timestamp_header(
                PubProps(SrcUri, DestUri, Props))
        end,
    Cfg#vshovel{publish_properties = PubProps2}.

get_endpoint_module(Protocol) ->
    get_endpoint_module(Protocol, undefined).

get_endpoint_module(Protocol, Version) ->
    case rabbit_vshovel_endpoint:module(Protocol, Version) of
        Module when is_atom(Module) ->
            ok = verify_if_loaded(Module),
            Module;
        _ -> fail({unsupported_protocol, Protocol})
    end.

verify_if_loaded(Module) ->
    case code:which(Module) of
        non_existing -> fail({protocol_implementation_not_found, Module});
        _ -> ok
    end.

validate_arguments(Mod, Arguments) ->
    case Mod:validate_arguments(Arguments) of
        {ok, Arguments0} -> Arguments0;
        {error, Reason}  -> fail({destination_arguments_validation_error, Reason})
    end.

validate_address(Mod, Address) ->
    case Mod:validate_address(Address) of
        {ok, Address0}  -> Address0;
        {error, Reason} -> fail({destination_address_validation_error, Reason})
    end.
validate_addresses(Mod, Addresses) ->
    [validate_address(Mod, Address) || Address <- Addresses].

ensure_started([]) -> ok;
ensure_started([App | Rem]) when is_atom(App) ->
    case application:ensure_started(App) of
        ok      -> ensure_started(Rem);
        {error, _}  -> application:start(App)
    end.

get_brokers(Endpoint) ->
    try get_endpoint(Endpoint, brokers, broker, broker_or_brokers) catch
      _:Reason ->
            %% Maybe user configured 'address' or 'addresses' instead.
            try get_addresses(Endpoint) catch
                _:_ -> throw(Reason)
            end
    end.

get_addresses(Endpoint) ->
    get_endpoint(Endpoint, addresses, address, address_or_addresses).

get_endpoint(Endpoint, MultipleTag, SingleTag, ErrorMsg) ->
    case pget(MultipleTag, Endpoint) of
        undefined ->
            case pget(SingleTag, Endpoint) of
                undefined -> 
                    fail({missing_endpoint_parameter, ErrorMsg});
                B         -> [B]
            end;
        Bs when is_list(Bs) ->
            Bs;
        B ->
            fail({expected_list, MultipleTag, B})
    end.

%% Data conversion helpers
to_atom(V)   when is_list(V)     -> list_to_atom(V);
to_atom(V)   when is_binary(V)   -> list_to_atom(binary_to_list(V));
to_atom(V)                       -> V.

to_list(V)   when is_atom(V)     -> atom_to_list(V);
to_list(V)   when is_binary(V)   -> binary_to_list(V);
to_list(V)   when is_integer(V)  -> integer_to_list(V);
to_list(V)                       -> V.

to_integer(V) when is_integer(V) -> V;
to_integer(V) when is_list(V)    -> list_to_integer(V);
to_integer(V) when is_binary(V)  -> binary_to_integer(V);
to_integer(V)                    -> throw({bad_type, V}).

to_binary(V)  when is_list(V)    -> list_to_binary(V);
to_binary(V)  when is_atom(V)    -> list_to_binary(atom_to_list(V));
to_binary(V) when is_list(V)     -> integer_to_binary(V);
to_binary(V)                     -> V.
