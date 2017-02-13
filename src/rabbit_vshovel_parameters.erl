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

-module(rabbit_vshovel_parameters).

-behaviour(rabbit_runtime_parameter).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_vshovel.hrl").

-export([validate/5, notify/5, notify/4,
         notify_clear/3, notify_clear/4]).
-export([register/0, unregister/0, parse/2]).

-import(rabbit_misc, [pget/2, pget/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "vShovel parameters"},
                    {mfa, {rabbit_vshovel_parameters, register, []}},
                    {cleanup, {?MODULE, unregister, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter, <<"vshovel">>, ?MODULE).

unregister() ->
    rabbit_registry:unregister(runtime_parameter, <<"vshovel">>).

validate(_VHost, <<"vshovel">>, Name, Def, User) ->
    {ok, DestType} = rabbit_vshovel_endpoint:ensure_protocol(pget(<<"dest-type">>, Def, ?AMQP_PROTOCOL)),
    {ok, Version}  = rabbit_vshovel_endpoint:ensure_version(pget(<<"dest-vsn">>, Def, ?DEFAULT_AMQP_VERSION)),

    Mod = rabbit_vshovel_config:get_endpoint_module(DestType, Version),
    [case Mod of
    	   Mod  when is_atom(Mod) -> ok;
    	   _Other  -> {error, "Unsupported protocol: \"~s\"", [DestType]}
     end,
     case pget2(<<"src-exchange">>, <<"src-queue">>, Def) of
         zero -> {error, "Must specify 'src-exchange' or 'src-queue'", []};
         one  -> ok;
         both -> {error, "Cannot specify 'src-exchange' and 'src-queue'", []}
     end,
     if DestType =:= ?AMQP_PROTOCOL ->
        case pget2(<<"dest-exchange">>, <<"dest-queue">>, Def) of
            zero -> ok;
            one  -> ok;
            both -> {error, "Cannot specify 'dest-exchange' and 'dest-queue'", []}
        end;
     true ->
        ok
     end,
     case {pget(<<"delete-after">>, Def), pget(<<"ack-mode">>, Def)} of
         {N, <<"no-ack">>} when is_integer(N) ->
             {error, "Cannot specify 'no-ack' and numerical 'delete-after'", []};
         _ ->
             ok
     end | rabbit_parameter_validation:proplist(Name, validation(User, Mod, Def), Def)];

validate(_VHost, _Component, Name, _Term, _User) ->
    {error, "name not recognised: ~p", [Name]}.

pget2(K1, K2, Defs) -> case {pget(K1, Defs), pget(K2, Defs)} of
                           {undefined, undefined} -> zero;
                           {undefined, _}         -> one;
                           {_,         undefined} -> one;
                           {_,         _}         -> both
                       end.

notify(VHost, <<"vshovel">>, Name, Definition) ->
    rabbit_vshovel_dyn_worker_sup_sup:adjust({VHost, Name}, Definition).

notify(VHost, <<"vshovel">>, Name, Definition, _Username) ->
    notify(VHost, <<"vshovel">>, Name, Definition).

notify_clear(VHost, <<"vshovel">>, Name) ->
    rabbit_vshovel_dyn_worker_sup_sup:stop_child({VHost, Name}).
notify_clear(VHost, <<"vshovel">>, Name, _Username) ->
    notify_clear(VHost, <<"vshovel">>, Name).
%%----------------------------------------------------------------------------

validation(User, CB, Def) ->
    [{<<"src-uri">>,         validate_uri_fun(User), mandatory},
     {<<"dest-uri">>,        validate_address_fun(User, CB), mandatory},
     {<<"dest-type">>,       fun validate_dest_type_fun/2, optional},
     {<<"dest-args">>,       validate_dest_args_fun(CB), optional},
     {<<"dest-vsn">>,        fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-port">>,       fun rabbit_parameter_validation:number/2,optional},
     {<<"src-exchange">>,    fun rabbit_parameter_validation:binary/2,optional},
     {<<"src-exchange-key">>,fun rabbit_parameter_validation:binary/2,optional},
     {<<"src-queue">>,       fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-exchange">>,   fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-exchange-key">>,fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-queue">>,      fun rabbit_parameter_validation:binary/2,optional},
     {<<"prefetch-count">>,  fun rabbit_parameter_validation:number/2,optional},
     {<<"reconnect-delay">>, fun rabbit_parameter_validation:number/2,optional},
     {<<"add-forward-headers">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"publish-properties">>, fun validate_properties/2,  optional},
     {<<"ack-mode">>,        rabbit_parameter_validation:enum(
                               ['no-ack', 'on-publish', 'on-confirm']), optional},
     {<<"delete-after">>,    fun validate_delete_after/2, optional}
    ].

validate_uri_fun(User) ->
    fun (Name, Term) -> validate_uri(Name, Term, User) end.

validate_uri(Name, Term, User) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, P}    -> validate_params_user(P, User);
                  {error, E} -> {error, "\"~s\" not a valid URI: ~p", [Term, E]}
              end;
        E  -> E
    end;
validate_uri(Name, Term, User) ->
    case rabbit_parameter_validation:list(Name, Term) of
        ok -> case [V || URI <- Term,
                         V <- [validate_uri(Name, URI, User)],
                         element(1, V) =:= error] of
                  []      -> ok;
                  [E | _] -> E
              end;
        E  -> E
    end.

validate_params_user(#amqp_params_direct{}, none) ->
    ok;
validate_params_user(#amqp_params_direct{virtual_host = VHost},
                     User = #user{username = Username}) ->
    case rabbit_vhost:exists(VHost) andalso
        (catch rabbit_access_control:check_vhost_access(
                 User, VHost, undefined)) of
        ok -> ok;
        _  -> {error, "user \"~s\" may not connect to vhost \"~s\"",
                  [Username, VHost]}
    end;
validate_params_user(#amqp_params_network{}, _User) ->
    ok.

validate_delete_after(_Name, <<"never">>)          -> ok;
validate_delete_after(_Name, <<"queue-length">>)   -> ok;
validate_delete_after(_Name, N) when is_integer(N) -> ok;
validate_delete_after(Name,  Term) ->
    {error, "~s should be number, \"never\" or \"queue-length\", actually was "
     "~p", [Name, Term]}.

%% TODO headers?
validate_properties(Name, Term) ->
    Str = fun rabbit_parameter_validation:binary/2,
    Num = fun rabbit_parameter_validation:number/2,
    rabbit_parameter_validation:proplist(
      Name, [{<<"content_type">>,     Str, optional},
             {<<"content_encoding">>, Str, optional},
             {<<"delivery_mode">>,    Num, optional},
             {<<"priority">>,         Num, optional},
             {<<"correlation_id">>,   Str, optional},
             {<<"reply_to">>,         Str, optional},
             {<<"expiration">>,       Str, optional},
             {<<"message_id">>,       Str, optional},
             {<<"timestamp">>,        Num, optional},
             {<<"type">>,             Str, optional},
             {<<"user_id">>,          Str, optional},
             {<<"app_id">>,           Str, optional},
             {<<"cluster_id">>,       Str, optional}], Term).

validate_dest_type_fun(_Name, Term) ->
    {ok, Term0} = rabbit_vshovel_endpoint:ensure_protocol(Term),
    case rabbit_vshovel_config:get_endpoint_module(Term0) of
        M when is_atom(M) -> ok;
        _Error -> {error, "Unsupported protocol: \"~s\"", [Term]}
    end.

validate_dest_args_fun(CB) ->
    fun (Name, Term) -> validate_dest_args_fun(Name, Term, CB) end.

validate_dest_args_fun(_Name, Term, CB) ->
    CB:validate_arguments(Term).

validate_address_fun(User, rabbit_vshovel_worker) -> validate_uri_fun(User);

validate_address_fun(_User, CB) ->
    fun (Name, Term) -> validate_address_fun(Name, Term, CB) end.

validate_address_fun(_Name, Term, CB) ->
    CB:validate_address(Term).


%%----------------------------------------------------------------------------
parse({VHost, Name}, Def) ->
    SrcURIs  = get_uris(<<"src-uri">>,       Def),
    DestURIs = get_uris(<<"dest-uri">>,      Def),
    DestType = pget(<<"dest-type">>,         Def, <<"amqp">>),
    Version  = pget(<<"dest-vsn">>,          Def, <<"0.9.1">>),
    DestArgs = pget(<<"dest-args">>,         Def, <<"{}">>),
    SrcX     = pget(<<"src-exchange">>,      Def, none),
    SrcXKey  = pget(<<"src-exchange-key">>,  Def, <<>>), %% [1]
    SrcQ     = pget(<<"src-queue">>,         Def, none),
    DestX    = pget(<<"dest-exchange">>,     Def, none),
    DestXKey = pget(<<"dest-exchange-key">>, Def, none),
    DestQ    = pget(<<"dest-queue">>,        Def, none),
    %% [1] src-exchange-key is never ignored if src-exchange is set
    {SrcFun, Queue, Table1} =
        case SrcQ of
            none -> {fun (_Conn, Ch) ->
                             Ms = [#'queue.declare'{exclusive = true},
                                   #'queue.bind'{routing_key = SrcXKey,
                                                 exchange    = SrcX}],
                             [amqp_channel:call(Ch, M) || M <- Ms]
                     end, <<>>, [{<<"src-exchange">>,     SrcX},
                                 {<<"src-exchange-key">>, SrcXKey}]};
            _    -> {fun (Conn, _Ch) ->
                             ensure_queue(Conn, SrcQ)
                     end, SrcQ, [{<<"src-queue">>, SrcQ}]}
        end,

    {ok, DestType0} = rabbit_vshovel_endpoint:ensure_protocol(DestType),
    CB              = rabbit_vshovel_config:get_endpoint_module(DestType0, Version),

    {ok, DestArgs0} = 
        CB:validate_arguments(DestArgs),

    %% Update state
    VShovelStateInit = 
        #vshovel{
               sources            = #endpoint{address              = SrcURIs,
                                              resource_declaration = SrcFun},
               destinations       = #endpoint{protocol             = DestType0,
                                              address              = DestURIs,
                                              arguments            = DestArgs0},
               prefetch_count     = pget(<<"prefetch-count">>, Def, 1000),
               ack_mode           = translate_ack_mode(
                                      pget(<<"ack-mode">>, Def, <<"on-confirm">>)),
               queue              = Queue,
               reconnect_delay    = pget(<<"reconnect-delay">>, Def, 1),
               delete_after       = opt_b2a(pget(<<"delete-after">>, Def, <<"never">>))
    },

    %% If protocol is 'amqp', treat as current implementation
    {ok,  if DestType0 =:= ?AMQP_PROTOCOL ->
              DestFun = fun (Conn, _Ch) ->
                                case DestQ of
                                    none -> ok;
                                    _    -> ensure_queue(Conn, DestQ)
                                end
                        end,
              {X, Key} = case DestQ of
                             none -> {DestX, DestXKey};
                             _    -> {<<>>,  DestQ}
                         end,
              Table2 = [{K, V} || {K, V} <- [{<<"dest-exchange">>,     DestX},
                                             {<<"dest-exchange-key">>, DestXKey},
                                             {<<"dest-queue">>,        DestQ}],
                                  V =/= none],
              PubFun = fun (_SrcURI, _DestURI, P0) ->
                               P1 = case X of
                                        none -> P0;
                                        _    -> P0#'basic.publish'{exchange = X}
                                    end,
                               case Key of
                                   none -> P1;
                                   _    -> P1#'basic.publish'{routing_key = Key}
                               end
                       end,
              AddHeaders = pget(<<"add-forward-headers">>, Def, false),
              Table0 = [{<<"vshovelled-by">>, rabbit_nodes:cluster_name()},
                        {<<"vshovel-type">>,  <<"dynamic">>},
                        {<<"vshovel-name">>,  Name},
                        {<<"vshovel-vhost">>, VHost}],
              SetProps = lookup_indices(pget(<<"publish-properties">>, Def, []),
                                        record_info(fields, 'P_basic')),
              AddTimestampHeader = pget(<<"add-timestamp-header">>, Def, false),
              PubPropsFun = 
                  fun (SrcURI, DestURI, P0) ->
                        P  = set_properties(P0, SetProps),
                        P1 = case AddHeaders of
                            true  -> rabbit_vshovel_util:update_headers(
                                       Table0, Table1 ++ Table2,
                                       SrcURI, DestURI, P);
                            false -> P
                        end,
                        case AddTimestampHeader of
                            true  -> rabbit_vshovel_util:add_timestamp_header(P1);
                            false -> P1
                        end
                  end,
              VShovelStateInit#vshovel{
                 destinations       = #endpoint{address              = DestURIs,
                                                resource_declaration = DestFun},
                 publish_fields     = PubFun,
                 publish_properties = PubPropsFun
              };
        true -> 
            VShovelStateInit
        end}.

get_uris(Key, Def) ->
    URIs = case pget(Key, Def) of
               B when is_binary(B) -> [B];
               L when is_list(L)   -> L
           end,
    [binary_to_list(URI) || URI <- URIs].

translate_ack_mode(<<"on-confirm">>) -> on_confirm;
translate_ack_mode(<<"on-publish">>) -> on_publish;
translate_ack_mode(<<"no-ack">>)     -> no_ack.

ensure_queue(Conn, Queue) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                               passive = true})
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
            {ok, Ch2} = amqp_connection:open_channel(Conn),
            amqp_channel:call(Ch2, #'queue.declare'{queue   = Queue,
                                                   durable = true}),
            catch amqp_channel:close(Ch2)

    after
        catch amqp_channel:close(Ch)
    end.

opt_b2a(B) when is_binary(B) -> list_to_atom(binary_to_list(B));
opt_b2a(N)                   -> N.

set_properties(Props, []) ->
    Props;
set_properties(Props, [{Ix, V} | Rest]) ->
    set_properties(setelement(Ix, Props, V), Rest).

lookup_indices(KVs, L) ->
    [{1 + list_find(list_to_atom(binary_to_list(K)), L), V} || {K, V} <- KVs].

list_find(K, L) -> list_find(K, L, 1).

list_find(K, [K|_], N) -> N;
list_find(K, [],   _N) -> exit({not_found, K});
list_find(K, [_|L], N) -> list_find(K, L, N + 1).