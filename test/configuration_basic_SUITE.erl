%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2010-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(configuration_basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(EXCHANGE,     <<"test_exchange">>).
-define(TO_VSHOVEL,   <<"to_the_vshovel">>).
-define(FROM_VSHOVEL, <<"from_the_vshovel">>).
-define(UNSHOVELLED,  <<"unshovelled">>).
-define(SHOVELLED,    <<"shovelled">>).
-define(TIMEOUT,      1000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          zero_vshovels,
          invalid_configuration,
          valid_configuration
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:rpc(Config2, 0,
      application, stop, [rabbitmq_vshovel]),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

zero_vshovels(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, zero_vshovels1, [Config]).

zero_vshovels1(_Config) ->
    %% vshovel can be started with zero vshovels configured
    ok = application:start(rabbitmq_vshovel),
    ok = application:stop(rabbitmq_vshovel),
    passed.

invalid_configuration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, invalid_configuration1, [Config]).

invalid_configuration1(_Config) ->
    %% various ways of breaking the config
    require_list_of_vshovel_configurations =
        test_broken_vshovel_configs(invalid_config),

    require_list_of_vshovel_configurations =
        test_broken_vshovel_configs([{test_vshovel, invalid_vshovel_config}]),

    Config = [{sources, [{broker, "amqp://"}]},
              {destinations, [{broker, "amqp://"}]},
              {queue, <<"">>}],

    {duplicate_vshovel_definition, test_vshovel} =
        test_broken_vshovel_configs(
          [{test_vshovel, Config}, {test_vshovel, Config}]),

    {invalid_parameters, [{invalid, invalid, invalid}]} =
        test_broken_vshovel_config([{invalid, invalid, invalid} | Config]),

    {duplicate_parameters, [queue]} =
        test_broken_vshovel_config([{queue, <<"">>} | Config]),

    {missing_parameters, Missing} =
        test_broken_vshovel_config([]),
    [destinations, queue, sources] = lists:sort(Missing),

    {unrecognised_parameters, [invalid]} =
        test_broken_vshovel_config([{invalid, invalid} | Config]),

    {require_list, invalid} =
        test_broken_vshovel_sources(invalid),

    {missing_endpoint_parameter, broker_or_brokers} =
        test_broken_vshovel_sources([]),

    {expected_list, brokers, invalid} =
        test_broken_vshovel_sources([{brokers, invalid}]),

    {expected_string_uri, 42} =
        test_broken_vshovel_sources([{brokers, [42]}]),

    {{unexpected_uri_scheme, "invalid"}, "invalid://"} =
        test_broken_vshovel_sources([{broker, "invalid://"}]),

    {{unable_to_parse_uri, no_scheme}, "invalid"} =
        test_broken_vshovel_sources([{broker, "invalid"}]),

    {expected_list,declarations, invalid} =
        test_broken_vshovel_sources([{broker, "amqp://"},
                                     {arguments,
                                         [{declarations, invalid}]}]),
    {unknown_method_name, 42} =
        test_broken_vshovel_sources([{broker, "amqp://"},
                                     {arguments,
                                         [{declarations, [42]}]}]),

    {expected_method_field_list, 'queue.declare', 42} =
        test_broken_vshovel_sources([{broker, "amqp://"},
                                     {arguments, 
                                         [{declarations, [{'queue.declare', 42}]}]}]),

    {unknown_fields, 'queue.declare', [invalid]} =
        test_broken_vshovel_sources(
          [{broker, "amqp://"},
           {arguments, [{declarations, [{'queue.declare', [invalid]}]}]}]),

    {{invalid_amqp_params_parameter, heartbeat, "text",
      [{"heartbeat", "text"}], {not_an_integer, "text"}}, _} =
        test_broken_vshovel_sources(
          [{broker, "amqp://localhost/?heartbeat=text"}]),

    {{invalid_amqp_params_parameter, username, "text",
      [{"username", "text"}],
      {parameter_unconfigurable_in_query, username, "text"}}, _} =
        test_broken_vshovel_sources([{broker, "amqp://?username=text"}]),

    {invalid_parameter_value, prefetch_count,
     {require_non_negative_integer, invalid}} =
        test_broken_vshovel_config([{prefetch_count, invalid} | Config]),

    {invalid_parameter_value, ack_mode,
     {ack_mode_value_requires_one_of,
      {no_ack, on_publish, on_confirm}, invalid}} =
        test_broken_vshovel_config([{ack_mode, invalid} | Config]),

    {invalid_parameter_value, queue,
     {require_binary, invalid}} =
        test_broken_vshovel_config([{sources, [{broker, "amqp://"}]},
                                   {destinations, [{broker, "amqp://"}]},
                                   {queue, invalid}]),

    {invalid_parameter_value, publish_properties,
     {require_list, invalid}} =
        test_broken_vshovel_config([{publish_properties, invalid} | Config]),

    {invalid_parameter_value, publish_properties,
     {unexpected_fields, [invalid], _}} =
        test_broken_vshovel_config([{publish_properties, [invalid]} | Config]),

    {{invalid_ssl_parameter, fail_if_no_peer_cert, "42", _,
      {require_boolean, '42'}}, _} =
        test_broken_vshovel_sources([{broker, "amqps://username:password@host:5673/vhost?cacertfile=/path/to/cacert.pem&certfile=/path/to/certfile.pem&keyfile=/path/to/keyfile.pem&verify=verify_peer&fail_if_no_peer_cert=42"}]),

    passed.

test_broken_vshovel_configs(Configs) ->
    application:set_env(rabbitmq_vshovel, vshovels, Configs),
    {error, {Error, _}} = application:start(rabbitmq_vshovel),
    Error.

test_broken_vshovel_config(Config) ->
    {invalid_vshovel_configuration, test_vshovel, Error} =
        test_broken_vshovel_configs([{test_vshovel, Config}]),
    Error.

test_broken_vshovel_sources(Sources) ->
    {invalid_parameter_value, sources, Error} =
        test_broken_vshovel_config([{sources, Sources},
                                   {destinations, [{broker, "amqp://"}]},
                                   {queue, <<"">>}]),
    Error.

valid_configuration(Config) ->
    ok = setup_vshovels(Config),

    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Chan, #'queue.declare' { exclusive = true }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?FROM_VSHOVEL }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' { queue = Q, exchange = ?EXCHANGE,
                                                routing_key = ?TO_VSHOVEL }),

    #'basic.consume_ok'{ consumer_tag = CTag } =
        amqp_channel:subscribe(Chan,
                               #'basic.consume' { queue = Q, exclusive = true },
                               self()),
    receive
        #'basic.consume_ok'{ consumer_tag = CTag } -> ok
    after ?TIMEOUT -> throw(timeout_waiting_for_consume_ok)
    end,

    ok = amqp_channel:call(Chan,
                           #'basic.publish' { exchange    = ?EXCHANGE,
                                              routing_key = ?TO_VSHOVEL },
                           #amqp_msg { payload = <<42>>,
                                       props   = #'P_basic' {
                                         delivery_mode = 2,
                                         content_type  = ?UNSHOVELLED }
                                     }),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag,
                            routing_key = ?FROM_VSHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' { delivery_mode = 2,
                                            content_type  = ?SHOVELLED,
                                            headers       = [{<<"x-vshovelled">>,
                                                              _, _}]}
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag })
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver1)
    end,

    [{test_vshovel, static, {running, _Info}, _Time}] =
        rabbit_ct_broker_helpers:rpc(Config, 0,
          rabbit_vshovel_status, status, []),

    receive
        {#'basic.deliver' { consumer_tag = CTag, delivery_tag = AckTag1,
                            routing_key = ?TO_VSHOVEL },
         #amqp_msg { payload = <<42>>,
                     props   = #'P_basic' { delivery_mode = 2,
                                            content_type  = ?UNSHOVELLED }
                   }} ->
            ok = amqp_channel:call(Chan, #'basic.ack'{ delivery_tag = AckTag1 })
    after ?TIMEOUT -> throw(timeout_waiting_for_deliver2)
    end,

    rabbit_ct_client_helpers:close_channel(Chan).

setup_vshovels(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_vshovels1, [Config]).

setup_vshovels1(Config) ->
    Hostname = ?config(rmq_hostname, Config),
    TcpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0,
      tcp_port_amqp),
    %% a working config
    application:set_env(
      rabbitmq_vshovel,
      vshovels,
      [{test_vshovel,
        [{sources,
          [{broker, rabbit_misc:format("amqp://~s:~b/%2f?heartbeat=5",
                                       [Hostname, TcpPort])},
           {arguments, [{declarations,
                          [{'queue.declare',    [exclusive, auto_delete]},
                           {'exchange.declare', [{exchange, ?EXCHANGE}, auto_delete]},
                           {'queue.bind',       [{queue, <<>>}, {exchange, ?EXCHANGE},
                                                 {routing_key, ?TO_VSHOVEL}]}]}
            ]}]},
         {destinations,
          [{broker, rabbit_misc:format("amqp://~s:~b/%2f",
                                       [Hostname, TcpPort])}]},
         {queue, <<>>},
         {ack_mode, on_confirm},
         {publish_fields, [{exchange, ?EXCHANGE}, {routing_key, ?FROM_VSHOVEL}]},
         {publish_properties, [{delivery_mode, 2},
                               {cluster_id,    <<"my-cluster">>},
                               {content_type,  ?SHOVELLED}]},
         {add_forward_headers, true}
        ]}],
      infinity),

    ok = application:start(rabbitmq_vshovel),
    await_running_vshovel(test_vshovel).

await_running_vshovel(Name) ->
    case [N || {N, _, {running, _}, _}
                      <- rabbit_vshovel_status:status(),
                         N =:= Name] of
        [_] -> ok;
        _   -> timer:sleep(100),
               await_running_vshovel(Name)
    end.
