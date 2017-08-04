-define(VSHOVEL_USER,           <<"rmq-vshovel">>).
-define(VSHOVEL_VERSION,        "1.0.0").

-define(AMQP_PROTOCOL,          amqp).
-define(HTTP_PROTOCOL,          http).
-define(DEFAULT_AMQP_VERSION,   "0.9.1").

-define(TO_ATOM(V),     rabbit_vshovel_config:to_atom(V)).
-define(TO_LIST(V),     rabbit_vshovel_config:to_list(V)).
-define(TO_INTEGER(V),  rabbit_vshovel_config:to_integer(V)).
-define(TO_BINARY(V),   rabbit_vshovel_config:to_binary(V)).

-record(endpoint,
        {protocol = ?AMQP_PROTOCOL,
         address,
         resource_declaration,
         arguments,
         state
        }).

-record(vshovel,
        {sources,
         destinations,
         prefetch_count,
         ack_mode,
         publish_fields,
         publish_properties,
         queue,
         reconnect_delay,  %% move later
         send_mode,
         name,
         delete_after = 'never',
         dest_mod,
         dest_state
        }).

-record(protocol,
        {mod,
         state,
         args
        }).

-type endpoint_state()   :: term().

-type vshovel_type()     :: 'static' | 'dynamic'.
-type vshovel_record()   :: #vshovel{}.
-type vshovel_result()   :: 'ok' | {'error', term()}.
-type vshovel_protocol() :: atom().
-type vshovel_error()    :: {'error', term()}.
-type vshovel_address()  :: term().
-type vshovel_arguments():: [term()].
