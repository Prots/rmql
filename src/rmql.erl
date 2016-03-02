-module(rmql).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([
    open_connection/5,
    open_connection/6,
    close_connection/1,

    open_channel/1,
    close_channel/1,

    declare_queue/5,
    declare_queue/6,
    bind_queue/3,
    bind_queue/4,

    declare_exchange/4,

    basic_qos/2,
    basic_consume/2,
    basic_consume/3,
    basic_publish/4,
    basic_publish/5,
    basic_ack/2,
    get_tag_payload/1
]).

-type exch_type() :: direct | fanout | topic.

-spec open_connection(string(), pos_integer(), binary(), binary(), binary()) ->
    {'ok', pid()} | {'error', any()}.
open_connection(Host, Port, Username, Password, VHost) ->
    AMQPNetworkParams = #amqp_params_network{
        host = Host,
        port = Port,
        username = Username,
        password = Password,
        virtual_host = VHost
    },
    amqp_connection:start(AMQPNetworkParams).


-spec open_connection(string(), pos_integer(), binary(), binary(), binary(), pos_integer()) ->
    {'ok', pid()} | {'error', any()}.
open_connection(Host, Port, Username, Password, VHost, Timeout) ->
    AMQPNetworkParams = #amqp_params_network{
        host = Host,
        port = Port,
        username = Username,
        password = Password,
        virtual_host = VHost
    },
    Pid = self(),
    %% Connect with timeout
    spawn(fun() ->
        Result = amqp_connection:start(AMQPNetworkParams),
        Pid ! {amqp_connection, Result}
    end),
    receive
        {amqp_connection, Result} -> Result
    after
        Timeout -> {error, timeout}
    end.


-spec declare_queue(pid(), binary(), boolean(), boolean(), boolean()) ->
    'ok' | {'error', any()}.
declare_queue(Chan, Queue, Durable, Exclusive, AutoDelete) ->
    declare_queue(Chan, Queue, Durable, Exclusive, AutoDelete, []).


-spec declare_queue(pid(), binary(), boolean(), boolean(), boolean(), list()) ->
    'ok' | {'error', any()}.
declare_queue(Chan, Queue, Durable, Exclusive, AutoDelete, Args) ->

    Method = #'queue.declare'{queue = Queue,
        durable = Durable,
        exclusive = Exclusive,
        auto_delete = AutoDelete,
        arguments = Args},
    try amqp_channel:call(Chan, Method) of
        #'queue.declare_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec declare_exchange(pid(), binary(), exch_type(), boolean()) -> 'ok' | {'error', any()}.
declare_exchange(Channel, Exchange, Type, Durable) ->
    XDeclare = #'exchange.declare'{exchange = Exchange,
        durable = Durable,
        type = atom_to_binary(Type, utf8)},
    try amqp_channel:call(Channel, XDeclare) of
        #'exchange.declare_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

bind_queue(Chan, Queue, Exchange) ->
    QBind = #'queue.bind'{queue = Queue, exchange = Exchange},
    try amqp_channel:call(Chan, QBind) of
        #'queue.bind_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec bind_queue(pid(), binary(), binary(), binary()) ->
    'ok' | {'error', any()}.
bind_queue(Chan, Queue, Exchange, RoutingKey) ->
    QBind = #'queue.bind'{queue = Queue, exchange = Exchange, routing_key = RoutingKey},
    try amqp_channel:call(Chan, QBind) of
        #'queue.bind_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_qos(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
basic_qos(Chan, PrefetchCount) ->
    Method = #'basic.qos'{prefetch_count = PrefetchCount},
    try amqp_channel:call(Chan, Method) of
        #'basic.qos_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_consume(pid(), binary(), boolean()) ->
    {'ok', binary()} | {'error', any()}.
basic_consume(Chan, Queue, NoAck) ->
    basic_consume(Chan, #'basic.consume'{queue = Queue, no_ack = NoAck}).

-spec basic_consume(pid(), #'basic.consume'{}) ->
    {'ok', binary()} | {'error', any()}.
basic_consume(Chan, BasicConsume = #'basic.consume'{}) ->
    try
        amqp_channel:subscribe(Chan, BasicConsume, self()),
        receive
            #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
                {ok, ConsumerTag}
        after
            5000 -> {error, timeout}
        end
    catch
        _:Reason -> {error, Reason}
    end.

-spec open_channel(pid()) -> {'ok', pid()} | {'error', any()}.
open_channel(Conn) ->
    case catch amqp_connection:open_channel(Conn) of
        {ok, _Chan} = Res -> Res;
        Error -> {error, Error}
    end.

-spec close_connection(pid()) -> 'ok'.
close_connection(Conn) ->
    catch (amqp_connection:close(Conn)),
    ok.

-spec close_channel(pid()) -> 'ok'.
close_channel(Chan) ->
    catch (amqp_channel:close(Chan)),
    ok.

-spec basic_publish(pid(), binary(), binary(), #'P_basic'{}) ->
    'ok' | {'error', any()}.
basic_publish(Chan, RoutingKey, Payload, Props = #'P_basic'{}) ->
    Method = #'basic.publish'{routing_key = RoutingKey},
    Content = #amqp_msg{payload = Payload, props = Props},
    try amqp_channel:call(Chan, Method, Content) of
        ok -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end;
basic_publish(Chan, RoutingKey, Payload, PropList) ->
    Props = prepare_basic_props(PropList),
    basic_publish(Chan, RoutingKey, Payload, Props).

basic_publish(Chan, Exchange, RoutingKey, Payload, Props = #'P_basic'{}) ->
    Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Content = #amqp_msg{payload = Payload, props = Props},
    try amqp_channel:call(Chan, Method, Content) of
        ok -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end;
basic_publish(Chan, Exchange, RoutingKey, Payload, PropList) ->
    Props = prepare_basic_props(PropList),
    basic_publish(Chan, Exchange, RoutingKey, Payload, Props).

-spec basic_ack(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
basic_ack(Chan, DeliveryTag) ->
    Method = #'basic.ack'{delivery_tag = DeliveryTag},
    try amqp_channel:call(Chan, Method) of
        ok -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

prepare_basic_props(Props) ->
    #'P_basic'{
        message_id = proplists:get_value(message_id, Props),
        correlation_id = proplists:get_value(correlation_id, Props),
        content_type = proplists:get_value(content_type, Props),
        content_encoding = proplists:get_value(content_encoding, Props),
        delivery_mode = proplists:get_value(delivery_mode, Props),
        reply_to = proplists:get_value(reply_to, Props),
        expiration = proplists:get_value(expiration, Props),
        timestamp = proplists:get_value(timestamp, Props),
        app_id = proplists:get_value(app_id, Props),
        headers = proplists:get_value(headers, Props),
        priority = proplists:get_value(priority, Props),
        type = proplists:get_value(type, Props),
        user_id = proplists:get_value(user_id, Props),
        cluster_id = proplists:get_value(cluster_id, Props)
    }.

get_tag_payload({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Payload}}) ->
    {Tag, Payload};
get_tag_payload(_) ->
    error.
