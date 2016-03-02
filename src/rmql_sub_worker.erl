-module(rmql_sub_worker).
-behaviour(gen_server).

-include("rmql.hrl").

%% API
-export([start_link/1]).

-ignore_xref([{start_link, 2}]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-record('DOWN', {
    ref :: reference(),
    type = process :: process,
    object :: pid(),
    info :: term() | noproc | noconnection
}).

-record(st, {
    host :: string(),
    port :: pos_integer(),
    username :: binary(),
    password :: binary(),
    vhost :: binary(),
    queue :: binary(),
    exchange :: binary(),
    prefetch_size :: pos_integer(),
    conn :: pid(),
    channel :: pid(),
    channel_tag,
    workers = [] :: list(),
    mod_fun :: {atom(), atom()},
    reconn_interval :: integer(),
    is_shutdown = false :: boolean()
}).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link(Attrs :: list()) -> {ok, pid()}.
start_link(Attrs) ->
    gen_server:start_link(?MODULE, Attrs, []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init(Args) ->

    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    VHost = proplists:get_value(vhost, Args),
    Queue = get_queue(proplists:get_value(queue, Args)),
    Exchange = proplists:get_value(exchange, Args),
    PrefSize = proplists:get_value(prefetch_size, Args),
    ModFun = proplists:get_value(handle_modfun, Args),
    ReconnectInterval = proplists:get_value(reconn_interval, Args),

    ?LOG_INFO("Start RMQ consumer: ~p:~p ~p~n", [Host, Port, Queue]),

    ok = schedule(open_connection, 0),

    St =
        #st{
            host = Host,
            port = Port,
            username = Username,
            password = Password,
            vhost = VHost,
            queue = Queue,
            exchange = Exchange,
            prefetch_size = PrefSize,
            reconn_interval = ReconnectInterval,
            mod_fun = ModFun
        },

    {ok, St}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    ?LOG_INFO("RMQ consumer got unknown msg: ~p~n", [_Msg]),
    {noreply, State}.

handle_info(open_connection, #st{conn = undefined, is_shutdown = false,
    host = Host, port = Port, username = Username, password = Password, vhost = VHost} = St) ->

    NewSt =
        case catch rmql:open_connection(Host, Port, Username, Password, VHost) of
            {ok, Conn} ->
                ?LOG_INFO("RMQ consumer open connection: ~p~n", [Conn]),
                _MonConnRef = erlang:monitor(process, Conn),
                ok = schedule(open_channel, 0),
                St#st{conn = Conn};
            _ConnError ->
                St
        end,
    {noreply, NewSt};
handle_info(open_channel, #st{conn = undefined, is_shutdown = false} = St) ->
    ok = schedule(open_connection, 0),
    {noreply, St};
handle_info(open_channel, #st{conn = Conn, channel = undefined, queue = Queue, exchange = Exchange,
    is_shutdown = false} = St) ->
    NewSt =
        case rmql:open_channel(Conn) of
            {ok, Channel} ->
                ?LOG_INFO("RMQ consumer open channel: ~p~n", [Channel]),
                try
                    _MonChannelRef = erlang:monitor(process, Channel),
%                    ok = rmql:declare_exchange(Channel, ?DOWNSTREAM_EXCHANGE, direct, true),
                    ok = rmql:declare_queue(Channel, Queue, true, false, false),
                    ok = rmql:bind_queue(Channel, Queue, Exchange, Queue),
                    ok = rmql:basic_qos(Channel, St#st.prefetch_size),

                    {ok, Tag} = rmql:basic_consume(Channel, Queue, false),
                    ?LOG_INFO("RMQ consumer subscribed to queue ~p: ~p~n", [Queue, Tag]),

                    St#st{channel = Channel, channel_tag = Tag}
                catch
                    _Exp:_Reason ->
                        ?LOG_INFO("Channel error ~p: ~p~n", [_Exp, _Reason]),
                        catch exit(Channel, kill),
                        St
                end;
            _ConnError ->
                St
        end,
    {noreply, NewSt};
handle_info(#'DOWN'{object = Pid} = Down, St = #st{conn = Pid, is_shutdown = false,
    host = Host, port = Port, queue = Queue, reconn_interval = ReconnInterval}) ->
    ?LOG_ERROR("RabbitMQ connection got down ~p:~p, queue ~p~n~p~n", [Host, Port, Queue, Down#'DOWN'.info]),
    ok = schedule(open_connection, ReconnInterval),
    {noreply, St#st{conn = undefined, channel = undefined, channel_tag = undefined, workers = []}};
handle_info(#'DOWN'{object = Pid}, St = #st{conn = Pid, is_shutdown = true}) ->
    {stop, rmq_conn_down, St#st{channel = undefined}};
handle_info(#'DOWN'{object = Pid} = Down, St = #st{channel = Pid, is_shutdown = false,
    host = Host, port = Port, queue = Queue, reconn_interval = ReconnInterval}) ->
    ?LOG_ERROR("RabbitMQ channel got down ~p:~p, queue ~p~n~p~n", [Host, Port, Queue, Down#'DOWN'.info]),
    ok = schedule(open_channel, ReconnInterval),
    {noreply, St#st{channel = undefined, channel_tag = undefined, workers = []}};
handle_info(#'DOWN'{object = Pid}, St = #st{conn = Pid, channel = Pid, is_shutdown = true}) ->
    ok = rmql:close_connection(Pid),
    {stop, rmq_channel_down, St#st{channel = undefined}};
%% Pid is nor connection nor channel so it's a spawned worker
handle_info(#'DOWN'{object = Pid}, St = #st{workers = Workers}) ->
    {noreply, St#st{workers = lists:delete(Pid, Workers)}};
% Message From RabbitMQ
handle_info(Msg, #st{workers = Workers, mod_fun = ModFun, exchange = Exchange} = State) ->
    NewWorkers =
        case rmql:get_tag_payload(Msg) of
            {Tag, Payload} ->
                Pid = spawn(fun() ->

                    try
                        handle(Payload, Exchange, ModFun)
                    after
                        ack(State, Tag)
                    end

                end),

                _Ref = monitor(process, Pid),
                [Pid | Workers];
            error ->
                #st{host = Host, port = Port, queue = Queue} = State,
                ?LOG_ERROR("Undefined msg in rabbitmq consumer ~p:~p, queue ~p ~p~n", [Host, Port, Queue, Msg]),
                Workers
        end,
    {noreply, State#st{workers = NewWorkers}}.

terminate(_Reason, #st{conn = Conn, channel = Channel}) ->
    ok = close_channel(Channel),
    ok = close_connection(Conn).

code_change(_OldVsn, State, _Extra) ->
    State.

%% ===================================================================
%% Internals
%% ===================================================================

close_channel(Pid) when is_pid(Pid) ->
    rmql:close_channel(Pid);
close_channel(_Pid) ->
    ok.

close_connection(Pid) when is_pid(Pid) ->
    rmql:close_connection(Pid);
close_connection(_Pid) ->
    ok.

schedule(Action, 0) ->
    self() ! Action,
    ok;
schedule(Action, Time) ->
    erlang:send_after(Time, self(), Action).

handle(Payload, Exchange, {Module, Function}) when is_binary(Payload) ->
    try
        erlang:apply(Module, Function, [Payload])
    catch
        HandleExp:HandleReason ->
            ?LOG_ERROR("Crash handle fun ~p ~p ~p ~p~n", [HandleExp, HandleReason, Payload, erlang:get_stacktrace()]),
            error
    end;
handle(Payload, _, _) ->
    ?LOG_ERROR("Bad request: ~p", [Payload]).

ack(St, Tag) ->
    ok = rmql:basic_ack(St#st.channel, Tag).

get_queue({Module, Function, Args}) ->
    erlang:apply(Module, Function, Args);
get_queue(QueueName) ->
    QueueName.
