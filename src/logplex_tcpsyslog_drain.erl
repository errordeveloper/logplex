%%%-------------------------------------------------------------------
%% @copyright Geoff Cant
%% @author Geoff Cant <nem@erlang.geek.nz>
%% @version {@vsn}, {@date} {@time}
%% @doc
%% @end
%%%-------------------------------------------------------------------
-module(logplex_tcpsyslog_drain).

-behaviour(gen_server).

-include("logplex.hrl").
-include("logplex_logging.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/3
         ,post_msg/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id :: binary(),
                host :: string() | inet:ip_address(),
                port :: inet:port_number(),
                sock = undefined :: 'undefined' | inet:socket(),
                %% Buffer for messages while disconnected
                buf = logplex_drain_buffer:new() :: logplex_drain_buffer:buf(),
                %% Last time we connected or successfully sent data
                last_good_time :: 'undefined' | erlang:timestamp(),
                %% TCP failures since last_good_time
                failures = 0 :: non_neg_integer(),
                %% Reconnect timer reference
                tref = undefined :: 'undefined' | reference()
               }).

-define(RECONNECT_MSG, reconnect).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end
%%--------------------------------------------------------------------
start_link(DrainID, Host, Port) ->
    gen_server:start_link(?MODULE,
                          [#state{id=DrainID,
                                  host=Host,
                                  port=Port}],
                          []).

post_msg(Server, Msg) when is_tuple(Msg) ->
    gen_server:cast(Server, {post, Msg}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([State0 = #state{}]) ->
    self() ! ?RECONNECT_MSG,
    {ok, State0}.

%% @private
handle_call(Call, _From, State) ->
    ?WARN("Unexpected call ~p.", [Call]),
    {noreply, State}.

%% @private
handle_cast({post, Msg}, State = #state{sock=undefined,
                                        buf=Buf}) ->
    NewBuf = logplex_drain_buffer:push(Msg, Buf),
    {noreply, State#state{buf=NewBuf}};

handle_cast({post, Msg}, State = #state{sock=S}) ->
    case post(Msg, S) of
        ok ->
            {noreply, tcp_good(State)};
        {error, Reason} ->
            ?ERR("~p (~p:~p) Couldn't write syslog message: ~p",
                 [State#state.id, State#state.host, State#state.port,
                  Reason]),
            {noreply, reconnect(tcp_error, State)}
    end;

handle_cast(Msg, State) ->
    ?WARN("Unexpected cast ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(?RECONNECT_MSG, State = #state{sock=undefined}) ->
    case connect(State) of
        {ok, Sock} ->
            {noreply, tcp_good(State#state{sock=Sock})};
        {error, Reason} ->
            NewState = tcp_bad(State#state{sock=undefined}),
            ?ERR("~p Couldn't connect to ~p:~p; ~p"
                 " (try ~p, last success: ~s)",
                 [NewState#state.id, NewState#state.host, NewState#state.port,
                  Reason, NewState#state.failures, time_failed(NewState)]),
            {noreply, reconnect(tcp_error, NewState)}
    end;

handle_info(Info, State) ->
    ?WARN("Unexpected info ~p", [Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-spec post(logplex_syslog_utils:syslog_msg(), inet:socket()) ->
                  'ok' |
                  {'error', term()}.
post(Msg, Sock) ->
    SyslogMsg = logplex_syslog_utils:to_msg(Msg),
    Packet = logplex_syslog_utils:frame(SyslogMsg),
    gen_tcp:send(Sock, Packet).

connect(#state{sock = undefined, host=Host, port=Port}) ->
    SendTimeoutS = logplex_app:config(tcp_syslog_send_timeout_secs),
    gen_tcp:connect(Host, Port, [binary
                                 %% We don't expect data, but why not.
                                 ,{active, true}
                                 ,{exit_on_close, true}
                                 ,{keepalive, true}
                                 ,{packet, raw}
                                 ,{reuseaddr, true}
                                 ,{send_timeout,
                                   timer:seconds(SendTimeoutS)}
                                 ,{send_timeout_close, true}
                                 ]).

-spec reconnect(Why::atom(), #state{}) -> #state{}.
reconnect(_Reason, State = #state{failures = F}) ->
    BackOff = erlang:min(logplex_app:config(tcp_syslog_backoff_max),
                         1 bsl F),
    Ref = erlang:start_timer(timer:seconds(BackOff), self(), ?RECONNECT_MSG),
    State#state{tref=Ref}.

tcp_good(State = #state{}) ->
    State#state{last_good_time = os:timestamp(),
                failures = 0}.

tcp_bad(State = #state{failures = F}) ->
    State#state{failures = F + 1}.

-spec time_failed(#state{}) -> iolist().
time_failed(State = #state{}) ->
    time_failed(os:timestamp(), State).
time_failed(Now, #state{last_good_time=T0})
  when is_tuple(T0) ->
    io_lib:format("~f ago", [timer:now_diff(T0, Now) / 1000000]);
time_failed(_, #state{last_good_time=undefined}) ->
    "never".

