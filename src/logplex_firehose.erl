%% Copyright (c) 2014 Alex Arnell <alex@heroku.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(logplex_firehose).
-behaviour(gen_server).

-include("logplex.hrl").
-include_lib("ex_uri/include/ex_uri.hrl").
-include("logplex_drain.hrl").
-include("logplex_logging.hrl").

-define(CHANNEL, {channel, ?MODULE}).

-export([post_msg/3]).
-export([where/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(pool, {next=1, size=0, pool={}}).
-record(state, {buf :: pid(),
                drain_pool :: #pool{}}).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, 
                          #state{}, []).

where() ->
    hd(logplex_channel:whereis(?CHANNEL)).

post_msg(ChannelId, TokenName, Msg)
  when is_integer(ChannelId),
       is_binary(TokenName),
       is_binary(Msg) ->
    logplex_stats:incr(#channel_stat{channel_id=?MODULE, key=channel_post}),
    gproc:send({p, l, ?CHANNEL}, {post, Msg}),
    ok;

post_msg(_ChannelId, _TokenName, _Msg) ->
    ok.

%%%--------------------------------------------------------------------
%%% gen_server callbacks
%%%--------------------------------------------------------------------

init(State0=#state{}) ->
    process_flag(trap_exit, true),
    ?INFO("at=spawn", []),
    State1 = start_firehose_channel(State0),
    State = start_firehose_drains(State1),
    {ok, State, hibernate}.

handle_call(Call, _From, State) ->
    ?WARN("Unexpected call ~p.", [Call]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?WARN("Unexpected cast ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info({mail, Buf, new_data}, S=#state{ buf=Buf, drain_pool=Drains }) ->
    pobox:active(Buf, fun filter_msg/2, Drains),
    {noreply, S};
handle_info({mail, Buf, Frame, Count, Lost}, S=#state{ buf=Buf, drain_pool=Drains0 }) ->
    Status={Dest, Drains} = next(Drains0),
    send(Dest, Buf, Frame, Count, Lost),
    log_stats(Status, Count, Lost),
    pobox:notify(Buf),
    {noreply, S#state{ drain_pool=Drains }};
handle_info(Info, State) ->
    ?WARN("Unexpected info ~p", [Info]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%--------------------------------------------------------------------
%%% internal functions
%%%--------------------------------------------------------------------

drain_urls() ->
    logplex_app:config(firehose_drain_urls, []).

start_firehose_channel(State0=#state{ buf=undefined }) ->
    {ok, Buf} = pobox:start_link(self(),
                     logplex_app:config(drain_buffer_size, 1024), queue),
    gproc:add_local_property(?CHANNEL, true),
    gproc:give_away({p, l, ?CHANNEL}, Buf),
    State0#state{ buf=Buf }.

start_firehose_drains(State0=#state{ drain_pool=undefined }) ->
    Drains = [ start_firehose_drain(Url) || Url <- drain_urls() ],
    DrainPool = #pool{ size=length(Drains), pool=list_to_tuple(Drains)},
    State0#state{ drain_pool=DrainPool }.

start_firehose_drain(Url) ->
    {ok, Id, Token} = logplex_drain:reserve_token(),
    Drain = logplex_drain:new(Id, ?MODULE, Token, http, Url),
    {ok, _} = logplex_drain:start(Drain),
    Drain.

filter_msg(_Msg, State=#pool{ size=0 }) ->
    {drop, State};
filter_msg(Msg, State) ->
    {{ok, Msg}, State}.

next(S=#pool{ size=0 }) ->
    {#drain{}, S};
next(#pool{ next=N, size=N, pool=P }) ->
    {erlang:element(N, P), #pool{next=1, size=N, pool=P}};
next(#pool{ next=N, size=S, pool=P }) ->
    {erlang:element(N, P), #pool{next=N+1, size=S, pool=P}}.

%% @private
send(#drain{ id=undefined }, _, _, _, _) ->
    ok;
send(#drain{ id=DrainId }, Buf, Frame, Count, Lost) ->
    gproc:send({n, l, {drain, DrainId}},
               {logplex_drain_buffer, Buf, {frame, Frame, Count, Lost}}),
    ok.

log_stats({#drain{ id=DrainId }, _}, Sent, Lost) ->
    logplex_stats:incr(#drain_stat{drain_id=DrainId,
                                   channel_id=?MODULE,
                                   key=drain_delivered}, Sent),
    logplex_stats:incr(#drain_stat{drain_id=DrainId,
                                   channel_id=?MODULE,
                                   key=drain_dropped}, Lost);
log_stats(_, _, _) ->
    ok.
