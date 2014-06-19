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
-include("logplex_logging.hrl").

-define(APP, logplex).
-define(CHANNEL_GROUP, {channel_group, ?MODULE}).

-export([create_channel_group/0,
         update_firehose_channels/0,
         is_firehose_channel/1,
         id_to_channel/1,
         register_channel/1,
         unregister_channel/1,
         post_msg/3]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

update_firehose_channels() ->
    Ids = logplex_app:config(firehose_channel_ids, []),
    update_firehose_channels(Ids).

id_to_channel({channel, Id}=Chan) when is_integer(Id) ->
    Chan;
id_to_channel(Id) when is_list(Id) ->
    id_to_channel(list_to_integer(Id));
id_to_channel(Id) when is_integer(Id) ->
    {channel, Id}.


is_firehose_channel({channel, Id}) ->
    List = logplex_app:config(firehose_channel_ids, []),
    in_list(Id, List).

register_channel({channel, _Id}=Channel) ->
    case is_firehose_channel(Channel) of
        false -> ok; % ignore
        true ->
            logplex_channel_group:join(?CHANNEL_GROUP, Channel)
    end.

unregister_channel({channel, _Id}=Channel) ->
    catch logplex_channel_group:unregister(?CHANNEL_GROUP, Channel).

post_msg(ChannelId, <<"heroku">>, Msg)
  when is_integer(ChannelId),
       is_binary(Msg) ->
    logplex_channel_group:post_msg(?CHANNEL_GROUP, Msg);

post_msg(_ChannelId, _TokenName, _Msg) ->
    ok.

%%%--------------------------------------------------------------------
%%% gen_server callbacks
%%%--------------------------------------------------------------------

init([]) ->
    create_channel_group(),
    update_firehose_channels(),
    {ok, []}.

handle_call(Call, _From, State) ->
    ?WARN("Unexpected call ~p.", [Call]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?WARN("Unexpected cast ~p", [Msg]),
    {noreply, State}.

%% @private
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

create_channel_group() ->
    logplex_channel_group:new(?CHANNEL_GROUP).

update_firehose_channels([]) ->
    [];
update_firehose_channels(Ids) ->
    Channels = lists:map(fun id_to_channel/1, Ids),
    [ logplex_channel:join_group(Channel, ?CHANNEL_GROUP) || Channel <- Channels].

in_list(_Id, []) ->
    false;
in_list(Id, [{channel, Id} | _Rest]) ->
    true;
in_list(Id, [_Hd | Rest]) ->
    in_list(Id, Rest).
