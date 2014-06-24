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

-include("logplex.hrl").
-include("logplex_logging.hrl").

-define(TAB, firehose).

-export([new/0,
         new/1,
         register_channel/1,
         unregister_channel/1,
         next_firehose/1,
         post_msg/3]).

-record(firehose, {idx :: integer(),
                   channel_id :: integer()}).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

new() ->
    new([]).

new(ChannelIds) when is_list(ChannelIds) ->
    ets:new(?TAB, [named_table, public, set,
                   {keypos, #firehose.idx},
                   {read_concurrency, true}]),
    [register_channel(Id) || Id <- ChannelIds ].

register_channel(ChannelId) when is_integer(ChannelId) ->
    ?INFO("channel_id=~p at=spawn", [ChannelId]),
    Idx = ets:info(?TAB, size),
    ets:insert(?TAB, #firehose{ idx=Idx, channel_id=ChannelId }).

unregister_channel(ChannelId) when is_integer(ChannelId) ->
    List = ets:tab2list(?TAB),
    ets:delete_all_objects(?TAB),
    [ register_channel(Id) || #firehose{ channel_id=Id } <- List, Id =/= ChannelId ],
    ok.

post_msg(ChannelId, TokenName, Msg)
  when is_integer(ChannelId),
       is_binary(TokenName),
       is_binary(Msg) ->
    case ets:lookup(?TAB, next_firehose(ets:info(?TAB, size))) of
        [#firehose{ channel_id=FirehoseId }] ->
            logplex_stats:incr(#firehose_stat{channel_id=FirehoseId, key=firehose_post}),
            logplex_channel:post_msg({channel, FirehoseId}, Msg);
        _ -> ok % ignored
    end.

next_firehose(Num) when Num > 0 ->
    erlang:phash2({erlang:make_ref(), self()}, Num);
next_firehose(_) ->
    0.
