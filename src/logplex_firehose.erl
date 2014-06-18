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

-define(APP, logplex).
-define(FIREHOSE_FILTER_TAB, firehose_filters).

-export([post_msg/3]).

post_msg(ChannelId, <<"heroku">>, Msg)
  when is_integer(ChannelId),
       is_binary(Msg) ->
    logplex_stats:incr(firehose_received),
    case logplex_app:config(firehose_channel_id, undefined) of
        undefined -> ok; % do nothing
        ChannelId -> ok; % do nothing
        FirehoseId ->
            logplex_channel:post_msg({channel, FirehoseId}, Msg)
    end;

post_msg(_ChannelId, _TokenName, _Msg) ->
    ok.
