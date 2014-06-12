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

-export([is_filtered/2,
         filter_token/1,
         filter_tokens/1,
         lookup_firehose/0,
         create_ets_table/0,
         process_msg/3]).

filter_token(TokenId) when is_binary(TokenId) ->
    case logplex_token:lookup(TokenId) of
        undefined -> false;
        Token -> filter_token(Token)
    end;

filter_token(Token) when is_record(Token, token) ->
    ets:insert(?FIREHOSE_FILTER_TAB, Token).

filter_tokens(Tokens) when is_list(Tokens) ->
    [ filter_token(Token) || Token <- Tokens ].

is_filtered(_ChannelId, Token) ->
    not ets:member(?FIREHOSE_FILTER_TAB, Token).

lookup_firehose() ->
    case logplex_app:config(firehose_channel_id, undefined) of
        undefined -> undefined;
        FirehoseId ->
            {channel, FirehoseId}
    end.

process_msg(ChannelId, Token, Msg)
  when is_integer(ChannelId),
       is_binary(Token),
       is_binary(Msg) ->
    case is_filtered(ChannelId, Token) of
        true -> ok;
        false -> post_msg(Msg)
    end.

create_ets_table() ->
    ets:new(?FIREHOSE_FILTER_TAB, [named_table, public, set, {keypos, #token.id}]).

%%% private

post_msg(Msg) ->
    case lookup_firehose() of
        undefined -> ok; % do nothing
        Firehose ->
            logplex_channel:post_msg(Firehose, Msg)
    end.
            
