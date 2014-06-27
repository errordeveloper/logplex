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
%% OF MERCHANLOOKUP_TABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(logplex_firehose).

-include("logplex.hrl").
-include("logplex_logging.hrl").

-define(LOOKUP_TAB, firehose_master).
-define(WORKER_TAB, firehose_workers).
-define(MASTER_KEY, master_shard).

-export([post_msg/3]).

-export([create_ets_table/0,
         lookup_shard/1,
         next_shard/1,
         read_and_store_master_info/0]).

-export([next_hash/1]).

-record(shard_pool, {key :: ?MASTER_KEY | logplex_channel:id(),
                     size=0 :: integer(),
                     pool={} :: tuple()}).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

create_ets_table() ->
    ets:new(?LOOKUP_TAB, [named_table, public, set,
                              {keypos, #shard_pool.key},
                              {read_concurrency, true}]),
    ets:new(?WORKER_TAB, [named_table, public, set,
                              {keypos, 1},
                              {read_concurrency, true}]).

next_shard(ChannelId) when is_integer(ChannelId) ->
    lookup_shard(next_hash(ChannelId)).

post_msg(SourceId, TokenName, Msg)
  when is_integer(SourceId),
       is_binary(TokenName) ->
    case next_shard(SourceId) of
        undefined -> ok; % no shards, drop
        SourceId -> ok; % do not firehose a firehose
        ChannelId when is_integer(ChannelId) ->
            logplex_stats:incr(#firehose_stat{channel_id=ChannelId, key=firehose_post}),
            logplex_channel:post_msg({channel, ChannelId}, Msg)
    end.

read_and_store_master_info() ->
    %% Ids = firehose_channel_ids(),
    %% Seed = [ Id || _ <- lists:seq(1, length(Ids)), Id <- Ids ],
    Seed = firehose_channel_ids(),
    ets:insert(?LOOKUP_TAB,
               #shard_pool{key=?MASTER_KEY, size=length(Seed), pool=Seed }),
    store_channels(1, Seed),
    ok.

store_channels(_Index, []) ->
    ok;
store_channels(Index, [Id | Rest]) ->
    ets:insert(?WORKER_TAB, {Index, Id}),
    store_channels(Index+1, Rest).

firehose_channel_ids() ->
    case logplex_app:config(firehose_channel_ids, []) of
        [] -> [];
        Ids when is_list(Ids) ->
            ChannelIdStrings = string:tokens(logplex_app:config(firehose_channel_ids, ""), ","),
            [ list_to_integer(Id) || Id <- ChannelIdStrings ]
    end.

%%%--------------------------------------------------------------------
%%% private functions
%%%--------------------------------------------------------------------

compute_hash(_, 0) ->
    0;
compute_hash(ChannelId, Bounds) ->
    erlang:phash2({os:timestamp(), self(), ChannelId}, Bounds) + 1.

lookup_shard(0) ->
    undefined;
lookup_shard(ShardId) when is_integer(ShardId) ->
    try ets:lookup_element(?WORKER_TAB, ShardId, 2) of
        ChannelId -> ChannelId
    catch error:badarg -> undefined
    end.

next_hash(ChannelId) ->
    try ets:lookup_element(?LOOKUP_TAB, ?MASTER_KEY, #shard_pool.size) of
        Num -> compute_hash(ChannelId, Num)
    catch error:badarg -> 0
    end.
