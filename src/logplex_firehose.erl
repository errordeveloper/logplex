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
-define(MASTER_KEY, master_shard).

-export([lookup_shard/1,
         next_shard/1,
         post_msg/3]).

-export([create_ets_table/0,
         read_and_store_master_info/0]).

-record(shard_pool, {key :: ?MASTER_KEY | logplex_channel:id(),
                     size=0 :: integer(),
                     pool={} :: tuple()}).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

create_ets_table() ->
    ets:new(?TAB, [named_table, public, set,
                              {keypos, #shard_pool.key},
                              {read_concurrency, true}]).

lookup_shard(ChannelId) when is_integer(ChannelId) ->
    case ets:lookup(?TAB, ChannelId) of
        [] -> update_shard_pool(ChannelId);
        [Shard] -> Shard
    end.

next_shard(ChannelId) when is_integer(ChannelId) ->
    select_random(lookup_shard(ChannelId)).

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
    case logplex_app:config(firehose_channel_ids, []) of
        ShardPool=#shard_pool{ key=?MASTER_KEY } ->
            ets:delete_all_objects(?TAB),
            true = ets:insert(?TAB, ShardPool),
            ShardPool;
        Ids when is_list(Ids) ->
            ChannelIdStrings = string:tokens(logplex_app:config(firehose_channel_ids, ""), ","),
            ChannelIds = list_to_tuple([ list_to_integer(Id) || Id <- ChannelIdStrings ]),
            application:set_env(logplex, firehose_channel_ids,
                                #shard_pool{ key=?MASTER_KEY,
                                             size=size(ChannelIds),
                                             pool=ChannelIds}),
            read_and_store_master_info()
    end.

%%%--------------------------------------------------------------------
%%% private functions
%%%--------------------------------------------------------------------

clone_shard_pool(Key, Orig=#shard_pool{pool=Pool}) ->
    case is_member(Key, Pool) of
        true ->
            #shard_pool{key=Key};
        false ->
            Orig#shard_pool{ key=Key }
    end.

is_member(Id, Tuple) when is_tuple(Tuple) ->
    is_member(Id, Tuple, 1).

is_member(_Id, Tuple, Pos) when Pos > size(Tuple) ->
    false;
is_member(Id, Tuple, Pos) ->
    case element(Pos, Tuple) =:= Id of
        true -> true;
        false -> is_member(Id, Tuple, Pos+1)
    end.

select_random(#shard_pool{ size=0 }) ->
    undefined;
select_random(#shard_pool{ size=Size, pool=Pool}) ->
    Pos = erlang:phash2({make_ref(), self()}, Size) + 1,
    erlang:element(Pos, Pool).

update_shard_pool(ChannelId) when is_integer(ChannelId) ->
    ShardPool = case ets:lookup(?TAB, ?MASTER_KEY) of
        [MasterInfo] -> clone_shard_pool(ChannelId, MasterInfo);
        [] ->
            clone_shard_pool(ChannelId, read_and_store_master_info())
    end,
    true = ets:insert(?TAB, ShardPool),
    ShardPool.

