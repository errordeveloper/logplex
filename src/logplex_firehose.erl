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
         create_ets_table/0,
         shard_info/1,
         next_firehose/1,
         read_and_store_master_info/0,
         post_msg/4]).

-record(firehose, {idx :: integer(),
                   channel_id :: integer()}).
-define(MASTER_KEY, master_shard).
-record(shard_info, {key :: ?MASTER_KEY | logplex_channel:id(),
                     size :: integer()}).
-type shard_info() :: #shard_info{}.
-export_type([shard_info/0]).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

new() ->
    new([]).

new(ChannelIds) when is_list(ChannelIds) ->
    ets:new(?TAB, [named_table, public, set,
                   {keypos, #firehose.idx},
                   {read_concurrency, true}]),
    [register_channel(Id) || Id <- ChannelIds ],
    ok.

register_channel(ChannelId) when is_integer(ChannelId) ->
    ?INFO("channel_id=~p at=spawn", [ChannelId]),
    Idx = ets:info(?TAB, size),
    true = ets:insert(?TAB, #firehose{ idx=Idx, channel_id=ChannelId }),
    ok.

unregister_channel(ChannelId) when is_integer(ChannelId) ->
    List = ets:tab2list(?TAB),
    ets:delete_all_objects(?TAB),
    [ register_channel(Id) || #firehose{ channel_id=Id } <- List, Id =/= ChannelId ],
    ok.

create_ets_table() ->
    ets:new(firehose_shards, [named_table, public, set,
                              {read_concurrency, true}]).

post_msg(SourceId, TokenId, Msg, ShardInfo)
  when is_integer(SourceId),
       is_binary(TokenId),
       is_tuple(Msg),
       is_record(ShardInfo, shard_info) ->
    case next_firehose(ShardInfo) of
        undefined -> ok; % no shards, drop
        SourceId -> ok; % do not firehose a firehose
        ChannelId when is_integer(ChannelId) ->
            post_msg(ChannelId, TokenId, Msg)
    end,
    ok.
    
%% post_msg(ChannelId, TokenName,
%%          {Facility, Severity, Time, _Source, Process, Msg})
%%   when is_integer(ChannelId),
%%        is_binary(TokenName) ->
%%     case ets:lookup(?TAB, next_shard(ets:info(?TAB, size))) of
%%         [#firehose{ channel_id=FirehoseId }] ->
%%             logplex_stats:incr(#firehose_stat{channel_id=FirehoseId, key=firehose_post}),
%%             logplex_channel:post_msg({channel, FirehoseId}, TokenName,
%%                                      {Facility, Severity, Time, TokenName, Process, Msg});
%%         _ -> ok % ignored
%%     end.

post_msg(ChannelId, TokenId,
         {Facility, Severity, Time, _Source, Process, Msg}) ->
logplex_stats:incr(#firehose_stat{channel_id=ChannelId, key=firehose_post}),
logplex_channel:post_msg({channel, ChannelId}, TokenId,
                         {Facility, Severity, Time, TokenId, Process, Msg}).

shard_info(ChannelId) when is_integer(ChannelId) ->
    case ets:lookup(firehose_shards, ChannelId) of
        [] ->
            update_shard_pool(ChannelId),
            shard_info(ChannelId);
        [ShardPool] ->
            #shard_info{ key=ChannelId, size=size(ShardPool)-1 }
    end.

next_firehose(#shard_info{ size=0 }) ->
    undefined;
next_firehose(Info=#shard_info{ size=Size, key=Key }) ->
    try ets:lookup_element(firehose_shards, Key, next_shard(Size)) of
        ChannelId -> ChannelId
    catch
        error:badarg ->
            update_shard_pool(Key),
            next_firehose(Info)
    end.

next_shard(Num) when Num > 1 ->
    erlang:phash2({erlang:make_ref(), self()}, Num) + 2;
next_shard(1) ->
    2.

update_shard_pool(ChannelId) when is_integer(ChannelId) ->
    case ets:lookup(firehose_shards, ?MASTER_KEY) of
        [] ->
            read_and_store_master_info(),
            update_shard_pool(ChannelId);
        [MasterInfo] ->
            case is_member(ChannelId, MasterInfo, 2) of
                true ->
                    ets:insert(firehose_shards, {ChannelId});
                false ->
                    ShardInfo = setelement(1, MasterInfo, ChannelId),
                    true = ets:insert(firehose_shards, ShardInfo)
            end
    end.

read_and_store_master_info() ->
    case logplex_app:config(firehose_channel_ids, []) of
        ShardPool when element(1, ShardPool) =:= ?MASTER_KEY ->
            ets:delete_all_objects(firehose_shards),
            true = ets:insert(firehose_shards, ShardPool),
            true;
        Ids when is_list(Ids) ->
            ChannelIdStrings = string:tokens(logplex_app:config(firehose_channel_ids, ""), ","),
            ChannelIds = [ list_to_integer(Id) || Id <- ChannelIdStrings ],
            ShardPool = list_to_tuple([?MASTER_KEY | ChannelIds]),
            application:set_env(logplex, firehose_channel_ids, ShardPool),
            true
    end.

is_member(_Id, Tuple, Pos) when Pos > size(Tuple) ->
    false;
is_member(Id, Tuple, Pos) ->
    case element(Pos, Tuple) =:= Id of
        true -> true;
        false -> is_member(Id, Tuple, Pos+1)
    end.
