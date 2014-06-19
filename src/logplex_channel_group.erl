-module(logplex_channel_group).

-export([new/1, delete/1, join/2, leave/2, post_msg/2]).

-include("logplex.hrl").
-include("logplex_logging.hrl").

-type name() :: atom() | binary().
-export_type([name/0]).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

new(Group)
  when is_atom(Group);
       is_binary(Group) ->
    new({channel_group, Group});
new({channel_group, Group}) ->
    gproc_pool:new(Group, round_robin, [{auto_size, true}]),
    {channel_group, Group}.

delete({channel_group, Group}) ->
    gproc_pool:delete(Group).

join({channel_group, Group}, {channel, ChannelId}=Channel) ->
    try gproc_pool:add_worker(Group, Channel) of
        N when is_integer(N) ->
            gproc_pool:connect_worker(Group, Channel),
            ?INFO("channel_id=~p channel_group=~p at=join", [ChannelId, Group]),
            true
    catch
        error:exists -> false
    end.

leave({channel_group, Group}, {channel, _Id}=Channel) ->
    gproc_pool:disconnect_worker(Group, Channel),
    gproc_pool:remove_worker(Group, Channel).

post_msg(Where, Msg) when is_binary(Msg) ->
    case logplex_syslog_utils:from_msg(Msg) of
        {error, _} = E -> E;
        ParsedMsg -> post_msg(Where, ParsedMsg)
    end;
post_msg({channel_group, Group}, Msg) ->
    case gproc_pool:pick(Group) of
        false -> ok;
        Worker ->
            incr_stat(Worker),
            gproc:send(Worker, {post, Msg}),
            ok
    end.

%%%--------------------------------------------------------------------
%%% internal functions
%%%--------------------------------------------------------------------

incr_stat({n, l, [gproc_pool, Group, _Id, {channel, Id}]}) ->
    logplex_stats:incr(#channel_group_stat{ channel_group=Group, channel_id=Id, key=group_channel_post}).
