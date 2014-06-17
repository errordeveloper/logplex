%%%-------------------------------------------------------------------
%% @copyright Heroku, 2013
%% @author Alex Arnell <alex.arnell@gmail.com>
%% @doc CommonTest test suite for logplex_message
%% @end
%%%-------------------------------------------------------------------

-module(logplex_firehose_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [firehose_env, heroku_token, same_channel].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    meck:unload(),
    Config.

init_per_testcase(_, Config) ->
    FirehoseId = 218913,
    FirehoseChannel = {channel, FirehoseId},
    application:set_env(logplex, firehose_channel_id, FirehoseId),
    meck:new(logplex_channel, [no_link]),
    meck:expect(logplex_channel, post_msg, fun(_ChannelId, _Msg) ->
                                                  ok
                                          end),
    [{firehose, FirehoseChannel} | Config].

end_per_testcase(_CaseName, Config) ->
    meck:unload(logplex_channel),
    Config.

firehose_env(Config) ->
    FirehoseChannel = ?config(firehose, Config),
    Msg1 = term_to_binary(make_ref()),
    Msg2 = term_to_binary(make_ref()),
    logplex_firehose:post_msg(1234, <<"heroku">>, Msg1),
    true = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg1]),
    application:unset_env(logplex, firehose_channel_id),
    logplex_firehose:post_msg(1234, <<"heroku">>, Msg2),
    false = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg2]),
    1 = meck:num_calls(logplex_channel, post_msg, [FirehoseChannel, '_']),
    meck:unload(logplex_channel).

heroku_token(Config) ->
    FirehoseChannel = ?config(firehose, Config),
    Msg1 = term_to_binary(make_ref()),
    Msg2 = term_to_binary(make_ref()),
    logplex_firehose:post_msg(1234, <<"heroku">>, Msg1),
    logplex_firehose:post_msg(1234, <<"app">>, Msg2),
    true = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg1]),
    false = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg2]),
    1 = meck:num_calls(logplex_channel, post_msg, [FirehoseChannel, '_']),
    meck:unload(logplex_channel).

same_channel(Config) ->
    {channel, FirehoseId} = FirehoseChannel = ?config(firehose, Config),
    Msg1 = term_to_binary(make_ref()),
    Msg2 = term_to_binary(make_ref()),
    logplex_firehose:post_msg(1234, <<"heroku">>, Msg1),
    logplex_firehose:post_msg(FirehoseId, <<"heroku">>, Msg2),
    true = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg1]),
    false = meck:called(logplex_channel, post_msg, [FirehoseChannel, Msg2]),
    1 = meck:num_calls(logplex_channel, post_msg, [FirehoseChannel, '_']),
    meck:unload(logplex_channel).
