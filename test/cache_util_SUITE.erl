%%
%%   Unit tests for cache_util module
%%
-module(cache_util_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
   Exclude = [module_info, init_per_suite, end_per_suite,
              do_recv_stats, do_recv_stats3],
   [Test || {Test, NAry} <- ?MODULE:module_info(exports),
      not lists:member(Test, Exclude),
      NAry =:= 1
   ].

%%%----------------------------------------------------------------------------
%%%
%%% mdiv tests
%%%
%%%----------------------------------------------------------------------------

mdiv_normal(_) ->
   5 = cache_util:mdiv(10, 2),
   3 = cache_util:mdiv(10, 3).

mdiv_undefined_x(_) ->
   undefined = cache_util:mdiv(undefined, 2).

mdiv_undefined_y(_) ->
   undefined = cache_util:mdiv(10, undefined).

mdiv_both_undefined(_) ->
   undefined = cache_util:mdiv(undefined, undefined).

%%%----------------------------------------------------------------------------
%%%
%%% madd tests
%%%
%%%----------------------------------------------------------------------------

madd_normal(_) ->
   15 = cache_util:madd(10, 5).

madd_undefined_x(_) ->
   undefined = cache_util:madd(undefined, 5).

madd_undefined_y(_) ->
   undefined = cache_util:madd(10, undefined).

madd_both_undefined(_) ->
   undefined = cache_util:madd(undefined, undefined).

%%%----------------------------------------------------------------------------
%%%
%%% mmul tests
%%%
%%%----------------------------------------------------------------------------

mmul_normal(_) ->
   50 = cache_util:mmul(10, 5).

mmul_undefined_x(_) ->
   undefined = cache_util:mmul(undefined, 5).

mmul_undefined_y(_) ->
   undefined = cache_util:mmul(10, undefined).

mmul_both_undefined(_) ->
   undefined = cache_util:mmul(undefined, undefined).

%%%----------------------------------------------------------------------------
%%%
%%% now tests
%%%
%%%----------------------------------------------------------------------------

now_returns_integer(_) ->
   T = cache_util:now(),
   true = is_integer(T),
   true = T > 0.

now_is_monotonic(_) ->
   T1 = cache_util:now(),
   timer:sleep(1100),
   T2 = cache_util:now(),
   true = T2 >= T1 + 1.

%%%----------------------------------------------------------------------------
%%%
%%% stats tests
%%%
%%%----------------------------------------------------------------------------

stats_undefined(_) ->
   ok = cache_util:stats(undefined, some_counter).

stats_fun(_) ->
   Self = self(),
   Fun = fun(Counter) -> Self ! {stats, Counter} end,
   _ = cache_util:stats(Fun, test_counter),
   receive
      {stats, test_counter} -> ok
   after 1000 ->
      error(stats_not_received)
   end.

stats_mfa(_) ->
   Self = self(),
   register(test_stats_util_receiver, Self),
   _ = cache_util:stats({?MODULE, do_recv_stats}, test_counter),
   receive
      {stats_util, test_counter} -> ok
   after 1000 ->
      error(stats_not_received)
   end,
   unregister(test_stats_util_receiver).

do_recv_stats(Counter) ->
   test_stats_util_receiver ! {stats_util, Counter}.

stats3_undefined(_) ->
   ok = cache_util:stats(undefined, some_counter, some_val).

stats3_fun(_) ->
   Self = self(),
   Fun = fun(Counter, Val) -> Self ! {stats, Counter, Val} end,
   _ = cache_util:stats(Fun, test_counter, 42),
   receive
      {stats, test_counter, 42} -> ok
   after 1000 ->
      error(stats_not_received)
   end.

stats3_mfa(_) ->
   Self = self(),
   register(test_stats3_util_receiver, Self),
   _ = cache_util:stats({?MODULE, do_recv_stats3}, test_counter, 42),
   receive
      {stats3_util, test_counter, 42} -> ok
   after 1000 ->
      error(stats_not_received)
   end,
   unregister(test_stats3_util_receiver).

do_recv_stats3(Counter, Val) ->
   test_stats3_util_receiver ! {stats3_util, Counter, Val}.
