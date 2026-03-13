%%
%%   Performance benchmark test suite for cache
%%
-module(cache_perf_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
   [
      perf_sequential_put_get,
      perf_sequential_put_get_large_values,
      perf_sequential_mixed_operations,
      perf_many_keys,
      perf_put_overwrite,
      perf_cache_with_eviction,
      perf_ordered_set,
      perf_concurrent_writers,
      perf_concurrent_readers,
      perf_concurrent_mixed
   ].

%%%----------------------------------------------------------------------------
%%%
%%% Sequential performance tests
%%%
%%%----------------------------------------------------------------------------

perf_sequential_put_get(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 10000,
   {PutTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> ok = cache:put(Cache, I, I) end,
         lists:seq(1, N)
      )
   end),
   {GetTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> I = cache:get(Cache, I) end,
         lists:seq(1, N)
      )
   end),
   PutOpsPerSec = N * 1000000 div max(PutTime, 1),
   GetOpsPerSec = N * 1000000 div max(GetTime, 1),
   ct:pal("Sequential put: ~p ops/sec (~p us total for ~p ops)", [PutOpsPerSec, PutTime, N]),
   ct:pal("Sequential get: ~p ops/sec (~p us total for ~p ops)", [GetOpsPerSec, GetTime, N]),
   true = PutOpsPerSec > 1000,
   true = GetOpsPerSec > 1000,
   ok = cache:drop(Cache).

perf_sequential_put_get_large_values(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 1000,
   Value = binary:copy(<<0>>, 10000),
   {PutTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> ok = cache:put(Cache, I, Value) end,
         lists:seq(1, N)
      )
   end),
   {GetTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> Value = cache:get(Cache, I) end,
         lists:seq(1, N)
      )
   end),
   PutOpsPerSec = N * 1000000 div max(PutTime, 1),
   GetOpsPerSec = N * 1000000 div max(GetTime, 1),
   ct:pal("Large value put: ~p ops/sec (~p us total for ~p ops)", [PutOpsPerSec, PutTime, N]),
   ct:pal("Large value get: ~p ops/sec (~p us total for ~p ops)", [GetOpsPerSec, GetTime, N]),
   true = PutOpsPerSec > 500,
   true = GetOpsPerSec > 500,
   ok = cache:drop(Cache).

perf_sequential_mixed_operations(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 5000,
   {Time, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) ->
            ok = cache:put(Cache, I, I),
            I = cache:get(Cache, I),
            true = cache:has(Cache, I),
            ok = cache:remove(Cache, I),
            false = cache:has(Cache, I)
         end,
         lists:seq(1, N)
      )
   end),
   OpsPerSec = (N * 5) * 1000000 div max(Time, 1),
   ct:pal("Mixed operations: ~p ops/sec (~p us total for ~p compound ops)", [OpsPerSec, Time, N]),
   true = OpsPerSec > 1000,
   ok = cache:drop(Cache).

perf_many_keys(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 50000,
   {PutTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> ok = cache:put(Cache, I, I) end,
         lists:seq(1, N)
      )
   end),
   {GetTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> I = cache:get(Cache, I) end,
         lists:seq(1, N)
      )
   end),
   PutOpsPerSec = N * 1000000 div max(PutTime, 1),
   GetOpsPerSec = N * 1000000 div max(GetTime, 1),
   ct:pal("Many keys put: ~p ops/sec (~p us total for ~p ops)", [PutOpsPerSec, PutTime, N]),
   ct:pal("Many keys get: ~p ops/sec (~p us total for ~p ops)", [GetOpsPerSec, GetTime, N]),
   true = PutOpsPerSec > 500,
   true = GetOpsPerSec > 500,
   ok = cache:drop(Cache).

perf_put_overwrite(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 10000,
   lists:foreach(
      fun(I) -> ok = cache:put(Cache, I rem 100, I) end,
      lists:seq(1, N)
   ),
   {Time, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> ok = cache:put(Cache, I rem 100, I) end,
         lists:seq(1, N)
      )
   end),
   OpsPerSec = N * 1000000 div max(Time, 1),
   ct:pal("Overwrite put: ~p ops/sec (~p us total for ~p ops)", [OpsPerSec, Time, N]),
   true = OpsPerSec > 1000,
   ok = cache:drop(Cache).

perf_cache_with_eviction(_Config) ->
   {ok, Cache} = cache:start_link([{n, 5}, {ttl, 5}, {size, 1000}, {check, 1}]),
   N = 5000,
   {Time, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) ->
            ok = cache:put(Cache, I, I),
            _ = cache:get(Cache, I)
         end,
         lists:seq(1, N)
      )
   end),
   OpsPerSec = (N * 2) * 1000000 div max(Time, 1),
   ct:pal("With eviction: ~p ops/sec (~p us total for ~p put+get ops)", [OpsPerSec, Time, N]),
   true = OpsPerSec > 500,
   ok = cache:drop(Cache).

perf_ordered_set(_Config) ->
   {ok, Cache} = cache:start_link([{type, ordered_set}, {n, 10}, {ttl, 600}]),
   N = 10000,
   {PutTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> ok = cache:put(Cache, I, I) end,
         lists:seq(1, N)
      )
   end),
   {GetTime, _} = timer:tc(fun() ->
      lists:foreach(
         fun(I) -> I = cache:get(Cache, I) end,
         lists:seq(1, N)
      )
   end),
   PutOpsPerSec = N * 1000000 div max(PutTime, 1),
   GetOpsPerSec = N * 1000000 div max(GetTime, 1),
   ct:pal("Ordered set put: ~p ops/sec (~p us total for ~p ops)", [PutOpsPerSec, PutTime, N]),
   ct:pal("Ordered set get: ~p ops/sec (~p us total for ~p ops)", [GetOpsPerSec, GetTime, N]),
   true = PutOpsPerSec > 500,
   true = GetOpsPerSec > 500,
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% Concurrent performance tests
%%%
%%%----------------------------------------------------------------------------

perf_concurrent_writers(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   NumProcs = 10,
   OpsPerProc = 1000,
   {Time, _} = timer:tc(fun() ->
      Parent = self(),
      Pids = [spawn_link(fun() ->
         lists:foreach(
            fun(I) ->
               Key = {Proc, I},
               ok = cache:put(Cache, Key, I)
            end,
            lists:seq(1, OpsPerProc)
         ),
         Parent ! {done, self()}
      end) || Proc <- lists:seq(1, NumProcs)],
      lists:foreach(fun(Pid) ->
         receive {done, Pid} -> ok after 30000 -> error(timeout) end
      end, Pids)
   end),
   TotalOps = NumProcs * OpsPerProc,
   OpsPerSec = TotalOps * 1000000 div max(Time, 1),
   ct:pal("Concurrent writers (~p procs): ~p ops/sec (~p us total for ~p ops)",
      [NumProcs, OpsPerSec, Time, TotalOps]),
   true = OpsPerSec > 500,
   ok = cache:drop(Cache).

perf_concurrent_readers(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 1000,
   lists:foreach(fun(I) -> ok = cache:put(Cache, I, I) end, lists:seq(1, N)),
   NumProcs = 10,
   OpsPerProc = 1000,
   {Time, _} = timer:tc(fun() ->
      Parent = self(),
      Pids = [spawn_link(fun() ->
         lists:foreach(
            fun(I) ->
               Key = (I rem N) + 1,
               Key = cache:get(Cache, Key)
            end,
            lists:seq(1, OpsPerProc)
         ),
         Parent ! {done, self()}
      end) || _ <- lists:seq(1, NumProcs)],
      lists:foreach(fun(Pid) ->
         receive {done, Pid} -> ok after 30000 -> error(timeout) end
      end, Pids)
   end),
   TotalOps = NumProcs * OpsPerProc,
   OpsPerSec = TotalOps * 1000000 div max(Time, 1),
   ct:pal("Concurrent readers (~p procs): ~p ops/sec (~p us total for ~p ops)",
      [NumProcs, OpsPerSec, Time, TotalOps]),
   true = OpsPerSec > 500,
   ok = cache:drop(Cache).

perf_concurrent_mixed(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   N = 100,
   lists:foreach(fun(I) -> ok = cache:put(Cache, I, I) end, lists:seq(1, N)),
   NumProcs = 10,
   OpsPerProc = 1000,
   {Time, _} = timer:tc(fun() ->
      Parent = self(),
      Pids = [spawn_link(fun() ->
         lists:foreach(
            fun(I) ->
               Key = (I rem N) + 1,
               case I rem 3 of
                  0 -> ok = cache:put(Cache, Key, I);
                  1 -> _ = cache:get(Cache, Key);
                  2 -> _ = cache:has(Cache, Key)
               end
            end,
            lists:seq(1, OpsPerProc)
         ),
         Parent ! {done, self()}
      end) || _ <- lists:seq(1, NumProcs)],
      lists:foreach(fun(Pid) ->
         receive {done, Pid} -> ok after 30000 -> error(timeout) end
      end, Pids)
   end),
   TotalOps = NumProcs * OpsPerProc,
   OpsPerSec = TotalOps * 1000000 div max(Time, 1),
   ct:pal("Concurrent mixed (~p procs): ~p ops/sec (~p us total for ~p ops)",
      [NumProcs, OpsPerSec, Time, TotalOps]),
   true = OpsPerSec > 500,
   ok = cache:drop(Cache).
