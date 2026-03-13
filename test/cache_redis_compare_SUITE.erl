%%
%%   Performance comparison: cache vs Redis vs raw ETS
%%
-module(cache_redis_compare_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
   [
      compare_sequential_put,
      compare_sequential_get,
      compare_sequential_mixed,
      compare_concurrent_mixed,
      compare_serde_simple_tuple,
      compare_serde_nested_map,
      compare_serde_large_proplist,
      compare_serde_mixed_record_like
   ].

init_per_suite(Config) ->
   application:ensure_all_started(cache),
   %% Verify Redis is running
   case os:cmd("redis-cli ping") of
      "PONG\n" ->
         os:cmd("redis-cli flushdb"),
         Config;
      _ ->
         {skip, "Redis not available"}
   end.

end_per_suite(_Config) ->
   os:cmd("redis-cli flushdb"),
   application:stop(cache),
   ok.

init_per_testcase(_TC, Config) ->
   os:cmd("redis-cli flushdb"),
   Config.

end_per_testcase(_TC, _Config) ->
   ok.

%%%----------------------------------------------------------------------------
%%%
%%% Comparison benchmarks
%%%
%%%----------------------------------------------------------------------------

compare_sequential_put(_Config) ->
   N = 5000,

   %% --- Raw ETS baseline ---
   Tab = ets:new(bench_ets, [set, public]),
   {EtsTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         ets:insert(Tab, {I, I})
      end, lists:seq(1, N))
   end),
   ets:delete(Tab),
   EtsOps = N * 1000000 div max(EtsTime, 1),

   %% --- cache library ---
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   {CacheTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         ok = cache:put(Cache, I, I)
      end, lists:seq(1, N))
   end),
   cache:drop(Cache),
   CacheOps = N * 1000000 div max(CacheTime, 1),

   %% --- Redis via port ---
   {RedisTime, _} = timer:tc(fun() ->
      redis_pipeline_set(N)
   end),
   RedisOps = N * 1000000 div max(RedisTime, 1),

   ct:pal("~n=== Sequential PUT (~p ops) ===", [N]),
   ct:pal("  Raw ETS:       ~9w ops/sec  (~6w us)", [EtsOps, EtsTime]),
   ct:pal("  cache lib:     ~9w ops/sec  (~6w us)", [CacheOps, CacheTime]),
   ct:pal("  Redis:         ~9w ops/sec  (~6w us)", [RedisOps, RedisTime]),
   ct:pal("  cache/Redis:   ~.2fx faster", [CacheOps / max(RedisOps, 1)]),
   ct:pal("  ETS/cache:     ~.2fx faster", [EtsOps / max(CacheOps, 1)]),
   ok.

compare_sequential_get(_Config) ->
   N = 5000,

   %% --- Raw ETS baseline ---
   Tab = ets:new(bench_ets, [set, public]),
   lists:foreach(fun(I) -> ets:insert(Tab, {I, I}) end, lists:seq(1, N)),
   {EtsTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         [{I, I}] = ets:lookup(Tab, I)
      end, lists:seq(1, N))
   end),
   ets:delete(Tab),
   EtsOps = N * 1000000 div max(EtsTime, 1),

   %% --- cache library ---
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   lists:foreach(fun(I) -> ok = cache:put(Cache, I, I) end, lists:seq(1, N)),
   {CacheTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         I = cache:get(Cache, I)
      end, lists:seq(1, N))
   end),
   cache:drop(Cache),
   CacheOps = N * 1000000 div max(CacheTime, 1),

   %% --- Redis ---
   redis_pipeline_set(N),
   {RedisTime, _} = timer:tc(fun() ->
      redis_pipeline_get(N)
   end),
   RedisOps = N * 1000000 div max(RedisTime, 1),

   ct:pal("~n=== Sequential GET (~p ops) ===", [N]),
   ct:pal("  Raw ETS:       ~9w ops/sec  (~6w us)", [EtsOps, EtsTime]),
   ct:pal("  cache lib:     ~9w ops/sec  (~6w us)", [CacheOps, CacheTime]),
   ct:pal("  Redis:         ~9w ops/sec  (~6w us)", [RedisOps, RedisTime]),
   ct:pal("  cache/Redis:   ~.2fx faster", [CacheOps / max(RedisOps, 1)]),
   ct:pal("  ETS/cache:     ~.2fx faster", [EtsOps / max(CacheOps, 1)]),
   ok.

compare_sequential_mixed(_Config) ->
   N = 2000,

   %% --- Raw ETS baseline ---
   Tab = ets:new(bench_ets, [set, public]),
   {EtsTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         ets:insert(Tab, {I, I}),
         [{I, I}] = ets:lookup(Tab, I),
         ets:delete(Tab, I)
      end, lists:seq(1, N))
   end),
   ets:delete(Tab),
   EtsOps = (N * 3) * 1000000 div max(EtsTime, 1),

   %% --- cache library ---
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   {CacheTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         ok = cache:put(Cache, I, I),
         I = cache:get(Cache, I),
         ok = cache:remove(Cache, I)
      end, lists:seq(1, N))
   end),
   cache:drop(Cache),
   CacheOps = (N * 3) * 1000000 div max(CacheTime, 1),

   %% --- Redis ---
   {RedisTime, _} = timer:tc(fun() ->
      redis_pipeline_mixed(N)
   end),
   RedisOps = (N * 3) * 1000000 div max(RedisTime, 1),

   ct:pal("~n=== Sequential MIXED put/get/delete (~p compound ops, ~p total) ===", [N, N * 3]),
   ct:pal("  Raw ETS:       ~9w ops/sec  (~6w us)", [EtsOps, EtsTime]),
   ct:pal("  cache lib:     ~9w ops/sec  (~6w us)", [CacheOps, CacheTime]),
   ct:pal("  Redis:         ~9w ops/sec  (~6w us)", [RedisOps, RedisTime]),
   ct:pal("  cache/Redis:   ~.2fx faster", [CacheOps / max(RedisOps, 1)]),
   ct:pal("  ETS/cache:     ~.2fx faster", [EtsOps / max(CacheOps, 1)]),
   ok.

compare_concurrent_mixed(_Config) ->
   N = 1000,
   NumProcs = 10,

   %% --- Raw ETS baseline ---
   Tab = ets:new(bench_ets, [set, public, {write_concurrency, true}, {read_concurrency, true}]),
   lists:foreach(fun(I) -> ets:insert(Tab, {I, I}) end, lists:seq(1, N)),
   {EtsTime, _} = timer:tc(fun() ->
      Parent = self(),
      Pids = [spawn_link(fun() ->
         lists:foreach(fun(I) ->
            Key = (I rem N) + 1,
            case I rem 3 of
               0 -> ets:insert(Tab, {Key, I});
               1 -> ets:lookup(Tab, Key);
               2 -> ets:member(Tab, Key)
            end
         end, lists:seq(1, N)),
         Parent ! {done, self()}
      end) || _ <- lists:seq(1, NumProcs)],
      lists:foreach(fun(Pid) ->
         receive {done, Pid} -> ok after 30000 -> error(timeout) end
      end, Pids)
   end),
   ets:delete(Tab),
   EtsTotalOps = NumProcs * N,
   EtsOps = EtsTotalOps * 1000000 div max(EtsTime, 1),

   %% --- cache library ---
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   lists:foreach(fun(I) -> ok = cache:put(Cache, I, I) end, lists:seq(1, N)),
   {CacheTime, _} = timer:tc(fun() ->
      Parent = self(),
      Pids = [spawn_link(fun() ->
         lists:foreach(fun(I) ->
            Key = (I rem N) + 1,
            case I rem 3 of
               0 -> cache:put(Cache, Key, I);
               1 -> cache:get(Cache, Key);
               2 -> cache:has(Cache, Key)
            end
         end, lists:seq(1, N)),
         Parent ! {done, self()}
      end) || _ <- lists:seq(1, NumProcs)],
      lists:foreach(fun(Pid) ->
         receive {done, Pid} -> ok after 30000 -> error(timeout) end
      end, Pids)
   end),
   cache:drop(Cache),
   CacheTotalOps = NumProcs * N,
   CacheOps = CacheTotalOps * 1000000 div max(CacheTime, 1),

   %% --- Redis (sequential, since redis is single-threaded anyway) ---
   redis_pipeline_set(N),
   {RedisTime, _} = timer:tc(fun() ->
      redis_pipeline_mixed(N)
   end),
   RedisTotalOps = N * 3,
   RedisOps = RedisTotalOps * 1000000 div max(RedisTime, 1),

   ct:pal("~n=== Concurrent MIXED (~p procs, ~p ops each) ===", [NumProcs, N]),
   ct:pal("  Raw ETS:       ~9w ops/sec  (~6w us, ~p ops)", [EtsOps, EtsTime, EtsTotalOps]),
   ct:pal("  cache lib:     ~9w ops/sec  (~6w us, ~p ops)", [CacheOps, CacheTime, CacheTotalOps]),
   ct:pal("  Redis:         ~9w ops/sec  (~6w us, ~p ops)", [RedisOps, RedisTime, RedisTotalOps]),
   ct:pal("  cache/Redis:   ~.2fx faster", [CacheOps / max(RedisOps, 1)]),
   ct:pal("  ETS/cache:     ~.2fx faster", [EtsOps / max(CacheOps, 1)]),
   ok.

%%%----------------------------------------------------------------------------
%%%
%%% Serialization cost comparison tests
%%%
%%% cache stores native Erlang terms directly in ETS (zero serde).
%%% Redis requires term_to_binary/binary_to_term for any Erlang term.
%%% These tests measure the real-world cost of that difference.
%%%
%%%----------------------------------------------------------------------------

compare_serde_simple_tuple(_Config) ->
   N = 5000,
   MakeVal = fun(I) -> {user, I, <<"alice">>, {role, admin}, [read, write, delete]} end,
   serde_bench("Simple Tuple ({user, id, name, role, perms})", N, MakeVal).

compare_serde_nested_map(_Config) ->
   N = 5000,
   MakeVal = fun(I) ->
      #{id => I,
        name => <<"test_user">>,
        metadata => #{created => {2026, 3, 13},
                      tags => [erlang, cache, performance],
                      settings => #{theme => dark, lang => en}},
        scores => lists:seq(1, 20)}
   end,
   serde_bench("Nested Map (3 levels, lists, tuples)", N, MakeVal).

compare_serde_large_proplist(_Config) ->
   N = 2000,
   MakeVal = fun(I) ->
      [{id, I},
       {name, <<"benchmark_entry">>},
       {tags, [<<"erlang">>, <<"otp">>, <<"ets">>, <<"cache">>]},
       {headers, [{<<"content-type">>, <<"application/json">>},
                  {<<"x-request-id">>, integer_to_binary(I)},
                  {<<"authorization">>, <<"Bearer tok_abc123">>}]},
       {body, #{payload => binary:copy(<<"x">>, 200),
                timestamp => erlang:system_time(millisecond),
                flags => {true, false, true, undefined}}},
       {history, [{action, created}, {action, updated}, {action, verified}]}]
   end,
   serde_bench("Large Proplist (HTTP-like request, ~300 bytes serialized)", N, MakeVal).

compare_serde_mixed_record_like(_Config) ->
   N = 5000,
   MakeVal = fun(I) ->
      {session, I,
       <<"sess_", (integer_to_binary(I))/binary>>,
       erlang:system_time(millisecond),
       [{ip, {192, 168, 1, I rem 255}},
        {ua, <<"Mozilla/5.0">>},
        {geo, {37.7749, -122.4194}}],
       active}
   end,
   serde_bench("Record-like Tuple (session with IP, geo, timestamps)", N, MakeVal).

serde_bench(Label, N, MakeVal) ->
   Values = [MakeVal(I) || I <- lists:seq(1, N)],
   SampleVal = hd(Values),
   SerializedSize = byte_size(term_to_binary(SampleVal)),

   %% --- Measure serialization cost alone ---
   {SerdeTime, _} = timer:tc(fun() ->
      lists:foreach(fun(V) ->
         Bin = term_to_binary(V),
         _ = binary_to_term(Bin)
      end, Values)
   end),
   SerdeOps = N * 1000000 div max(SerdeTime, 1),

   %% --- cache library (zero serde) ---
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 600}]),
   {CachePutTime, _} = timer:tc(fun() ->
      lists:foldl(fun(V, I) ->
         ok = cache:put(Cache, I, V),
         I + 1
      end, 1, Values)
   end),
   {CacheGetTime, _} = timer:tc(fun() ->
      lists:foreach(fun(I) ->
         _ = cache:get(Cache, I)
      end, lists:seq(1, N))
   end),
   cache:drop(Cache),
   CachePutOps = N * 1000000 div max(CachePutTime, 1),
   CacheGetOps = N * 1000000 div max(CacheGetTime, 1),
   CacheRoundTrip = CachePutTime + CacheGetTime,
   CacheRtOps = N * 1000000 div max(CacheRoundTrip, 1),

   %% --- Redis with term_to_binary/binary_to_term (realistic Erlang usage) ---
   {RedisPutTime, _} = timer:tc(fun() ->
      Commands = lists:map(fun({I, V}) ->
         Bin = base64:encode(term_to_binary(V)),
         K = integer_to_list(I),
         ["SET ", K, " ", Bin, "\r\n"]
      end, lists:zip(lists:seq(1, N), Values)),
      redis_pipe(Commands)
   end),
   {RedisGetTime, _} = timer:tc(fun() ->
      %% For a fair read comparison, we do individual GETs with deserialization
      lists:foreach(fun(I) ->
         K = integer_to_list(I),
         Result = os:cmd("redis-cli GET " ++ K),
         %% Strip trailing newline from redis-cli output
         Trimmed = string:trim(Result, trailing, "\n"),
         case Trimmed of
            "" -> ok;
            B  -> _ = binary_to_term(base64:decode(list_to_binary(B)))
         end
      end, lists:seq(1, min(N, 500)))
   end),
   RedisGetN = min(N, 500),
   RedisPutOps = N * 1000000 div max(RedisPutTime, 1),
   RedisGetOps = RedisGetN * 1000000 div max(RedisGetTime, 1),
   RedisRoundTrip = RedisPutTime + (RedisGetTime * N div RedisGetN),
   RedisRtOps = N * 1000000 div max(RedisRoundTrip, 1),

   ct:pal("~n=== Serialization Cost: ~s ===", [Label]),
   ct:pal("  Serialized size:       ~p bytes per value", [SerializedSize]),
   ct:pal("  Serde only (encode+decode): ~9w ops/sec (~6w us)", [SerdeOps, SerdeTime]),
   ct:pal("  ---"),
   ct:pal("  cache PUT (no serde):  ~9w ops/sec  (~6w us)", [CachePutOps, CachePutTime]),
   ct:pal("  cache GET (no serde):  ~9w ops/sec  (~6w us)", [CacheGetOps, CacheGetTime]),
   ct:pal("  cache round-trip:      ~9w ops/sec  (~6w us)", [CacheRtOps, CacheRoundTrip]),
   ct:pal("  ---"),
   ct:pal("  Redis PUT (with serde):~9w ops/sec  (~6w us)", [RedisPutOps, RedisPutTime]),
   ct:pal("  Redis GET (with serde):~9w ops/sec  (~6w us, ~p sampled)", [RedisGetOps, RedisGetTime, RedisGetN]),
   ct:pal("  Redis round-trip (est):~9w ops/sec  (~6w us)", [RedisRtOps, RedisRoundTrip]),
   ct:pal("  ---"),
   ct:pal("  cache/Redis round-trip: ~.2fx faster", [CacheRtOps / max(RedisRtOps, 1)]),
   os:cmd("redis-cli flushdb"),
   ok.

%%%----------------------------------------------------------------------------
%%%
%%% Redis helpers - uses redis-cli pipeline for fair comparison
%%%
%%%----------------------------------------------------------------------------

redis_pipeline_set(N) ->
   %% Build a pipeline of SET commands and send via redis-cli --pipe
   Commands = lists:map(fun(I) ->
      K = integer_to_list(I),
      ["SET ", K, " ", K, "\r\n"]
   end, lists:seq(1, N)),
   redis_pipe(Commands).

redis_pipeline_get(N) ->
   Commands = lists:map(fun(I) ->
      K = integer_to_list(I),
      ["GET ", K, "\r\n"]
   end, lists:seq(1, N)),
   redis_pipe(Commands).

redis_pipeline_mixed(N) ->
   Commands = lists:map(fun(I) ->
      K = integer_to_list(I),
      [
         "SET ", K, " ", K, "\r\n",
         "GET ", K, "\r\n",
         "DEL ", K, "\r\n"
      ]
   end, lists:seq(1, N)),
   redis_pipe(Commands).

redis_pipe(Commands) ->
   %% Use inline redis commands via redis-cli pipe mode for maximum throughput
   Blob = iolist_to_binary(Commands),
   TmpFile = "/tmp/redis_bench_cmds.txt",
   file:write_file(TmpFile, Blob),
   os:cmd("redis-cli --pipe < " ++ TmpFile ++ " 2>/dev/null"),
   file:delete(TmpFile),
   ok.
