%%
%%   Copyright 2015 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(cache_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
   Exclude = [module_info, init_per_suite, end_per_suite,
              init_per_testcase, end_per_testcase,
              do_handle_stats],
   [Test || {Test, NAry} <- ?MODULE:module_info(exports),
      not lists:member(Test, Exclude),
      NAry =:= 1
   ].

%%%----------------------------------------------------------------------------
%%%
%%% cache primitives
%%%
%%%----------------------------------------------------------------------------

lifecycle(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:drop(Cache).

lifecycle_named(_Config) ->
   {ok, _Pid} = cache:start_link(test_named_cache, []),
   ok = cache:drop(test_named_cache).

lifecycle_global(_Config) ->
   {ok, _Pid} = cache:start_link({global, test_global_cache}, []),
   ok = cache:drop({global, test_global_cache}).

i(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Spec = cache:i(Cache),
   {heap, Heaps} = lists:keyfind(heap, 1, Spec),
   true = is_list(Heaps),
   true = length(Heaps) > 0,
   {expire, Expires} = lists:keyfind(expire, 1, Spec),
   true = is_list(Expires),
   {size, Sizes} = lists:keyfind(size, 1, Spec),
   true = lists:all(fun(S) -> is_integer(S) end, Sizes),
   {memory, Mems} = lists:keyfind(memory, 1, Spec),
   true = lists:all(fun(M) -> is_integer(M) end, Mems),
   ok = cache:drop(Cache).

i_specific_field(_Config) ->
   {ok, Cache} = cache:start_link([{n, 5}]),
   Heaps = cache:i(Cache, heap),
   5 = length(Heaps),
   Sizes = cache:i(Cache, size),
   5 = length(Sizes),
   ok = cache:drop(Cache).

heap(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   [{key, val}] = ets:lookup(cache:heap(Cache, 1), key),
   ok = cache:drop(Cache).

heap_recover_failure(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   badarg = cache:heap(Cache, 10000),
   [{key, val}] = ets:lookup(cache:heap(Cache, 1), key),
   ok = cache:drop(Cache).

purge(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:put(Cache, key, val),
   val = cache:get(Cache, key),
   cache:purge(Cache),
   undefined = cache:get(Cache, key),
   ok = cache:drop(Cache).

purge_multiple_keys(_Config) ->
   {ok, Cache} = cache:start_link([]),
   lists:foreach(fun(I) -> ok = cache:put(Cache, I, I * 10) end, lists:seq(1, 100)),
   100 = cache:get(Cache, 10),
   cache:purge(Cache),
   undefined = cache:get(Cache, 10),
   undefined = cache:get(Cache, 50),
   ok = cache:drop(Cache).

evict_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 10}]),
   ok = cache:put(Cache, key, val, 3),
   timer:sleep(1200),
   val = cache:lookup(Cache, key),
   timer:sleep(2200),
   undefined = cache:lookup(Cache, key),
   ok = cache:drop(Cache).

evict_no_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 3}, {ttl, 3}]),
   ok = cache:put(Cache, key, val),
   timer:sleep(1200),
   val = cache:lookup(Cache, key),
   timer:sleep(2200),
   undefined = cache:lookup(Cache, key),
   ok = cache:drop(Cache).

evict_ooc(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {size, 20}, {check, 1}]),
   ok = cache:put(Cache, key1, val),
   ok = cache:put(Cache, key2, val),
   [2 | _] = cache:i(Cache, size),
   timer:sleep(1200),
   [0, 2 | _] = cache:i(Cache, size),
   ok = cache:drop(Cache).

evict_oom(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {memory, 512}, {check, 1}]),
   LargeVal = binary:copy(<<0>>, 256),
   ok = cache:put(Cache, key1, LargeVal),
   ok = cache:put(Cache, key2, LargeVal),
   timer:sleep(1500),
   Sizes = cache:i(Cache, size),
   [0 | _] = Sizes,
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% cache basic i/o
%%%
%%%----------------------------------------------------------------------------

put(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   [{key, val}] = ets:lookup(cache:heap(Cache, 1), key),
   ok = cache:drop(Cache).

put_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put(Cache, key, val, 30),
   val = cache:get(Cache, key),
   TTL = cache:ttl(Cache, key),
   true = is_integer(TTL) andalso TTL > 0,
   ok = cache:drop(Cache).

put_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val, undefined, 5000),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

put_overwrite(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val1),
   val1 = cache:get(Cache, key),
   ok = cache:put(Cache, key, val2),
   val2 = cache:get(Cache, key),
   ok = cache:drop(Cache).

put_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:put_(Cache, key, val),
   val = cache:get(Cache, key),
   ok  = cache:drop(Cache).

put_async_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put_(Cache, key, val, 20),
   timer:sleep(50),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

put_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:put_(Cache, key, val, undefined, true),
   true = is_reference(Ref),
   receive
      {Ref, ok} -> ok
   after 5000 ->
      error(timeout)
   end,
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

get(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 10}]),
   ok  = cache:put(Cache, key1, val),
   val = cache:get(Cache, key1),
   ok  = cache:put(Cache, key2, val, 5),
   val = cache:get(Cache, key2),
   undefined = cache:get(Cache, unknown),
   ok = cache:drop(Cache).

get_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   val = cache:get(Cache, key, 5000),
   ok = cache:drop(Cache).

get_async(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   Ref = cache:get_(Cache, key),
   true = is_reference(Ref),
   receive
      {Ref, val} -> ok
   after 5000 ->
      error(timeout)
   end,
   ok = cache:drop(Cache).

get_async_miss(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:get_(Cache, unknown),
   true = is_reference(Ref),
   receive
      {Ref, undefined} -> ok
   after 5000 ->
      error(timeout)
   end,
   ok = cache:drop(Cache).

get_promotes_to_head(_Config) ->
   {ok, Cache} = cache:start_link([{n, 3}, {ttl, 9}]),
   ok = cache:put(Cache, key, val),
   Heap1 = cache:heap(Cache, 1),
   [{key, val}] = ets:lookup(Heap1, key),
   timer:sleep(3500),
   val = cache:get(Cache, key),
   NewHeap1 = cache:heap(Cache, 1),
   true = NewHeap1 =/= Heap1,
   [{key, val}] = ets:lookup(NewHeap1, key),
   ok = cache:drop(Cache).

lookup(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:put(Cache, key, val),
   val = cache:lookup(Cache, key),
   undefined = cache:lookup(Cache, unknown),
   ok = cache:drop(Cache).

lookup_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   val = cache:lookup(Cache, key, 5000),
   ok = cache:drop(Cache).

lookup_async(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   Ref = cache:lookup_(Cache, key),
   true = is_reference(Ref),
   receive
      {Ref, val} -> ok
   after 5000 ->
      error(timeout)
   end,
   ok = cache:drop(Cache).

has(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   true  = cache:has(Cache, key),
   false = cache:has(Cache, unknown),
   ok = cache:drop(Cache).

has_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   true = cache:has(Cache, key, 5000),
   false = cache:has(Cache, unknown, 5000),
   ok = cache:drop(Cache).

ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n,10}, {ttl, 60}]),
   ok = cache:put(Cache, key1, val),
   true = cache:ttl(Cache, key1) > 55,
   ok = cache:put(Cache, key2, val, 10),
   true = cache:ttl(Cache, key2) > 9,
   undefined = cache:ttl(Cache, unknown),
   ok = cache:drop(Cache).

ttl_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put(Cache, key, val),
   TTL = cache:ttl(Cache, key, 5000),
   true = TTL > 55,
   ok = cache:drop(Cache).

remove(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   true = cache:has(Cache, key),
   ok = cache:remove(Cache, key),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

remove_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   ok = cache:remove(Cache, key, 5000),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

remove_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   true = cache:has(Cache, key),
   ok = cache:remove_(Cache, key),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

remove_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   Ref = cache:remove_(Cache, key, true),
   true = is_reference(Ref),
   receive
      {Ref, ok} -> ok
   after 5000 ->
      error(timeout)
   end,
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

remove_nonexistent(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:remove(Cache, nonexistent_key),
   ok = cache:drop(Cache).

apply(_Config) ->
   {ok, Cache} = cache:start_link([]),
   val = cache:apply(Cache, key, fun(undefined) -> val end),
   val = cache:get(Cache, key),
   lav = cache:apply(Cache, key, fun(val) -> lav end),
   lav = cache:get(Cache, key),
   ok = cache:drop(Cache).

apply_returns_undefined(_Config) ->
   {ok, Cache} = cache:start_link([]),
   undefined = cache:apply(Cache, key, fun(_) -> undefined end),
   undefined = cache:get(Cache, key),
   ok = cache:drop(Cache).

apply_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   val = cache:apply(Cache, key, fun(undefined) -> val end, 5000),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

apply_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   cache:apply_(Cache, key, fun(undefined) -> val end),
   val = cache:get(Cache, key),
   cache:apply_(Cache, key, fun(val) -> lav end),
   lav = cache:get(Cache, key),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% cache extended i/o
%%%
%%%----------------------------------------------------------------------------

acc(_Config) ->
   {ok, Cache} = cache:start_link([]),
   undefined = cache:acc(Cache, key, 1),
   1  = cache:acc(Cache, key, 10),
   11 = cache:acc(Cache, key, 1),
   ok = cache:drop(Cache).

acc_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   undefined = cache:acc(Cache, key, 1, 5000),
   ok = cache:drop(Cache).

acc_tuple(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, {0, 0, 0}),
   {0, 0, 0} = cache:acc(Cache, key, [{1, 5}, {3, 10}]),
   {5, 0, 10} = cache:get(Cache, key),
   ok = cache:drop(Cache).

acc_tuple_single(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, {100, 200}),
   100 = cache:acc(Cache, key, 5),
   {105, 200} = cache:get(Cache, key),
   ok = cache:drop(Cache).

acc_badarg(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, <<"binary">>),
   badarg = cache:acc(Cache, key, 1),
   ok = cache:drop(Cache).

acc_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   cache:acc_(Cache, key, 1),
   cache:acc_(Cache, key, 10),
   11 = cache:get(Cache, key),
   ok = cache:drop(Cache).

acc_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   cache:acc_(Cache, key, 1, true),
   timer:sleep(50),
   1 = cache:get(Cache, key),
   ok = cache:drop(Cache).

set(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:set(Cache, key, val),
   val = cache:get(Cache, key),
   ok  = cache:drop(Cache).

set_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:set(Cache, key, val, 30),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

set_with_ttl_and_timeout(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:set(Cache, key, val, 30, 5000),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

set_async(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:set_(Cache, key, val),
   timer:sleep(50),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

set_async_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:set_(Cache, key, val, 20),
   timer:sleep(50),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

set_async_with_flag(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:set_(Cache, key, val, undefined, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

add(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:add(Cache, key, val),
   {error, conflict}  = cache:add(Cache, key, val),
   ok  = cache:drop(Cache).

add_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:add(Cache, key, val, 30),
   val = cache:get(Cache, key),
   {error, conflict} = cache:add(Cache, key, other),
   ok = cache:drop(Cache).

add_with_ttl_and_timeout(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:add(Cache, key, val, 30, 5000),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

add_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   cache:add_(Cache, key, val),
   cache:add_(Cache, key, non),
   val = cache:get(Cache, key),
   ok  = cache:drop(Cache).

add_async_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:add_(Cache, key, val, 20),
   timer:sleep(50),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

add_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:add_(Cache, key, val, undefined, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

replace(_Config) ->
   {ok, Cache} = cache:start_link([]),
   {error, not_found} = cache:replace(Cache, key, val),
   ok  = cache:set(Cache, key, val),
   ok  = cache:replace(Cache, key, val),
   ok  = cache:drop(Cache).

replace_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put(Cache, key, val),
   ok = cache:replace(Cache, key, newval, 30),
   newval = cache:get(Cache, key),
   ok = cache:drop(Cache).

replace_with_ttl_and_timeout(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put(Cache, key, val),
   ok = cache:replace(Cache, key, newval, 30, 5000),
   newval = cache:get(Cache, key),
   ok = cache:drop(Cache).

replace_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   cache:replace_(Cache, key, non),
   undefined = cache:get(Cache, key),
   ok  = cache:set(Cache, key, val),
   cache:replace_(Cache, key, foo),
   foo = cache:get(Cache, key),
   ok  = cache:drop(Cache).

replace_async_with_ttl(_Config) ->
   {ok, Cache} = cache:start_link([{n, 10}, {ttl, 60}]),
   ok = cache:put(Cache, key, val),
   ok = cache:replace_(Cache, key, newval, 20),
   timer:sleep(50),
   newval = cache:get(Cache, key),
   ok = cache:drop(Cache).

replace_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   Ref = cache:replace_(Cache, key, newval, undefined, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

append(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:append(Cache, key, a),
   [a] = cache:get(Cache, key),
   ok  = cache:append(Cache, key, b),
   [a, b] = cache:get(Cache, key),
   ok  = cache:append(Cache, key, c),
   [a, b, c] = cache:get(Cache, key),
   ok  = cache:drop(Cache).

append_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:append(Cache, key, a, 5000),
   [a] = cache:get(Cache, key),
   ok = cache:drop(Cache).

append_to_non_list(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, single),
   ok = cache:append(Cache, key, b),
   [single, b] = cache:get(Cache, key),
   ok = cache:drop(Cache).

append_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:append_(Cache, key, a),
   ok  = cache:append_(Cache, key, b),
   ok  = cache:append_(Cache, key, c),
   [a, b, c] = cache:get(Cache, key),
   ok  = cache:drop(Cache).

append_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:append_(Cache, key, a, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

prepend(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:prepend(Cache, key, a),
   [a] = cache:get(Cache, key),
   ok  = cache:prepend(Cache, key, b),
   [b, a] = cache:get(Cache, key),
   ok  = cache:prepend(Cache, key, c),
   [c, b, a] = cache:get(Cache, key),
   ok  = cache:drop(Cache).

prepend_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:prepend(Cache, key, a, 5000),
   [a] = cache:get(Cache, key),
   ok = cache:drop(Cache).

prepend_to_non_list(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, single),
   ok = cache:prepend(Cache, key, b),
   [b, single] = cache:get(Cache, key),
   ok = cache:drop(Cache).

prepend_(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok  = cache:prepend_(Cache, key, a),
   ok  = cache:prepend_(Cache, key, b),
   ok  = cache:prepend_(Cache, key, c),
   [c, b, a] = cache:get(Cache, key),
   ok  = cache:drop(Cache).

prepend_async_with_ack(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Ref = cache:prepend_(Cache, key, a, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

delete(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   true = cache:has(Cache, key),
   ok = cache:delete(Cache, key),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

delete_with_timeout(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   ok = cache:delete(Cache, key, 5000),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

delete_async(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   ok = cache:delete_(Cache, key),
   false = cache:has(Cache, key),
   ok = cache:drop(Cache).

delete_async_with_flag(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, key, val),
   Ref = cache:delete_(Cache, key, true),
   true = is_reference(Ref),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% cache policies
%%%
%%%----------------------------------------------------------------------------

mru_policy_get(_Config) ->
   {ok, Cache} = cache:start_link([{policy, mru}, {n, 3}, {ttl, 9}]),
   ok = cache:put(Cache, key, val),
   val = cache:get(Cache, key),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

mru_policy_no_promote(_Config) ->
   {ok, Cache} = cache:start_link([{policy, mru}, {n, 3}, {ttl, 9}]),
   ok = cache:put(Cache, key, val),
   Heap1 = cache:heap(Cache, 1),
   [{key, val}] = ets:lookup(Heap1, key),
   timer:sleep(3500),
   val = cache:get(Cache, key),
   NewHeap1 = cache:heap(Cache, 1),
   [] = ets:lookup(NewHeap1, key),
   ok = cache:drop(Cache).

ordered_set_type(_Config) ->
   {ok, Cache} = cache:start_link([{type, ordered_set}]),
   ok = cache:put(Cache, key1, val1),
   ok = cache:put(Cache, key2, val2),
   val1 = cache:get(Cache, key1),
   val2 = cache:get(Cache, key2),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% stats callback
%%%
%%%----------------------------------------------------------------------------

stats_fun_callback(_Config) ->
   Self = self(),
   StatsFun = fun(Event) -> Self ! {stats, Event} end,
   {ok, Cache} = cache:start_link([{stats, StatsFun}]),
   ok = cache:put(Cache, key, val),
   receive {stats, {cache, _, put}} -> ok after 1000 -> error(no_put_stats) end,
   val = cache:get(Cache, key),
   receive {stats, {cache, _, hit}} -> ok after 1000 -> error(no_hit_stats) end,
   undefined = cache:get(Cache, missing),
   receive {stats, {cache, _, miss}} -> ok after 1000 -> error(no_miss_stats) end,
   ok = cache:remove(Cache, key),
   receive {stats, {cache, _, remove}} -> ok after 1000 -> error(no_remove_stats) end,
   ok = cache:drop(Cache).

stats_mfa_callback(_Config) ->
   Self = self(),
   register(test_stats_receiver, Self),
   StatsMFA = {?MODULE, do_handle_stats},
   {ok, Cache} = cache:start_link([{stats, StatsMFA}]),
   ok = cache:put(Cache, key, val),
   receive {stats_mfa, {cache, _, put}} -> ok after 1000 -> error(no_put_stats) end,
   unregister(test_stats_receiver),
   ok = cache:drop(Cache).

do_handle_stats(Event) ->
   test_stats_receiver ! {stats_mfa, Event}.

%%%----------------------------------------------------------------------------
%%%
%%% cache configuration options
%%%
%%%----------------------------------------------------------------------------

custom_n_segments(_Config) ->
   {ok, Cache} = cache:start_link([{n, 5}]),
   Heaps = cache:i(Cache, heap),
   5 = length(Heaps),
   ok = cache:drop(Cache).

custom_check_interval(_Config) ->
   {ok, Cache} = cache:start_link([{check, 1}]),
   ok = cache:put(Cache, key, val),
   val = cache:get(Cache, key),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% various key/value types
%%%
%%%----------------------------------------------------------------------------

binary_keys(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, <<"binary_key">>, val),
   val = cache:get(Cache, <<"binary_key">>),
   ok = cache:drop(Cache).

tuple_keys(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, {compound, key, 1}, val),
   val = cache:get(Cache, {compound, key, 1}),
   ok = cache:drop(Cache).

integer_keys(_Config) ->
   {ok, Cache} = cache:start_link([]),
   ok = cache:put(Cache, 42, val),
   val = cache:get(Cache, 42),
   ok = cache:drop(Cache).

large_values(_Config) ->
   {ok, Cache} = cache:start_link([]),
   LargeVal = binary:copy(<<"x">>, 1024 * 1024),
   ok = cache:put(Cache, key, LargeVal),
   LargeVal = cache:get(Cache, key),
   ok = cache:drop(Cache).

many_keys(_Config) ->
   {ok, Cache} = cache:start_link([]),
   N = 1000,
   lists:foreach(
      fun(I) -> ok = cache:put(Cache, I, I * 10) end,
      lists:seq(1, N)
   ),
   lists:foreach(
      fun(I) ->
         Expected = I * 10,
         Expected = cache:get(Cache, I)
      end,
      lists:seq(1, N)
   ),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% heir functionality
%%%
%%%----------------------------------------------------------------------------

heir_receives_evicted_segments(_Config) ->
   Self = self(),
   {ok, Cache} = cache:start_link([{n, 3}, {ttl, 3}, {check, 1}, {heir, Self}]),
   ok = cache:put(Cache, key, val),
   receive
      {'ETS-TRANSFER', _Tab, _FromPid, evicted} -> ok
   after 10000 ->
      error(heir_not_notified)
   end,
   ok = cache:drop(Cache).

heir_named_process(_Config) ->
   Self = self(),
   register(test_heir_proc, Self),
   {ok, Cache} = cache:start_link([{n, 3}, {ttl, 3}, {check, 1}, {heir, test_heir_proc}]),
   ok = cache:put(Cache, key, val),
   receive
      {'ETS-TRANSFER', _Tab, _FromPid, evicted} -> ok
   after 10000 ->
      error(heir_not_notified)
   end,
   unregister(test_heir_proc),
   ok = cache:drop(Cache).

%%%----------------------------------------------------------------------------
%%%
%%% unknown message handling
%%%
%%%----------------------------------------------------------------------------

handle_unknown_call(_Config) ->
   {ok, Cache} = cache:start_link([]),
   try
      gen_server:call(Cache, unknown_message, 1000)
   catch
      exit:{timeout, _} -> ok
   end,
   true = is_process_alive(Cache),
   ok = cache:drop(Cache).

handle_unknown_cast(_Config) ->
   {ok, Cache} = cache:start_link([]),
   gen_server:cast(Cache, unknown_message),
   timer:sleep(50),
   true = is_process_alive(Cache),
   ok = cache:drop(Cache).

handle_unknown_info(_Config) ->
   {ok, Cache} = cache:start_link([]),
   Cache ! unknown_message,
   timer:sleep(50),
   true = is_process_alive(Cache),
   ok = cache:drop(Cache).

