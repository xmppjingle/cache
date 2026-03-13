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
-module(cache_heap_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
   [Test || {Test, NAry} <- ?MODULE:module_info(exports),
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      Test =/= init_per_testcase,
      Test =/= end_per_testcase,
      NAry =:= 1
   ].

init_per_testcase(_, Config) ->
   meck:new(cache_util, [passthrough]),
   meck:expect(cache_util, now, fun() -> 0 end),
   Config.

end_per_testcase(_, _) ->
   meck:unload(cache_util).

%%
heap_init(_) ->
   {heap, set, 10, 6, 100, 1280, Segments} = cache_heap:new(set, 10, 60, 1000, 102400),
   Expect = lists:seq(6, 60, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

heap_init_ordered_set(_) ->
   {heap, ordered_set, 5, 10, undefined, undefined, Segments} = cache_heap:new(ordered_set, 5, 50, undefined, undefined),
   Expect = lists:seq(10, 50, 10),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)],
   5 = queue:len(Segments).

heap_init_no_quotas(_) ->
   {heap, set, 3, 20, undefined, undefined, Segments} = cache_heap:new(set, 3, 60, undefined, undefined),
   Expect = lists:seq(20, 60, 20),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

%%
heap_purge(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {heap, set, 10, 6, 100, 1280, Segments} = cache_heap:purge(undefined, Heap),
   Expect = lists:seq(6, 60, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

heap_purge_preserves_config(_) ->
   Heap = cache_heap:new(set, 5, 50, 500, 51200),
   {heap, set, 5, 10, 100, 1280, Segments} = cache_heap:purge(undefined, Heap),
   5 = queue:len(Segments).

%%
heap_refs(_) ->
   Heap = cache_heap:new(set, 3, 30, undefined, undefined),
   Refs = cache_heap:refs(Heap),
   3 = length(Refs),
   [30, 20, 10] = [Expire || {Expire, _} <- Refs].

%%
heap_slip_ok(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {ok, Heap} = cache_heap:slip(undefined, Heap).

%%
heap_slip_ttl(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   meck:expect(cache_util, now, fun() -> 6 end),
   {ttl,
      {heap, set, 10, 6, 100, 1280, Segments}
   } = cache_heap:slip(undefined, Heap),
   Expect = lists:seq(12, 66, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

heap_slip_ttl_boundary(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   meck:expect(cache_util, now, fun() -> 5 end),
   {ok, _} = cache_heap:slip(undefined, Heap).

heap_slip_ooc(_) ->
   Heap = cache_heap:new(set, 3, 30, 6, undefined),
   {heap, set, 3, 10, 2, undefined, Segments} = Heap,
   {_, Ref} = queue:last(Segments),
   ets:insert(Ref, {a, 1}),
   ets:insert(Ref, {b, 2}),
   {ooc, _NewHeap} = cache_heap:slip(undefined, Heap).

heap_slip_oom(_) ->
   Heap = cache_heap:new(set, 3, 30, undefined, 256),
   {heap, set, 3, 10, undefined, MemQuota, Segments} = Heap,
   {_, Ref} = queue:last(Segments),
   lists:foreach(
      fun(I) -> ets:insert(Ref, {I, binary:copy(<<0>>, 100)}) end,
      lists:seq(1, 100)
   ),
   true = ets:info(Ref, memory) >= MemQuota,
   {oom, _NewHeap} = cache_heap:slip(undefined, Heap).

heap_slip_no_false_ooc_when_undefined(_) ->
   Heap = cache_heap:new(set, 3, 30, undefined, undefined),
   {heap, set, 3, 10, undefined, undefined, Segments} = Heap,
   {_, Ref} = queue:last(Segments),
   lists:foreach(
      fun(I) -> ets:insert(Ref, {I, I}) end,
      lists:seq(1, 1000)
   ),
   {ok, _} = cache_heap:slip(undefined, Heap).

%%
heap_split_last(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {{60, _}, Segments} = cache_heap:split(Heap),
   Expect = lists:seq(6, 54, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

%%
heap_split_45(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {{48, _}, Segments} = cache_heap:split(45, Heap),
   Expect = lists:seq(6, 42, 6) ++ lists:seq(54, 60, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

%%
heap_split_15(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {{18, _}, Segments} = cache_heap:split(15, Heap),
   Expect = lists:seq(6, 12, 6) ++ lists:seq(24, 60, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

%%
heap_split_65(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {{60, _}, Segments} = cache_heap:split(65, Heap),
   Expect = lists:seq(6, 54, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

%%
heap_split_0(_) ->
   Heap = cache_heap:new(set, 10, 60, 1000, 102400),
   {{6, _}, Segments} = cache_heap:split(0, Heap),
   Expect = lists:seq(12, 60, 6),
   Expect = [Expire || {Expire, _} <- queue:to_list(Segments)].

heap_split_returns_valid_refs(_) ->
   Heap = cache_heap:new(set, 5, 50, undefined, undefined),
   {{_, Ref}, _Tail} = cache_heap:split(Heap),
   true = is_reference(Ref),
   true = ets:insert(Ref, {test, value}),
   [{test, value}] = ets:lookup(Ref, test).

