package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.Cache
import io.github.pellse.reactive.assembler.Cache.cached
import io.github.pellse.reactive.assembler.RuleMapperSource
import org.reactivestreams.Publisher

fun <ID, R> ((List<ID>) -> Publisher<R>).cached(): RuleMapperSource<ID, List<ID>, R> = cached(this)

fun <ID, R> ((List<ID>) -> Publisher<R>).cached(mapFactory: () -> MutableMap<ID, List<R>>): RuleMapperSource<ID, List<ID>, R> = cached(this, mapFactory)

fun <ID, R> ((List<ID>) -> Publisher<R>).cached(map: MutableMap<ID, List<R>>): RuleMapperSource<ID, List<ID>, R> = cached(this, map)

fun <ID, R> ((List<ID>) -> Publisher<R>).cached(cache: Cache<ID, R>): RuleMapperSource<ID, List<ID>, R> = cached(this, cache)

fun <ID, R> ((List<ID>) -> Publisher<R>).cached(
    getAllPresent: (Iterable<ID>) -> Map<ID, List<R>>,
    putAll: (Map<ID, List<R>>) -> Unit
): RuleMapperSource<ID, List<ID>, R> = cached(this, getAllPresent, putAll)
