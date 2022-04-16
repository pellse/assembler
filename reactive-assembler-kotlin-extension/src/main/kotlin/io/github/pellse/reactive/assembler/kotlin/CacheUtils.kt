package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.CacheFactory
import io.github.pellse.reactive.assembler.CacheFactory.cached

import io.github.pellse.reactive.assembler.RuleMapperSource
import org.reactivestreams.Publisher

fun <ID, R, RRC> ((List<ID>) -> Publisher<R>).cached(): RuleMapperSource<ID, List<ID>, R, RRC> = cached(this)

fun <ID, R, RRC> ((List<ID>) -> Publisher<R>).cached(mapFactory: () -> MutableMap<ID, Collection<R>>): RuleMapperSource<ID, List<ID>, R, RRC> = cached(this, mapFactory)

fun <ID, R, RRC> ((List<ID>) -> Publisher<R>).cached(map: MutableMap<ID, Collection<R>>): RuleMapperSource<ID, List<ID>, R, RRC> = cached(this, map)

fun <ID, R, RRC> ((List<ID>) -> Publisher<R>).cached(cache: CacheFactory<ID, R>): RuleMapperSource<ID, List<ID>, R, RRC> = cached(this, cache)
