package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.caching.CacheFactory
import io.github.pellse.reactive.assembler.caching.CacheFactory.cached

import io.github.pellse.reactive.assembler.RuleMapperSource
import org.reactivestreams.Publisher

fun <ID, IDC: Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(): RuleMapperSource<ID, IDC, R, RRC> =
    cached(this)

fun <ID, IDC: Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(mapFactory: () -> MutableMap<ID, RRC>): RuleMapperSource<ID, IDC, R, RRC> =
    cached(this, mapFactory)

fun <ID, IDC: Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(map: MutableMap<ID, RRC>): RuleMapperSource<ID, IDC, R, RRC> =
    cached(this, map)

fun <ID, IDC: Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(cache: CacheFactory<ID, R, RRC>): RuleMapperSource<ID, IDC, R, RRC> =
    cached(this, cache)
