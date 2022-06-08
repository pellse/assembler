package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.caching.CacheFactory
import io.github.pellse.reactive.assembler.caching.CacheFactory.cached

import io.github.pellse.reactive.assembler.RuleMapperSource
import org.reactivestreams.Publisher
import java.util.function.Function

fun <ID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, IDC, R, RRC> = cached(this, *delegateCacheFactories)

fun <ID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    mapFactory: () -> MutableMap<ID, RRC>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, IDC, R, RRC> = cached(this, mapFactory, *delegateCacheFactories)

fun <ID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    map: MutableMap<ID, RRC>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, IDC, R, RRC> = cached(this, map, *delegateCacheFactories)

fun <ID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    cache: CacheFactory<ID, R, RRC>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, IDC, R, RRC> = cached(this, cache, *delegateCacheFactories)
