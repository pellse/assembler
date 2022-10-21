package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.caching.CacheFactory
import io.github.pellse.reactive.assembler.caching.CacheFactory.cached

import io.github.pellse.reactive.assembler.RuleMapperSource
import io.github.pellse.reactive.assembler.caching.Cache.cache
import org.reactivestreams.Publisher
import java.util.function.Function

fun <ID, EID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, EID, IDC, R, RRC> = cached(this, *delegateCacheFactories)

fun <ID, EID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    mapFactory: () -> MutableMap<ID, List<R>>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, EID, IDC, R, RRC> = cached(this, cache(mapFactory), *delegateCacheFactories)

fun <ID, EID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    map: MutableMap<ID, List<R>>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, EID, IDC, R, RRC> = cached(this, map, *delegateCacheFactories)

fun <ID, EID, IDC : Collection<ID>, R, RRC> ((IDC) -> Publisher<R>).cached(
    cache: CacheFactory<ID, R, RRC>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>>
): RuleMapperSource<ID, EID, IDC, R, RRC> = cached(this, cache, *delegateCacheFactories)
