/*
 * Copyright 2023 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.caching.CacheFactory
import io.github.pellse.reactive.assembler.caching.CacheFactory.cached

import io.github.pellse.reactive.assembler.RuleMapperSource
import io.github.pellse.reactive.assembler.caching.CacheFactory.cache
import org.reactivestreams.Publisher
import java.util.function.Function

fun <T, TC : Collection<T>, ID, EID, R, RRC> ((TC) -> Publisher<R>).cached(
    vararg delegateCacheFactories: Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>
): RuleMapperSource<T, TC, ID, EID, R, RRC> = cached(this, *delegateCacheFactories)

fun <T, TC : Collection<T>, ID, EID, R, RRC> ((TC) -> Publisher<R>).cached(
    mapFactory: () -> MutableMap<ID, List<R>>,
    vararg delegateCacheFactories: Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>
): RuleMapperSource<T, TC, ID, EID, R, RRC> = cached(this, cache(mapFactory), *delegateCacheFactories)

fun <T, TC : Collection<T>, ID, EID, R, RRC> ((TC) -> Publisher<R>).cached(
    cache: CacheFactory<T, ID, R, RRC>,
    vararg delegateCacheFactories: Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>>
): RuleMapperSource<T, TC, ID, EID, R, RRC> = cached(this, cache, *delegateCacheFactories)
