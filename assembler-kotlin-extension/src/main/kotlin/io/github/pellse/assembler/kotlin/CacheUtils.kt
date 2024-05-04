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

package io.github.pellse.assembler.kotlin

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext
import io.github.pellse.assembler.RuleMapperSource
import io.github.pellse.assembler.caching.CacheFactory
import io.github.pellse.assembler.caching.CacheFactory.cached
import io.github.pellse.assembler.caching.CacheFactory.cachedMany
import org.reactivestreams.Publisher
import java.util.function.Function

fun <T, TC : Collection<T>, ID, R> ((TC) -> Publisher<R>).cached(
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>
): RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> = cached(this, *delegateCacheFactories)

fun <T, TC : Collection<T>, ID, R> ((TC) -> Publisher<R>).cached(
    cache: CacheFactory<ID, R, R>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, R>, CacheFactory<ID, R, R>>
): RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> = cached(this, cache, *delegateCacheFactories)

fun <T, TC : Collection<T>, ID, EID, R, RC: Collection<R>> ((TC) -> Publisher<R>).cachedMany(
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>
): RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> = cachedMany(this, *delegateCacheFactories)

fun <T, TC : Collection<T>, ID, EID, R, RC: Collection<R>> ((TC) -> Publisher<R>).cachedMany(
    cache: CacheFactory<ID, R, RC>,
    vararg delegateCacheFactories: Function<CacheFactory<ID, R, RC>, CacheFactory<ID, R, RC>>
): RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> = cachedMany(this, cache, *delegateCacheFactories)
