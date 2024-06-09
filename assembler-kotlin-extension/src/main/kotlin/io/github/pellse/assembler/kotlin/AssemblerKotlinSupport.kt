/*
 * Copyright 2024 Sebastien Pelletier
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

import io.github.pellse.assembler.AssemblerBuilder.WithCorrelationIdResolverBuilder
import io.github.pellse.assembler.AssemblerBuilder.assemblerOf
import io.github.pellse.assembler.RuleMapper
import io.github.pellse.assembler.RuleMapper.oneToMany
import io.github.pellse.assembler.RuleMapper.oneToOne
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext
import io.github.pellse.assembler.RuleMapperSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux.fromIterable

inline fun <reified T> assembler(): WithCorrelationIdResolverBuilder<T> = assemblerOf(T::class.java)

fun <T, TC : Collection<T>, R> ((TC) -> Iterable<R>).toPublisher(): (TC) -> Publisher<R> = { ids -> fromIterable(this(ids)) }

fun <T, TC: Collection<T>, ID, R> ((TC) -> Publisher<R>).oneToOne(defaultResultProvider: (ID) -> R): RuleMapper<T, TC, ID, R, R> =
    oneToOne(this, defaultResultProvider)

fun <T, TC: Collection<T>, ID, R> RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>>.oneToOne(defaultResultProvider: (ID) -> R): RuleMapper<T, TC, ID, R, R> =
    oneToOne(this, defaultResultProvider)

fun <T, TC : Collection<T>, ID, EID, R> ((TC) -> Publisher<R>).oneToMany(idResolver: (R) -> EID): RuleMapper<T, TC, ID, R, List<R>> =
    oneToMany(idResolver, this)

fun <T, TC : Collection<T>, ID, EID, R> RuleMapperSource<T, TC, ID, EID, R, List<R>, OneToManyContext<T, TC, ID, EID, R, List<R>>>.oneToMany(idResolver: (R) -> EID): RuleMapper<T, TC, ID, R, List<R>> =
    oneToMany(idResolver, this)

fun <T, TC : Collection<T>, ID, EID, R, RC : Collection<R>> ((TC) -> Publisher<R>).oneToMany(idResolver: (R) -> EID, collectionFactory: () -> RC): RuleMapper<T, TC, ID, R, RC> =
    oneToMany(idResolver, this, collectionFactory)

fun <T, TC : Collection<T>, ID, EID, R, RC : Collection<R>> RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>>.oneToMany(idResolver: (R) -> EID, collectionFactory: () -> RC): RuleMapper<T, TC, ID, R, RC> =
    oneToMany(idResolver, this, collectionFactory)
