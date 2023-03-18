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

import io.github.pellse.reactive.assembler.Rule
import io.github.pellse.reactive.assembler.QueryCache
import io.github.pellse.reactive.assembler.QueryCache.cached
import java.time.Duration

fun <ID, R> Rule<ID, R>.cached(): Rule<ID, R> = cached(this)
fun <ID, R> Rule<ID, R>.cached(cache: QueryCache<ID, R>): Rule<ID, R> = cached(this, cache)
fun <ID, R> Rule<ID, R>.cached(ttl: Duration): Rule<ID, R> = cached(this, ttl)
fun <ID, R> Rule<ID, R>.cached(cache: QueryCache<ID, R>, ttl: Duration): Rule<ID, R> = cached(this, cache, ttl)
