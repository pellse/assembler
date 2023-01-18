package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.Rule
import io.github.pellse.reactive.assembler.QueryCache
import io.github.pellse.reactive.assembler.QueryCache.cached
import java.time.Duration

fun <ID, R> Rule<ID, R>.cached(): Rule<ID, R> = cached(this)
fun <ID, R> Rule<ID, R>.cached(cache: QueryCache<ID, R>): Rule<ID, R> = cached(this, cache)
fun <ID, R> Rule<ID, R>.cached(ttl: Duration): Rule<ID, R> = cached(this, ttl)
fun <ID, R> Rule<ID, R>.cached(cache: QueryCache<ID, R>, ttl: Duration): Rule<ID, R> = cached(this, cache, ttl)
