package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.AssemblerBuilder.WithIdExtractorBuilder
import io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf
import io.github.pellse.reactive.assembler.RuleMapper
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.RuleMapperSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux.fromIterable

inline fun <reified T> assembler(): WithIdExtractorBuilder<T> = assemblerOf(T::class.java)

fun <ID, IDC : Collection<ID>, R> ((IDC) -> Iterable<R>).toPublisher(): (IDC) -> Publisher<R> = { ids -> fromIterable(this(ids)) }

fun <ID, IDC : Collection<ID>, R> ((IDC) -> Publisher<R>).oneToOne(defaultResultProvider: (ID) -> R): RuleMapper<ID, IDC, R, R> =
    oneToOne(this, defaultResultProvider)

fun <ID, IDC : Collection<ID>, R> RuleMapperSource<ID, IDC, R, R>.oneToOne(defaultResultProvider: (ID) -> R): RuleMapper<ID, IDC, R, R> =
    oneToOne(this, defaultResultProvider)

fun <ID, IDC : Collection<ID>, R> ((IDC) -> Publisher<R>).oneToMany(): RuleMapper<ID, IDC, R, List<R>> =
    oneToMany(this)

fun <ID, IDC : Collection<ID>, R> RuleMapperSource<ID, IDC, R, List<R>>.oneToMany(): RuleMapper<ID, IDC, R, List<R>> =
    oneToMany(this)

fun <ID, IDC : Collection<ID>, R, RC : Collection<R>> ((IDC) -> Publisher<R>).oneToMany(collectionFactory: () -> RC): RuleMapper<ID, IDC, R, RC> =
    oneToMany(this, collectionFactory)

fun <ID, IDC : Collection<ID>, R, RC : Collection<R>> RuleMapperSource<ID, IDC, R, RC>.oneToMany(collectionFactory: () -> RC): RuleMapper<ID, IDC, R, RC> =
    oneToMany(this, collectionFactory)
