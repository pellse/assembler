package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.reactive.assembler.AssemblerBuilder.WithIdExtractorBuilder
import io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf

inline fun <reified T> assembler(): WithIdExtractorBuilder<T> = assemblerOf(T::class.java)
