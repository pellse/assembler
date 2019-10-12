/*
 * Copyright 2018 Sebastien Pelletier
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

package io.github.pellse.assembler.akkastream;

import akka.stream.javadsl.Source;
import io.github.pellse.assembler.AssemblerAdapter;
import io.github.pellse.util.function.checked.CheckedSupplier;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static akka.stream.javadsl.Source.*;
import static java.util.stream.Collectors.toList;

public final class AkkaSourceAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, Source<R, ?>> {

    private final UnaryOperator<Source<Map<ID, ?>, ?>> sourceTransformer;

    private AkkaSourceAdapter(UnaryOperator<Source<Map<ID, ?>, ?>> sourceTransformer) {
        this.sourceTransformer = sourceTransformer;
    }

    @Override
    public Source<R, ?> convertMapperSources(CheckedSupplier<Iterable<T>, Throwable> topLevelEntitiesProvider,
                                             Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                             BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {
        return lazily(() -> single(topLevelEntitiesProvider.get()))
                .flatMapConcat(entities -> zipN(mapperSourcesBuilder.apply(entities)
                        .map(this::createAkkaSource)
                        .collect(toList()))
                        .map(mapperResults -> aggregateStreamBuilder.apply(entities, mapperResults)))
                .flatMapConcat(s -> from(s::iterator));
    }

    private Source<Map<ID, ?>, ?> createAkkaSource(Supplier<Map<ID, ?>> mappingSupplier) {
        return sourceTransformer.apply(lazily(() -> single(mappingSupplier.get())));
    }

    public static <T, ID, R> AkkaSourceAdapter<T, ID, R> akkaSourceAdapter() {
        return akkaSourceAdapter(false);
    }

    public static <T, ID, R> AkkaSourceAdapter<T, ID, R> akkaSourceAdapter(boolean async) {
        return akkaSourceAdapter(source -> async ? source.async() : source);
    }

    public static <T, ID, R> AkkaSourceAdapter<T, ID, R> akkaSourceAdapter(UnaryOperator<Source<Map<ID, ?>, ?>> sourceTransformer) {
        return new AkkaSourceAdapter<>(sourceTransformer);
    }
}
