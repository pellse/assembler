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

package io.github.pellse.assembler.microprofile;

import io.github.pellse.assembler.AssemblerAdapter;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.assembler.microprofile.LazyPublisherBuilder.lazyPublisherBuilder;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams.fromCompletionStage;
import static org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams.fromIterable;

public final class PublisherBuilderAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, PublisherBuilder<R>> {

    private final boolean lazy;
    private final Executor executor;

    private PublisherBuilderAdapter(boolean lazy, Executor executor) {
        this.lazy = lazy;
        this.executor = executor;
    }

    @Override
    public PublisherBuilder<R> convertMapperSources(Supplier<Iterable<T>> topLevelEntitiesProvider,
                                                    Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                                    BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {
        return lazy
                ? lazyPublisherBuilder(() -> buildPublisher(topLevelEntitiesProvider, mapperSourcesBuilder, aggregateStreamBuilder))
                : buildPublisher(topLevelEntitiesProvider, mapperSourcesBuilder, aggregateStreamBuilder);
    }

    private PublisherBuilder<R> buildPublisher(Supplier<Iterable<T>> topLevelEntitiesProvider,
                                               Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                               BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        return fromCompletionStage(toCompletableFuture(topLevelEntitiesProvider))
                .flatMap(entities -> fromCompletionStage(
                        fromIterable(mapperSourcesBuilder.apply(entities)::iterator)
                                .flatMapCompletionStage(this::toCompletableFuture)
                                .toList()
                                .run())
                        .map(mapperResults -> aggregateStreamBuilder.apply(entities, mapperResults))
                        .flatMapIterable(stream -> stream::iterator));
    }

    private <U> CompletableFuture<U> toCompletableFuture(Supplier<U> mapperSource) {
        return executor != null ? supplyAsync(mapperSource, executor) : supplyAsync(mapperSource);
    }

    public static <T, ID, R> AssemblerAdapter<T, ID, R, Publisher<R>> publisherAdapter() {
        return publisherAdapter(null);
    }

    public static <T, ID, R> AssemblerAdapter<T, ID, R, Publisher<R>> publisherAdapter(boolean lazy) {
        return publisherAdapter(lazy, null);
    }

    public static <T, ID, R> AssemblerAdapter<T, ID, R, Publisher<R>> publisherAdapter(Executor executor) {
        return publisherAdapter(false, null);
    }

    public static <T, ID, R> AssemblerAdapter<T, ID, R, Publisher<R>> publisherAdapter(boolean lazy, Executor executor) {
        PublisherBuilderAdapter<T, ID, R> adapter = publisherBuilderAdapter(lazy, executor);
        return (topLevelEntitiesProvider, mapperSourceSuppliers, aggregateStreamBuilder) ->
                adapter.convertMapperSources(topLevelEntitiesProvider, mapperSourceSuppliers, aggregateStreamBuilder).buildRs();
    }

    public static <T, ID, R> PublisherBuilderAdapter<T, ID, R> publisherBuilderAdapter() {
        return publisherBuilderAdapter(null);
    }

    public static <T, ID, R> PublisherBuilderAdapter<T, ID, R> publisherBuilderAdapter(Executor executor) {
        return publisherBuilderAdapter(false, executor);
    }

    public static <T, ID, R> PublisherBuilderAdapter<T, ID, R> publisherBuilderAdapter(boolean lazy) {
        return publisherBuilderAdapter(lazy, null);
    }

    public static <T, ID, R> PublisherBuilderAdapter<T, ID, R> publisherBuilderAdapter(boolean lazy, Executor executor) {
        return new PublisherBuilderAdapter<>(lazy, executor);
    }
}
