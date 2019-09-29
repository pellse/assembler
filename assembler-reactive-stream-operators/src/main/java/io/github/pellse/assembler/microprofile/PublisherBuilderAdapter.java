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
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;

public class PublisherBuilderAdapter<ID, R> implements AssemblerAdapter<ID, R, PublisherBuilder<R>> {

    private final Executor executor;

    private PublisherBuilderAdapter(Executor executor) {
        this.executor = executor;
    }

    @Override
    public PublisherBuilder<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> mapperSourceSuppliers,
                                                    Function<List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        CompletionStage<List<Map<ID, ?>>> mapperResults = ReactiveStreams.fromIterable(mapperSourceSuppliers::iterator)
                .flatMapCompletionStage(this::toCompletableFuture)
                .collect(toList())
                .run();

        return ReactiveStreams.fromCompletionStage(mapperResults)
                .map(aggregateStreamBuilder)
                .flatMapIterable(stream -> stream::iterator);
    }

    private CompletableFuture<Map<ID, ?>> toCompletableFuture(Supplier<Map<ID, ?>> mapperSource) {
        return executor != null ? supplyAsync(mapperSource, executor) : supplyAsync(mapperSource);
    }

    public static <ID, R> AssemblerAdapter<ID, R, Publisher<R>> publisherAdapter() {
        return publisherAdapter(null);
    }

    public static <ID, R> AssemblerAdapter<ID, R, Publisher<R>> publisherAdapter(Executor executor) {
        PublisherBuilderAdapter<ID, R> adapter = publisherBuilderAdapter(executor);
        return (mapperSourceSuppliers, aggregateStreamBuilder) -> adapter.convertMapperSources(mapperSourceSuppliers, aggregateStreamBuilder).buildRs();
    }

    public static <ID, R> PublisherBuilderAdapter<ID, R> publisherBuilderAdapter() {
        return publisherBuilderAdapter(null);
    }

    public static <ID, R> PublisherBuilderAdapter<ID, R> publisherBuilderAdapter(Executor executor) {
        return new PublisherBuilderAdapter<>(executor);
    }
}
