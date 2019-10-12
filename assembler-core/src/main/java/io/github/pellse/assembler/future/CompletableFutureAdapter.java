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

package io.github.pellse.assembler.future;

import io.github.pellse.assembler.AssemblerAdapter;
import io.github.pellse.util.function.checked.CheckedSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public final class CompletableFutureAdapter<T, ID, R, CR extends Collection<R>> implements AssemblerAdapter<T, ID, R, CompletableFuture<CR>> {

    private final Executor executor;
    private final Supplier<CR> collectionFactory;

    private CompletableFutureAdapter(Executor executor, Supplier<CR> collectionFactory) {
        this.executor = executor;
        this.collectionFactory = requireNonNull(collectionFactory);
    }

    @Override
    public CompletableFuture<CR> convertMapperSources(CheckedSupplier<Iterable<T>, Throwable> topLevelEntitiesProvider,
                                                      Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                                      BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {
        return supplyAsync(topLevelEntitiesProvider)
                .thenCompose(entities -> {
                    List<CompletableFuture<Map<ID, ?>>> mappingFutures = mapperSourcesBuilder.apply(entities)
                            .map(this::toCompletableFuture)
                            .collect(toList());

                    return allOf(mappingFutures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> aggregateStreamBuilder.apply(entities,
                                    mappingFutures.stream()
                                            .map(CompletableFuture::join)
                                            .collect(toList()))
                                    .collect(toCollection(collectionFactory)));
                });
    }

    private <U> CompletableFuture<U> toCompletableFuture(Supplier<U> mapperSource) {
        return executor != null ? supplyAsync(mapperSource, executor) : supplyAsync(mapperSource);
    }

    public static <T, ID, R> CompletableFutureAdapter<T, ID, R, List<R>> completableFutureAdapter() {
        return completableFutureAdapter(ArrayList::new, null);
    }

    public static <T, ID, R> CompletableFutureAdapter<T, ID, R, List<R>> completableFutureAdapter(Executor executor) {
        return completableFutureAdapter(ArrayList::new, executor);
    }

    public static <T, ID, R, CR extends Collection<R>> CompletableFutureAdapter<T, ID, R, CR> completableFutureAdapter(Supplier<CR> collectionFactory, Executor executor) {
        return new CompletableFutureAdapter<>(executor, collectionFactory);
    }
}
