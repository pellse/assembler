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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class CompletableFutureAdapter<ID, R, CR extends Collection<R>> implements AssemblerAdapter<ID, R, CompletableFuture<CR>> {

    private final Executor executor;
    private final Supplier<CR> collectionFactory;

    private CompletableFutureAdapter(Executor executor, Supplier<CR> collectionFactory) {
        this.executor = executor;
        this.collectionFactory = requireNonNull(collectionFactory);
    }

    @Override
    public CompletableFuture<CR> convertMapperSources(Stream<Supplier<Map<ID, ?>>> mapperSources,
                                                      Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder) {

        List<CompletableFuture<Map<ID, ?>>> mappingFutures = mapperSources
                .map(s -> executor != null ? supplyAsync(s, executor) : supplyAsync(s))
                .collect(toList());

        return allOf(mappingFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> domainObjectStreamBuilder.apply(
                        mappingFutures.stream()
                                .map(CompletableFuture::join)
                                .collect(toList()))
                        .collect(toCollection(collectionFactory)));
    }

    public static <ID, R> CompletableFutureAdapter<ID, R, List<R>> completableFutureAdapter() {
        return completableFutureAdapter(ArrayList::new, null);
    }

    public static <ID, R> CompletableFutureAdapter<ID, R, List<R>> completableFutureAdapter(Executor executor) {
        return completableFutureAdapter(ArrayList::new, executor);
    }

    public static <ID, R, CR extends Collection<R>> CompletableFutureAdapter<ID, R, CR> completableFutureAdapter(Supplier<CR> collectionFactory, Executor executor) {
        return new CompletableFutureAdapter<>(executor, collectionFactory);
    }
}
