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

package io.github.pellse.assembler.flux;

import io.github.pellse.assembler.AssemblerAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.zip;
import static reactor.core.publisher.Mono.fromSupplier;
import static reactor.core.scheduler.Schedulers.parallel;

public final class FluxAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, Flux<R>> {

    private final Scheduler scheduler;

    private FluxAdapter(Scheduler scheduler) {
        this.scheduler = requireNonNull(scheduler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<R> convertMapperSources(Supplier<Iterable<T>> topLevelEntitiesProvider,
                                        Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                        BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        return toMono(topLevelEntitiesProvider)
                .flatMapMany(entities ->
                        zip(mapperSourcesBuilder.apply(entities).map(this::toMono).collect(toList()),
                                mapperResults -> aggregateStreamBuilder.apply(entities, Stream.of(mapperResults)
                                        .map(mapResult -> (Map<ID, ?>) mapResult)
                                        .collect(toList()))))
                .flatMap(Flux::fromStream);
    }

    private <U> Mono<U> toMono(Supplier<U> mapperSource) {
        return fromSupplier(mapperSource).subscribeOn(scheduler);
    }

    public static <T, ID, R> FluxAdapter<T, ID, R> fluxAdapter() {
        return fluxAdapter(parallel());
    }

    public static <T, ID, R> FluxAdapter<T, ID, R> fluxAdapter(Scheduler scheduler) {
        return new FluxAdapter<>(scheduler);
    }
}
