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

package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.zip;
import static reactor.core.publisher.Mono.from;
import static reactor.core.scheduler.Schedulers.parallel;

public final class FluxAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, Flux<R>> {

    private final Scheduler scheduler;

    private FluxAdapter(Scheduler scheduler) {
        this.scheduler = requireNonNull(scheduler);
    }

    public static <T, ID, R> FluxAdapter<T, ID, R> fluxAdapter() {
        return fluxAdapter(parallel());
    }

    public static <T, ID, R> FluxAdapter<T, ID, R> fluxAdapter(Scheduler scheduler) {
        return new FluxAdapter<>(scheduler);
    }

    @SuppressWarnings("unchecked")
    private static <ID> List<Map<ID, ?>> toMapperResultList(Object[] mapperResults) {
        return Stream.of(mapperResults)
                .map(mapResult -> (Map<ID, ?>) mapResult)
                .collect(toList());
    }

    @Override
    public Flux<R> convertSubQueryMappers(
            Publisher<T> topLevelEntitiesProvider,
            Function<Iterable<T>, Stream<Publisher<? extends Map<ID, ?>>>> subQueryMapperBuilder,
            BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        return Flux.from(topLevelEntitiesProvider)
                .collectList()
                .flatMapMany(entities ->
                        zip(subQueryMapperBuilder.apply(entities).map(publisher -> from(publisher).subscribeOn(scheduler)).collect(toList()),
                                mapperResults -> aggregateStreamBuilder.apply(entities, toMapperResultList(mapperResults))))
                .flatMap(Flux::fromStream);
    }
}
