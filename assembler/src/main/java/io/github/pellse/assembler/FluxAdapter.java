/*
 * Copyright 2024 Sebastien Pelletier
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

package io.github.pellse.assembler;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.github.pellse.util.reactive.ReactiveUtils.*;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.zip;
import static reactor.core.publisher.Mono.from;
import static reactor.core.scheduler.Schedulers.*;

public interface FluxAdapter {

    static <T, K, R> AssemblerAdapter<T, K, R> fluxAdapter() {
        return fluxAdapter(isVirtualThreadSupported());
    }

    static <T, K, R> AssemblerAdapter<T, K, R> fluxAdapter(boolean useVirtualThreads) {
        return fluxAdapter(useVirtualThreads ? getVirtualThreadScheduler() : scheduler(() -> newBoundedElastic(getRuntime().availableProcessors(), MAX_VALUE, "assemblerScheduler")));
    }

    static <T, K, R> AssemblerAdapter<T, K, R> fluxAdapter(Scheduler scheduler) {

        return (topLevelEntitiesProvider, subQueryMapperBuilder, aggregateStreamBuilder) -> Flux.from(topLevelEntitiesProvider)
                .collectList()
                .flatMapMany(entities ->
                        zip(subQueryMapperBuilder.apply(entities).map(publisher -> from(publisher).transform(subscribeMonoOn(scheduler))).toList(),
                                mapperResults -> aggregateStreamBuilder.apply(entities, toMapperResultList(mapperResults))))
                .flatMapSequential(Flux::fromStream);
    }

    @SuppressWarnings("unchecked")
    private static <K> List<Map<K, ?>> toMapperResultList(Object[] mapperResults) {

        return Stream.of(mapperResults)
                .map(mapResult -> (Map<K, ?>) mapResult)
                .collect(toList());
    }

    private static Scheduler getVirtualThreadScheduler() {
        return scheduler(() -> fromExecutorService(newVirtualThreadPerTaskExecutor()));
    }
}
