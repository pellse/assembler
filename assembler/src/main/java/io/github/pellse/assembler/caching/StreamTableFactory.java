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

package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.ErrorHandler;
import io.github.pellse.assembler.LifeCycleEventListener;
import io.github.pellse.assembler.LifeCycleEventSource;
import io.github.pellse.assembler.WindowingStrategy;
import io.github.pellse.assembler.caching.CacheEvent.Updated;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.lang.System.Logger;
import java.util.List;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Collector;

import static io.github.pellse.assembler.ErrorHandler.OnErrorContinue.onErrorContinue;
import static io.github.pellse.assembler.LifeCycleEventSource.concurrentLifeCycleEventListener;
import static io.github.pellse.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.assembler.caching.CacheEvent.toCacheEvent;
import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static io.github.pellse.util.reactive.ReactiveUtils.publishFluxOn;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;

public interface StreamTableFactory {

    int MAX_WINDOW_SIZE = 1;

    Logger logger = getLogger(StreamTableFactory.class.getName());

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> streamTable(Supplier<Flux<R>> dataSourceSupplier) {
        return streamTable(dataSourceSupplier.get());
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> streamTable(Flux<R> dataSource) {
        return streamTable(dataSource, __ -> true, identity());
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U> CacheTransformer<ID, R, RRC, CTX> streamTable(
            Supplier<Flux<U>> dataSourceSupplier,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return streamTable(dataSourceSupplier.get(), isAddOrUpdateEvent, cacheEventValueExtractor);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U> CacheTransformer<ID, R, RRC, CTX> streamTable(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return streamTable(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)), null, null, null, null, null);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U extends CacheEvent<R>> CacheTransformer<ID, R, RRC, CTX> streamTable(
            Flux<U> dataSource,
            WindowingStrategy<U> windowingStrategy,
            ErrorHandler errorHandler,
            LifeCycleEventSource lifeCycleEventSource,
            Scheduler scheduler,
            Function<CacheFactory<ID, R, RRC, CTX>, CacheFactory<ID, R, RRC, CTX>> cacheTransformer) {

        return cacheFactory -> cacheContext -> {

            final var mapCollector = cacheContext.mapCollector();

            final var cache = ofNullable(cacheTransformer)
                    .map(transformer -> transformer.apply(cacheFactory))
                    .orElseGet(() -> cacheContext.cacheTransformer().apply(cacheFactory))
                    .create(cacheContext);

            final var cacheSourceFlux = requireNonNull(dataSource, "dataSource cannot be null")
                    .transform(publishFluxOn(scheduler))
                    .transform(requireNonNullElse(windowingStrategy, flux -> flux.window(MAX_WINDOW_SIZE)))
                    .flatMap(flux -> flux.collect(partitioningBy(Updated.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), mapCollector), toMap(eventMap.get(false), mapCollector)))
                    .transform(requireNonNullElse(errorHandler, onErrorContinue(StreamTableFactory::logError)).toFluxErrorHandler());

            requireNonNullElse(lifeCycleEventSource, LifeCycleEventListener::start)
                    .addLifeCycleEventListener(concurrentLifeCycleEventListener(lifeCycleEventAdapter(cacheSourceFlux, Flux::subscribe, Disposable::dispose)));

            return cache;
        };
    }

    private static <ID, R, RRC> Map<ID, RRC> toMap(List<? extends CacheEvent<R>> cacheEvents, IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector) {
        return isEmpty(cacheEvents) ? Map.of() : cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(mapCollector.apply(cacheEvents.size()));
    }

    private static void logError(Throwable t, Object faultyData) {
        logger.log(WARNING, "Error while updating cache in streamTable() with " + faultyData, t);
    }
}