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

import io.github.pellse.assembler.LifeCycleEventListener;
import io.github.pellse.assembler.LifeCycleEventSource;
import io.github.pellse.assembler.caching.CacheEvent.Updated;
import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.lang.System.Logger;
import java.util.List;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Collector;

import static io.github.pellse.assembler.LifeCycleEventSource.concurrentLifeCycleEventListener;
import static io.github.pellse.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.assembler.caching.AutoCacheFactory.OnErrorContinue.onErrorContinue;
import static io.github.pellse.assembler.caching.CacheEvent.toCacheEvent;
import static io.github.pellse.util.ObjectUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.isEmpty;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    Logger logger = getLogger(CacheFactory.class.getName());

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> autoCache(Supplier<Flux<R>> dataSourceSupplier) {
        return autoCache(dataSourceSupplier.get());
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheTransformer<ID, R, RRC, CTX> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, __ -> true, identity());
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U> CacheTransformer<ID, R, RRC, CTX> autoCache(
            Supplier<Flux<U>> dataSourceSupplier,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return autoCache(dataSourceSupplier.get(), isAddOrUpdateEvent, cacheEventValueExtractor);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U> CacheTransformer<ID, R, RRC, CTX> autoCache(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return autoCache(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)), null, null, null, null, null);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>, U extends CacheEvent<R>> CacheTransformer<ID, R, RRC, CTX> autoCache(
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
                    .transform(scheduleOn(scheduler, Flux::publishOn))
                    .transform(requireNonNullElse(windowingStrategy, flux -> flux.window(MAX_WINDOW_SIZE)))
                    .flatMap(flux -> flux.collect(partitioningBy(Updated.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), mapCollector), toMap(eventMap.get(false), mapCollector)))
                    .transform(requireNonNullElse(errorHandler, onErrorContinue(AutoCacheFactory::logError)).toFluxErrorHandler());

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

    private static <T> Function<Flux<T>, Flux<T>> scheduleOn(Scheduler scheduler, BiFunction<Flux<T>, Scheduler, Flux<T>> scheduleFunction) {
        return flux -> scheduler != null ? scheduleFunction.apply(flux, scheduler) : flux;
    }

    private static void logError(Throwable t, Object faultyData) {
        logger.log(WARNING, "Error while updating cache in autoCache() with " + faultyData, t);
    }

    sealed interface ErrorHandler {
        <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler();
    }

    @FunctionalInterface
    interface WindowingStrategy<R> extends Function<Flux<R>, Flux<Flux<R>>> {
    }

    record OnErrorContinue<E extends Throwable>(
            Predicate<E> errorPredicate,
            BiConsumer<Throwable, Object> errorConsumer) implements ErrorHandler {

        public static OnErrorContinue<?> onErrorContinue() {
            return onErrorContinue(doNothing());
        }

        public static OnErrorContinue<?> onErrorContinue(Consumer<Throwable> errorConsumer) {
            return onErrorContinue((t, o) -> errorConsumer.accept(t));
        }

        public static OnErrorContinue<?> onErrorContinue(BiConsumer<Throwable, Object> errorConsumer) {
            return onErrorContinue(e -> true, errorConsumer);
        }

        public static <E extends Throwable> OnErrorContinue<E> onErrorContinue(Predicate<E> errorPredicate, BiConsumer<Throwable, Object> errorConsumer) {
            return new OnErrorContinue<>(errorPredicate, errorConsumer);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux.onErrorContinue(errorPredicate(), errorConsumer());
        }
    }

    record OnErrorResume(
            Predicate<Throwable> errorPredicate,
            Consumer<Throwable> errorConsumer) implements ErrorHandler {

        public static OnErrorResume onErrorResume() {
            return onErrorResume(doNothing());
        }

        public static OnErrorResume onErrorResume(Consumer<Throwable> errorConsumer) {
            return onErrorResume(__ -> true, errorConsumer);
        }

        public static OnErrorResume onErrorResume(Predicate<Throwable> errorPredicate, Consumer<Throwable> errorConsumer) {
            return new OnErrorResume(errorPredicate, errorConsumer);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux
                    .doOnError(errorPredicate(), errorConsumer())
                    .onErrorResume(errorPredicate(), __ -> Mono.empty());
        }
    }

    record OnErrorMap(Function<? super Throwable, ? extends Throwable> mapper) implements ErrorHandler {

        public static OnErrorMap onErrorMap(Function<? super Throwable, ? extends Throwable> mapper) {
            return new OnErrorMap(mapper);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux.onErrorMap(mapper());
        }
    }

    record OnErrorStop() implements ErrorHandler {

        public static OnErrorStop onErrorStop() {
            return new OnErrorStop();
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return Flux::onErrorStop;
        }
    }
}
