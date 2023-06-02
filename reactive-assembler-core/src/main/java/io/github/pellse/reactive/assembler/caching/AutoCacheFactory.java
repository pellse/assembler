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

package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.LifeCycleEventListener;
import io.github.pellse.reactive.assembler.LifeCycleEventSource;
import io.github.pellse.reactive.assembler.caching.CacheEvent.Updated;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.lang.System.Logger;
import java.util.List;
import java.util.Map;
import java.util.function.*;

import static io.github.pellse.reactive.assembler.LifeCycleEventSource.concurrentLifeCycleEventListener;
import static io.github.pellse.reactive.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorContinue.onErrorContinue;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.toCacheEvent;
import static io.github.pellse.util.ObjectUtils.doNothing;
import static io.github.pellse.util.ObjectUtils.ifNotNull;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    Logger logger = getLogger(CacheFactory.class.getName());

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> autoCache(Supplier<Flux<R>> dataSourceSupplier) {
        return autoCache(dataSourceSupplier.get());
    }

    static <T, ID, R, RRC> CacheTransformer<T, ID, R, RRC> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, __ -> true, identity());
    }

    static <T, ID, R, RRC, U> CacheTransformer<T, ID, R, RRC> autoCache(
            Supplier<Flux<U>> dataSourceSupplier,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return autoCache(dataSourceSupplier.get(), isAddOrUpdateEvent, cacheEventValueExtractor);
    }

    static <T, ID, R, RRC, U> CacheTransformer<T, ID, R, RRC> autoCache(
            Flux<U> dataSource,
            Predicate<U> isAddOrUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return autoCache(dataSource.map(toCacheEvent(isAddOrUpdateEvent, cacheEventValueExtractor)), null, null, null, null, null);
    }

    static <T, ID, R, RRC, U extends CacheEvent<R>> CacheTransformer<T, ID, R, RRC> autoCache(
            Flux<U> dataSource,
            WindowingStrategy<U> windowingStrategy,
            ErrorHandler errorHandler,
            LifeCycleEventSource lifeCycleEventSource,
            Scheduler scheduler,
            Function<CacheFactory<T, ID, R, RRC>, CacheFactory<T, ID, R, RRC>> concurrentCacheTransformer) {

        return cacheFactory -> (fetchFunction, context) -> {
            final var cache = requireNonNullElse(concurrentCacheTransformer, ConcurrentCacheFactory::concurrent)
                    .apply(cacheFactory)
                    .create(fetchFunction, context);

            final var idExtractor = context.correlationIdExtractor();

            final var cacheSourceFlux = requireNonNull(dataSource, "dataSource cannot be null")
                    .transform(scheduleOn(scheduler, Flux::publishOn))
                    .transform(requireNonNullElse(windowingStrategy, flux -> flux.window(MAX_WINDOW_SIZE)))
                    .flatMap(flux -> flux.collect(partitioningBy(Updated.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), idExtractor), toMap(eventMap.get(false), idExtractor)))
                    .transform(requireNonNullElse(errorHandler, onErrorContinue(AutoCacheFactory::logError)).toFluxErrorHandler())
                    .doFinally(__ -> ifNotNull(scheduler, Scheduler::dispose));

            requireNonNullElse(lifeCycleEventSource, LifeCycleEventListener::start)
                    .addLifeCycleEventListener(concurrentLifeCycleEventListener(lifeCycleEventAdapter(cacheSourceFlux, Flux::subscribe, Disposable::dispose)));

            return cache;
        };
    }

    private static <ID, R> Map<ID, List<R>> toMap(List<? extends CacheEvent<R>> cacheEvents, Function<R, ID> correlationIdExtractor) {
        return cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(groupingBy(correlationIdExtractor));
    }

    private static <T> Function<Flux<T>, Flux<T>> scheduleOn(Scheduler scheduler, BiFunction<Flux<T>, Scheduler, Flux<T>> scheduleFunction) {
        return flux -> scheduler != null ? scheduleFunction.apply(flux, scheduler) : flux;
    }

    private static void logError(Throwable t, Object object) {
        logger.log(WARNING, "Error while updating cache in autoCache()", t);
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
