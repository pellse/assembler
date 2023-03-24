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

import io.github.pellse.reactive.assembler.LifeCycleEventSource;
import io.github.pellse.reactive.assembler.LifeCycleEventSource.LifeCycleEventListener;
import io.github.pellse.reactive.assembler.caching.CacheEvent.Updated;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.github.pellse.reactive.assembler.LifeCycleEventSource.concurrentLifeCycleEventListener;
import static io.github.pellse.reactive.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.toCacheEvent;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrentCache;
import static io.github.pellse.util.ObjectUtils.runIf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    static <R> WindowingStrategy<R> defaultWindowingStrategy() {
        return flux -> flux.window(MAX_WINDOW_SIZE);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, __ -> true, identity());
    }

    static <ID, R, RRC, U> CacheTransformer<ID, R, RRC> autoCache(
            Flux<U> dataSource,
            Predicate<U> isUpdateEvent,
            Function<U, R> cacheEventValueExtractor) {

        return autoCache(
                dataSource.map(toCacheEvent(isUpdateEvent, cacheEventValueExtractor)),
                defaultWindowingStrategy(),
                onErrorStop(),
                LifeCycleEventListener::start);
    }

    static <ID, R, RRC, T extends CacheEvent<R>> CacheTransformer<ID, R, RRC> autoCache(
            Flux<T> dataSource,
            WindowingStrategy<T> windowingStrategy,
            ErrorHandler errorHandler,
            LifeCycleEventSource lifeCycleEventSource) {

        return autoCache(dataSource, windowingStrategy, errorHandler, lifeCycleEventSource, null);
    }

    static <ID, R, RRC, T extends CacheEvent<R>> CacheTransformer<ID, R, RRC> autoCache(
            Flux<T> dataSource,
            WindowingStrategy<T> windowingStrategy,
            ErrorHandler errorHandler,
            LifeCycleEventSource lifeCycleEventSource,
            Scheduler scheduler) {

        return cacheFactory -> (fetchFunction, context) -> {
            var cache = concurrentCache(cacheFactory.create(fetchFunction, context));
            var idExtractor = context.correlationIdExtractor();

            var cacheSourceFlux = dataSource
                    .transform(scheduleOn(scheduler, Flux::publishOn))
                    .transform(windowingStrategy)
                    .flatMap(flux -> flux.collect(partitioningBy(Updated.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), idExtractor), toMap(eventMap.get(false), idExtractor)))
                    .transform(errorHandler.toFluxErrorHandler())
                    .doFinally(__ -> runIf(scheduler, Objects::nonNull, Scheduler::dispose))
                    .transform(scheduleOn(scheduler, Flux::subscribeOn));

            lifeCycleEventSource.addLifeCycleEventListener(
                    concurrentLifeCycleEventListener(
                            lifeCycleEventAdapter(cacheSourceFlux, Flux::subscribe, Disposable::dispose)));

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

    sealed interface ErrorHandler {
        <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler();
    }

    @FunctionalInterface
    interface WindowingStrategy<R> extends Function<Flux<R>, Flux<Flux<R>>> {
    }

    record OnErrorContinue(Consumer<Throwable> errorConsumer) implements ErrorHandler {
        public static OnErrorContinue onErrorContinue(Consumer<Throwable> errorConsumer) {
            return new OnErrorContinue(errorConsumer);
        }

        @Override
        public <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler() {
            return flux -> flux.onErrorContinue((error, object) -> errorConsumer().accept(error));
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
