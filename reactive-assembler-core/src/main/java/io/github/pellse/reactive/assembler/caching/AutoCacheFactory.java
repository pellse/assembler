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

import static io.github.pellse.reactive.assembler.LifeCycleEventSource.concurrentLifeCycleEventListener;
import static io.github.pellse.reactive.assembler.LifeCycleEventSource.lifeCycleEventAdapter;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorStop.onErrorStop;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrentCache;
import static io.github.pellse.util.ObjectUtils.runIf;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    sealed interface ErrorHandler {
        <T> Function<Flux<T>, Flux<T>> toFluxErrorHandler();
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

    @FunctionalInterface
    interface WindowingStrategy<R> extends Function<Flux<R>, Flux<Flux<R>>> {
    }

    static <R> WindowingStrategy<R> defaultWindowingStrategy() {
        return flux -> flux.window(MAX_WINDOW_SIZE);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource.map(CacheEvent::updated), defaultWindowingStrategy(), onErrorStop(), LifeCycleEventListener::start);
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
}
