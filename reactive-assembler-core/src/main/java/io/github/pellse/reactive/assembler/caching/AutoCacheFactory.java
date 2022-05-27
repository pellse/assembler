package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.caching.CacheFactory.Context;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.execute;
import static java.util.stream.Collectors.partitioningBy;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    interface CRUDCache<ID, RRC> extends Cache<ID, RRC> {

        Mono<?> updateAll(Map<ID, RRC> mapMonoToAdd, Map<ID, RRC> mapMonoToRemove);
    }

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <R> Flux<CacheEvent<R>> toCacheEvent(Flux<R> flux) {
        return flux.map(AddUpdateEvent::new);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<CacheEvent<R>> dataSource) {
        return autoCache(dataSource, MAX_WINDOW_SIZE);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<CacheEvent<R>> dataSource, int maxWindowSize) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<CacheEvent<R>> dataSource, Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<CacheEvent<R>> dataSource,
            int maxWindowSize,
            Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<CacheEvent<R>> dataSource,
            WindowingStrategy<CacheEvent<R>> windowingStrategy) {

        return cacheFactory -> (fetchFunction, context) -> {
            var cache = synchronous(cacheFactory.create(fetchFunction, context));

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .flatMap(flux -> flux.collect(partitioningBy(AddUpdateEvent.class::isInstance)))
                    .flatMap(eventMap -> cache.updateAll(toMap(eventMap.get(true), context), toMap(eventMap.get(false), context)));

            cacheSourceFlux.subscribe();
            return cache;
        };
    }

    private static <ID, RRC> CRUDCache<ID, RRC> synchronous(Cache<ID, RRC> delegateCache) {

        return new CRUDCache<>() {

            private final AtomicBoolean shouldRunFlag = new AtomicBoolean();

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return execute(delegateCache.getAll(ids, computeIfAbsent), shouldRunFlag);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return execute(delegateCache.putAll(map), shouldRunFlag);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return execute(delegateCache.removeAll(map), shouldRunFlag);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapMonoToAdd, Map<ID, RRC> mapMonoToRemove) {
                return execute(
                        delegateCache.putAll(mapMonoToAdd).then(delegateCache.removeAll(mapMonoToRemove)),
                        shouldRunFlag);
            }
        };
    }

    private static <ID, R, RRC> Map<ID, RRC> toMap(List<CacheEvent<R>> cacheEvents, Context<ID, R, RRC> context) {
        return cacheEvents.stream()
                .map(CacheEvent::value)
                .collect(context.mapCollector().apply(-1));
    }
}
