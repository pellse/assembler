package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrent;

public interface AutoCacheFactory {

    int MAX_WINDOW_SIZE = 1;

    @FunctionalInterface
    interface WindowingStrategy<R> {
        Flux<Flux<R>> toWindowedFlux(Flux<R> flux);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(Flux<R> dataSource) {
        return autoCache(dataSource, MAX_WINDOW_SIZE);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(Flux<R> dataSource, int maxWindowSize) {
        return autoCache(dataSource, flux -> flux.window(maxWindowSize));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(Flux<R> dataSource, Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.window(maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<R> dataSource,
            int maxWindowSize,
            Duration maxWindowTime) {
        return autoCache(dataSource, flux -> flux.windowTimeout(maxWindowSize, maxWindowTime));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> autoCache(
            Flux<R> dataSource,
            WindowingStrategy<R> windowingStrategy) {

        return cacheFactory -> (fetchFunction, context) -> {
            var cache = concurrent(cacheFactory.create(fetchFunction, context));

            var cacheSourceFlux = windowingStrategy.toWindowedFlux(dataSource)
                    .map(flux -> flux.collect(context.mapCollector().apply(-1)))
                    .flatMap(cache::putAll);

            cacheSourceFlux.subscribe();
            return cache;
        };
    }
}
