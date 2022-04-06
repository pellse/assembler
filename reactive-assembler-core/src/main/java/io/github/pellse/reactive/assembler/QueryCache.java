package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionalInterface
public interface QueryCache<ID, R> extends BiFunction<Iterable<ID>, Mapper<ID, R>, Mono<Map<ID, R>>> {

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper) {
        return cached(mapper, defaultCache());
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, QueryCache<ID, R> cache) {
        return cached(mapper, cache, null);
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, Duration ttl) {
        return cached(mapper, defaultCache(), ttl);
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, QueryCache<ID, R> cache, Duration ttl) {
        return entityIds -> cache.apply(entityIds, ids -> toCachedMono(mapper.apply(ids), ttl));
    }

    static <ID, R> QueryCache<ID, R> cache(Supplier<Map<Iterable<ID>, Mono<Map<ID, R>>>> mapSupplier) {
        return cache(mapSupplier.get());
    }

    static <ID, R> QueryCache<ID, R> cache(Map<Iterable<ID>, Mono<Map<ID, R>>> map) {
        return map::computeIfAbsent;
    }

    private static <T> Mono<T> toCachedMono(Mono<T> mono, Duration ttl) {
        return ttl != null ? mono.cache(ttl) : mono.cache();
    }

    private static <ID, R> QueryCache<ID, R> defaultCache() {
        return cache(ConcurrentHashMap::new);
    }
}
