package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static reactor.core.publisher.Mono.from;

@FunctionalInterface
public interface MapperCache<ID, R> extends BiFunction<Iterable<ID>, Mapper<ID, R>, Publisher<Map<ID, R>>> {

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper) {
        return cached(mapper, defaultCache());
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, MapperCache<ID, R> cache) {
        return cached(mapper, cache, null);
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, Duration ttl) {
        return cached(mapper, defaultCache(), ttl);
    }

    static <ID, R> Mapper<ID, R> cached(Mapper<ID, R> mapper, MapperCache<ID, R> cache, Duration ttl) {
        return entityIds -> cache.apply(entityIds, ids -> toCachedMono(from(mapper.apply(ids)), ttl));
    }

    static <ID, R> MapperCache<ID, R> newCache(Supplier<Map<Iterable<ID>, Publisher<Map<ID, R>>>> mapSupplier) {
        return newCache(mapSupplier.get());
    }

    static <ID, R> MapperCache<ID, R> newCache(Map<Iterable<ID>, Publisher<Map<ID, R>>> map) {
        return map::computeIfAbsent;
    }

    private static <T> Mono<T> toCachedMono(Mono<T> mono, Duration ttl) {
        return ttl != null ? mono.cache(ttl) : mono.cache();
    }

    private static <ID, R> MapperCache<ID, R> defaultCache() {
        return newCache(ConcurrentHashMap::new);
    }
}
