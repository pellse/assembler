package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.Cache;
import io.github.pellse.reactive.assembler.CacheFactory;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheFactory {

    static <ID, R> CacheFactory<ID, R> caffeineCache() {
        return caffeineCache(newBuilder());
    }

    static <ID, R> CacheFactory<ID, R> caffeineCache(Function<Caffeine<Object, Object>, Caffeine<Object, Object>> customizer) {
        return caffeineCache(customizer.apply(newBuilder()));
    }

    static <ID, R> CacheFactory<ID, R> caffeineCache(Caffeine<Object, Object> caffeine) {

        return (fetchFunction, idExtractor) -> new Cache<>() {

            private final AsyncCache<ID, Collection<R>> delegateCache = caffeine.buildAsync();

            @Override
            public Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return fromFuture(delegateCache.getAll(ids, (keys, executor) ->
                        computeIfAbsent ? fetchFunction.apply(keys).toFuture() : completedFuture(emptyMap())));
            }

            @Override
            public void putAll(Map<ID, Collection<R>> map) {
                map.forEach((id, coll) -> delegateCache.put(id, completedFuture(coll)));
            }
        };
    }
}
