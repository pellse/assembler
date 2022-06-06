package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.caching.CacheFactory;

import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.toMono;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheFactory {

    static <ID, R, RRC> CacheFactory<ID, R, RRC> caffeineCache() {
        return caffeineCache(newBuilder());
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> caffeineCache(Function<Caffeine<Object, Object>, Caffeine<Object, Object>> customizer) {
        return caffeineCache(customizer.apply(newBuilder()));
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> caffeineCache(Caffeine<Object, Object> caffeine) {

        final AsyncCache<ID, RRC> delegateCache = caffeine.buildAsync();

        return (fetchFunction, context) -> adapterCache(
                (ids, computeIfAbsent) -> fromFuture(delegateCache.getAll(ids, (keys, executor) ->
                        computeIfAbsent ? fetchFunction.apply(keys).toFuture() : completedFuture(emptyMap()))),
                toMono(map -> map.forEach((id, value) -> delegateCache.put(id, completedFuture(value)))),
                toMono(map -> delegateCache.synchronous().invalidateAll(map.keySet()))
        );
    }
}
