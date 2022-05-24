package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.caching.Cache;
import io.github.pellse.reactive.assembler.caching.CacheFactory;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.util.ObjectUtils.also;
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

        return (fetchFunction, context) -> new Cache<>() {

            private final AsyncCache<ID, RRC> delegateCache = caffeine.buildAsync();

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return fromFuture(delegateCache.getAll(ids, (keys, executor) ->
                        computeIfAbsent ? fetchFunction.apply(keys).toFuture() : completedFuture(emptyMap())));
            }

            @Override
            public Mono<Map<ID, RRC>> putAll(Mono<Map<ID, RRC>> mapMono) {
                return mapMono
                        .map(m -> also(m, map -> map.forEach((id, value) -> delegateCache.put(id, completedFuture(value)))));
            }
        };
    }
}
