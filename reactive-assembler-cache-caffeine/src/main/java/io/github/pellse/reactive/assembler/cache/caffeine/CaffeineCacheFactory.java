package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.CacheFactory;

import java.util.Collection;

import static com.github.benmanes.caffeine.cache.AsyncCacheLoader.bulk;
import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheFactory {

    static <ID, R> CacheFactory<ID, R> caffeineCache() {
        return caffeineCache(newBuilder());
    }

    static <ID, R> CacheFactory<ID, R> caffeineCache(Caffeine<Object, Object> caffeine) {

        return fetchFunction -> {
            final AsyncLoadingCache<ID, Collection<R>> delegateCache = caffeine.buildAsync(
                    bulk((ids, executor) -> fetchFunction.apply(ids).toFuture()));

            return ids -> fromFuture(delegateCache.getAll(ids));
        };
    }
}
