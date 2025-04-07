package io.github.pellse.assembler.caching.spring;

import io.github.pellse.assembler.caching.factory.CacheContext;
import io.github.pellse.assembler.caching.factory.CacheFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.CacheManager;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

import static io.github.pellse.assembler.caching.factory.AsyncCacheFactory.async;
import static io.github.pellse.assembler.caching.spring.SpringCacheFactory.AsyncSupport.DEFAULT;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.publisher.Mono.just;

public interface SpringCacheFactory {

    enum AsyncSupport {
        SYNC,
        ASYNC,
        DEFAULT // Let the framework detect and use async api if available by the underlying cache
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> springCache(CacheManager cacheManager, String cacheName) {
        return springCache(cacheManager, cacheName, DEFAULT);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> springCache(CacheManager cacheManager, String cacheName, AsyncSupport asyncSupport) {
        return springCache(requireNonNull(cacheManager.getCache(cacheName)), asyncSupport);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> springCache(Cache delegateCache) {
        return springCache(delegateCache, DEFAULT);
    }

    static <ID, R, RRC, CTX extends CacheContext<ID, R, RRC, CTX>> CacheFactory<ID, R, RRC, CTX> springCache(Cache delegateCache, AsyncSupport asyncSupport) {

        final BiFunction<Mono<Cache>, ID, Mono<RRC>> cacheGetter = switch (asyncSupport) {
            case SYNC -> SpringCacheFactory::get;
            case ASYNC -> SpringCacheFactory::retrieve;
            case DEFAULT -> cacheGetter(delegateCache);
        };

        final CacheFactory<ID, R, RRC, CTX> springCacheFactory = cacheContext -> new io.github.pellse.assembler.caching.Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return fromIterable(ids)
                        .flatMap(this::buildMapEntry)
                        .collectMap(Entry::getKey, Entry::getValue);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return getAll(ids)
                        .flatMap(cachedData -> fetchFunction.apply(diff(ids, cachedData.keySet()))
                                .doOnNext(this::addAll)
                                .map(fetchedData -> fetchedData.isEmpty() ? cachedData : cacheContext.mapMerger().apply(cachedData, fetchedData)));
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return just(also(map, this::addAll));
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return just(also(map, m -> m.keySet().forEach(delegateCache::evict)));
            }

            private Mono<Entry<ID, RRC>> buildMapEntry(ID id) {
                return just(delegateCache)
                        .transform(cacheMono -> cacheGetter.apply(cacheMono, id))
                        .map(value -> entry(id, value));
            }

            private void addAll(Map<ID, RRC> map) {
                map.forEach(delegateCache::put);
            }
        };

        return async(springCacheFactory);
    }

    private static <ID, RRC> BiFunction<Mono<Cache>, ID, Mono<RRC>> cacheGetter(Cache delegateCache) {
        try {
            delegateCache.retrieve(new Object());
            return SpringCacheFactory::retrieve;
        } catch (Exception __) {
            return SpringCacheFactory::get;
        }
    }

    @SuppressWarnings("unchecked")
    private static <ID, RRC> Mono<RRC> get(Mono<Cache> delegateCacheMono, ID id) {
        return delegateCacheMono
                .mapNotNull(cache -> cache.get(id))
                .mapNotNull(wrapper -> (RRC) wrapper.get());
    }

    @SuppressWarnings("unchecked")
    private static <ID, RRC> Mono<RRC> retrieve(Mono<Cache> delegateCacheMono, ID id) {
        return delegateCacheMono
                .mapNotNull(cache -> cache.retrieve(id))
                .flatMap(Mono::fromFuture)
                .mapNotNull(value -> (RRC) (value instanceof ValueWrapper wrapper ? wrapper.get() : value));
    }
}
