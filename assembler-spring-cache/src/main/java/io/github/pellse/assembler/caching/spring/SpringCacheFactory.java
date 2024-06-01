package io.github.pellse.assembler.caching.spring;

import io.github.pellse.assembler.caching.Cache;
import io.github.pellse.assembler.caching.CacheContext;
import io.github.pellse.assembler.caching.CacheFactory;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.CacheManager;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.spring.SpringCacheFactory.AsyncSupport.DEFAULT;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtils.*;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.publisher.Mono.just;

public interface SpringCacheFactory {

    enum AsyncSupport {
        SYNC, ASYNC, DEFAULT
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> springCache(CacheManager cacheManager, String cacheName) {
        return springCache(cacheManager, cacheName, DEFAULT);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> springCache(CacheManager cacheManager, String cacheName, AsyncSupport asyncSupport) {
        return springCache(requireNonNull(cacheManager.getCache(cacheName)), asyncSupport);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> springCache(org.springframework.cache.Cache delegateCache){
        return springCache(delegateCache, DEFAULT);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC>> CacheFactory<ID, EID, R, RRC, CTX> springCache(org.springframework.cache.Cache delegateCache, AsyncSupport asyncSupport) {

        return context -> new Cache<>() {

            private final Function<ID, Mono<Entry<ID, RRC>>> cacheGetter = switch(asyncSupport) {
                case SYNC -> this::get;
                case ASYNC -> this::retrieve;
                case DEFAULT -> supportAsync(delegateCache) ? this::retrieve : this::get;
            };

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return fromIterable(ids)
                        .flatMap(cacheGetter)
                        .collectMap(Entry::getKey, Entry::getValue);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return getAll(ids)
                        .flatMap(cachedData -> fetchFunction.apply(intersect(ids, cachedData.keySet()))
                                .doOnNext(this::putAll)
                                .map(fetchedData -> fetchedData.isEmpty() ? cachedData : context.ctx().mapMerger().apply(cachedData, fetchedData)));
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return just(also(map, m -> m.forEach(delegateCache::put)));
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return just(also(map, m -> m.keySet().forEach(delegateCache::evict)));
            }

            @SuppressWarnings("unchecked")
            private Mono<Entry<ID, RRC>> get(ID id) {
                return just(delegateCache)
                        .mapNotNull(cache -> cache.get(id))
                        .mapNotNull(wrapper -> (RRC) wrapper.get())
                        .map(value -> entry(id, value));
            }

            @SuppressWarnings("unchecked")
            private Mono<Entry<ID, RRC>> retrieve(ID id) {
                return just(delegateCache)
                        .mapNotNull(cache -> cache.retrieve(id))
                        .flatMap(Mono::fromFuture)
                        .mapNotNull(value -> value instanceof ValueWrapper wrapper ? (RRC) wrapper.get() : (RRC) value)
                        .map(value -> entry(id, value));
            }
        };
    }

    private static boolean supportAsync(org.springframework.cache.Cache delegateCache) {
        try {
            delegateCache.retrieve(new Object());
            return true;
        } catch (Exception __) {
            return false;
        }
    }
}
