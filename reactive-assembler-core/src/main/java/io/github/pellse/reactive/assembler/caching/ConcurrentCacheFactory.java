package io.github.pellse.reactive.assembler.caching;

import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;

import static io.github.pellse.reactive.assembler.caching.ConcurrentCache.concurrent;

public interface ConcurrentCacheFactory {

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrentCache() {
        return ConcurrentCacheFactory::concurrentCache;
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrentCache(CacheFactory<ID, R, RRC> delegateCacheFactory) {
        return (fetchFunction, context) -> concurrent(delegateCacheFactory.create(fetchFunction, context));
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrentCache(long maxAttempts) {
        return cacheFactory -> concurrentCache(cacheFactory, maxAttempts);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrentCache(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts) {
        return (fetchFunction, context) -> concurrent(delegateCacheFactory.create(fetchFunction, context), maxAttempts);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrentCache(long maxAttempts, Duration delay) {
        return cacheFactory -> concurrentCache(cacheFactory, maxAttempts, delay);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrentCache(CacheFactory<ID, R, RRC> delegateCacheFactory, long maxAttempts, Duration delay) {
        return (fetchFunction, context) -> concurrent(delegateCacheFactory.create(fetchFunction, context), maxAttempts, delay);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrentCache(RetrySpec retrySpec) {
        return cacheFactory -> concurrentCache(cacheFactory, retrySpec);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrentCache(CacheFactory<ID, R, RRC> delegateCacheFactory, RetrySpec retrySpec) {
        return (fetchFunction, context) -> concurrent(delegateCacheFactory.create(fetchFunction, context), retrySpec);
    }

    static <ID, R, RRC> CacheTransformer<ID, R, RRC> concurrentCache(RetryBackoffSpec retrySpec) {
        return cacheFactory -> concurrentCache(cacheFactory, retrySpec);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> concurrentCache(CacheFactory<ID, R, RRC> delegateCacheFactory, RetryBackoffSpec retrySpec) {
        return (fetchFunction, context) -> concurrent(delegateCacheFactory.create(fetchFunction, context), retrySpec);
    }
}
