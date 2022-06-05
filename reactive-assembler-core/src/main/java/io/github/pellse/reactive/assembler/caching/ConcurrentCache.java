package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

class BusyException extends Exception {
}

public interface ConcurrentCache {

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache) {
        return concurrent(delegateCache, retryStrategy(indefinitely(), RetrySpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts) {
        return concurrent(delegateCache, retryStrategy(max(maxAttempts), RetrySpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts, Duration delay) {
        return concurrent(delegateCache, retryStrategy(fixedDelay(maxAttempts, delay), RetryBackoffSpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, Retry retrySpec) {

        final AtomicBoolean shouldRunFlag = new AtomicBoolean();

        return adapterCache(
                (ids, computeIfAbsent) -> execute(delegateCache.getAll(ids, computeIfAbsent), shouldRunFlag, retrySpec),
                map -> execute(delegateCache.putAll(map), shouldRunFlag, retrySpec),
                map -> execute(delegateCache.removeAll(map), shouldRunFlag, retrySpec)
        );
    }

    static <T> Mono<T> execute(Mono<T> mono, final AtomicBoolean shouldRunFlag) {
        return execute(mono, shouldRunFlag, retryStrategy(indefinitely(), RetrySpec::filter));
    }

    static <T> Mono<T> execute(Mono<T> mono, final AtomicBoolean shouldRunFlag, Retry retrySpec) {
        return defer(() -> just(shouldRunFlag.compareAndSet(false, true)))
                .filter(shouldRun -> shouldRun)
                .flatMap($ -> mono)
                .switchIfEmpty(error(BusyException::new))
                .doFinally($ -> shouldRunFlag.set(false))
                .retryWhen(retrySpec);
    }

    static <T extends Retry> T retryStrategy(T retrySpec, BiFunction<T, Predicate<? super Throwable>, T> filterFunction) {
        return filterFunction.apply(retrySpec, e -> e instanceof BusyException);
    }
}
