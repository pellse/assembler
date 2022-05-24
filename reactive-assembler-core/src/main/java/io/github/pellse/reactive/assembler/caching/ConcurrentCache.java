package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static reactor.core.publisher.Mono.*;
import static reactor.util.retry.Retry.*;

class BusyException extends Exception {
}

public interface ConcurrentCache {

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache) {
        return concurrent(delegateCache, indefinitely());
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts) {
        return concurrent(delegateCache, retryStrategy(max(maxAttempts), RetrySpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, long maxAttempts, Duration delay) {
        return concurrent(delegateCache, retryStrategy(fixedDelay(maxAttempts, delay), RetryBackoffSpec::filter));
    }

    static <ID, RRC> Cache<ID, RRC> concurrent(Cache<ID, RRC> delegateCache, Retry retrySpec) {

        return new Cache<>() {

            private final AtomicBoolean shouldRunFlag = new AtomicBoolean();

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return execute(delegateCache.getAll(ids, computeIfAbsent), shouldRunFlag);
            }

            @Override
            public Mono<Map<ID, RRC>> putAll(Mono<Map<ID, RRC>> mapMono) {
                return execute(delegateCache.putAll(mapMono), shouldRunFlag);
            }

            private Mono<Map<ID, RRC>> execute(Mono<Map<ID, RRC>> mono, final AtomicBoolean shouldRunFlag) {
                return defer(() -> just(shouldRunFlag.compareAndSet(false, true)))
                        .filter(shouldRun -> shouldRun)
                        .flatMap($ -> mono)
                        .switchIfEmpty(error(BusyException::new))
                        .doFinally($ -> shouldRunFlag.set(false))
                        .retryWhen(retrySpec);
            }
        };
    }

    private static <T extends Retry> T retryStrategy(T retrySpec, BiFunction<T, Predicate<? super Throwable>, T> filterFunction) {
        return filterFunction.apply(retrySpec, BusyException.class::isInstance);
    }
}
