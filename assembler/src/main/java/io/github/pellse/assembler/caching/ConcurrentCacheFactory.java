/*
 * Copyright 2023 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.RetryBackoffSpec;
import reactor.util.retry.RetrySpec;

import java.time.Duration;

import static io.github.pellse.assembler.caching.ConcurrentCache.concurrentCache;
import static io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy.READ;
import static io.github.pellse.concurrent.ConcurrentExecutor.ConcurrencyStrategy.WRITE;

public interface ConcurrentCacheFactory {

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent() {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), concurrencyStrategy(context));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), concurrencyStrategy);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(long maxAttempts) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), maxAttempts, concurrencyStrategy(context));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(long maxAttempts, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), maxAttempts, concurrencyStrategy);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(long maxAttempts, Duration minBackoff) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), maxAttempts, minBackoff, concurrencyStrategy(context));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(long maxAttempts, Duration minBackoff, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), maxAttempts, minBackoff, concurrencyStrategy);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetrySpec retrySpec) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy(context));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetrySpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetryBackoffSpec retrySpec) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy(context));
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetryBackoffSpec retrySpec, Scheduler retryScheduler) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy(context), retryScheduler);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy);
    }

    static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> CacheTransformer<ID, EID, R, RRC, CTX> concurrent(RetryBackoffSpec retrySpec, ConcurrencyStrategy concurrencyStrategy, Scheduler retryScheduler) {
        return cacheFactory -> context -> concurrentCache(cacheFactory.create(context), retrySpec, concurrencyStrategy, retryScheduler);
    }

    // Helpers

    private static <ID, EID, R, RRC, CTX extends CacheContext<ID, EID, R, RRC, CTX>> ConcurrencyStrategy concurrencyStrategy(CacheContext<ID, EID, R, RRC, CTX> context) {
        return context.isEmptySource() ? READ : WRITE;
    }
}