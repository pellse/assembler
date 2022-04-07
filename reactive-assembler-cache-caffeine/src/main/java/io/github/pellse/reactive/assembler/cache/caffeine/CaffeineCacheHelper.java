package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

public interface CaffeineCacheHelper {

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> caffeineCache() {
        return caffeineCache(newBuilder());
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> caffeineCache(Caffeine<Object, Object> caffeine) {
        return caffeineCache(caffeine, Caffeine::build);
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> caffeineCache(Function<Caffeine<Object, Object>, Cache<ID, List<R>>> builder) {
        return caffeineCache(newBuilder(), builder);
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> caffeineCache(
            Caffeine<Object, Object> caffeine,
            Function<Caffeine<Object, Object>, Cache<ID, List<R>>> builder
    ) {
        return caffeineCache(builder.apply(caffeine));
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> caffeineCache(Cache<ID, List<R>> delegateCache) {
        return new io.github.pellse.reactive.assembler.Cache<>() {

            @Override
            public Map<ID, List<R>> getAllPresent(Iterable<ID> ids) {
                return delegateCache.getAllPresent(ids);
            }

            @Override
            public void putAll(Map<ID, List<R>> map) {
                delegateCache.putAll(map);
            }
        };
    }
}
