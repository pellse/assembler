package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

public interface CaffeineMapperCacheHelper {

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> cache() {
        return cache(newBuilder());
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> cache(Caffeine<Object, Object> caffeine) {
        return cache(caffeine, Caffeine::build);
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> cache(Function<Caffeine<Object, Object>, Cache<ID, List<R>>> builder) {
        return cache(newBuilder(), builder);
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> cache(
            Caffeine<Object, Object> caffeine,
            Function<Caffeine<Object, Object>, Cache<ID, List<R>>> builder
    ) {
        return cache(builder.apply(caffeine));
    }

    static <ID, R> io.github.pellse.reactive.assembler.Cache<ID, R> cache(Cache<ID, List<R>> delegateCache) {
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
