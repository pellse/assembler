package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.MapperCache;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

public interface CaffeineMapperCacheHelper {

    static <ID, R> MapperCache<ID, R> newCache() {
        return newCache(newBuilder());
    }

    static <ID, R> MapperCache<ID, R> newCache(Caffeine<Object, Object> caffeine) {
        return newCache(caffeine, Caffeine::build);
    }

    static <ID, R> MapperCache<ID, R> newCache(Function<Caffeine<Object, Object>, Cache<Iterable<ID>, Publisher<Map<ID, R>>>> builder) {
        return newCache(newBuilder(), builder);
    }

    static <ID, R> MapperCache<ID, R> newCache(Caffeine<Object, Object> caffeine,
                                               Function<Caffeine<Object, Object>, Cache<Iterable<ID>, Publisher<Map<ID, R>>>> builder) {
        return builder.apply(caffeine)::get;
    }
}
