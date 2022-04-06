package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.QueryCache;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

public interface CaffeineMapperCacheHelper {

    static <ID, R> QueryCache<ID, R> cache() {
        return cache(newBuilder());
    }

    static <ID, R> QueryCache<ID, R> cache(Caffeine<Object, Object> caffeine) {
        return cache(caffeine, Caffeine::build);
    }

    static <ID, R> QueryCache<ID, R> cache(Function<Caffeine<Object, Object>, Cache<Iterable<ID>, Mono<Map<ID, R>>>> builder) {
        return cache(newBuilder(), builder);
    }

    static <ID, R> QueryCache<ID, R> cache(Caffeine<Object, Object> caffeine,
                                           Function<Caffeine<Object, Object>, Cache<Iterable<ID>, Mono<Map<ID, R>>>> builder) {
        return builder.apply(caffeine)::get;
    }
}
