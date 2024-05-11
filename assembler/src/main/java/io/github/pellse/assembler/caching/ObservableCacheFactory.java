package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.CacheFactory.CacheTransformer;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.assembler.caching.Cache.adapterCache;

public interface ObservableCacheFactory {

    static <ID, EID, R, RRC> CacheTransformer<ID, EID, R, RRC> observableCache(
            Consumer<Map<ID, RRC>> onGetAll,
            Consumer<Map<ID, RRC>> onComputeAll,
            Consumer<Map<ID, RRC>> onPutAll,
            Consumer<Map<ID, RRC>> onRemoveAll) {

        return cacheFactory -> cacheContext -> {

            final var cache = cacheFactory.create(cacheContext);

            return adapterCache(
                    ids -> cache.getAll(ids).transform(call(onGetAll)),
                    (ids, fetchFunction) -> cache.computeAll(ids, fetchFunction).transform(call(onComputeAll)),
                    map -> cache.putAll(map).transform(call(map, onPutAll)),
                    map -> cache.removeAll(map).transform(call(map, onRemoveAll))
            );
        };
    }

    static <T> Function<Mono<T>, Mono<T>> call(Consumer<T> consumer) {
        return mono -> consumer != null ? mono.doOnNext(consumer) : mono;
    }

    static <T, U> Function<Mono<T>, Mono<T>> call(U value, Consumer<U> consumer) {
        return mono -> consumer != null ? mono.doOnNext(__ -> consumer.accept(value)) : mono;
    }
}
