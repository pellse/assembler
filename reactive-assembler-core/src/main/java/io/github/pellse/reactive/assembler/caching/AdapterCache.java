package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface AdapterCache {

    static <ID, R> Cache<ID, R> adapterCache(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, List<R>>>> getAll,
            Function<Map<ID, List<R>>, Mono<?>> putAll,
            Function<Map<ID, List<R>>, Mono<?>> removeAll
    ) {
        return new Cache<>() {
            @Override
            public Mono<Map<ID, List<R>>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return getAll.apply(ids, computeIfAbsent);
            }

            @Override
            public Mono<?> putAll(Map<ID, List<R>> map) {
                return putAll.apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, List<R>> map) {
                return removeAll.apply(map);
            }
        };
    }
}
