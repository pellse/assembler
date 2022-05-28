package io.github.pellse.reactive.assembler.caching;

import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface AdapterCache {

    static <ID, RRC> Cache<ID, RRC> adapterCache(
            BiFunction<Iterable<ID>, Boolean, Mono<Map<ID, RRC>>> getAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll
    ) {
        return new Cache<>() {
            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids, boolean computeIfAbsent) {
                return getAll.apply(ids, computeIfAbsent);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return putAll.apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return removeAll.apply(map);
            }
        };
    }
}
