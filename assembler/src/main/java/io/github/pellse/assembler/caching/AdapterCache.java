package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.caching.Cache.FetchFunction;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public interface AdapterCache {

    static <ID, RRC> Cache<ID, RRC> adapterCache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll) {

        return adapterCache(getAll, computeAll, putAll, removeAll, null);
    }

    static <ID, RRC> Cache<ID, RRC> adapterCache(
            Function<Iterable<ID>, Mono<Map<ID, RRC>>> getAll,
            BiFunction<Iterable<ID>, FetchFunction<ID, RRC>, Mono<Map<ID, RRC>>> computeAll,
            Function<Map<ID, RRC>, Mono<?>> putAll,
            Function<Map<ID, RRC>, Mono<?>> removeAll,
            BiFunction<Map<ID, RRC>, Map<ID, RRC>, Mono<?>> updateAll) {

        return new Cache<>() {

            @Override
            public Mono<Map<ID, RRC>> getAll(Iterable<ID> ids) {
                return getAll.apply(ids);
            }

            @Override
            public Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction) {
                return computeAll.apply(ids, fetchFunction);
            }

            @Override
            public Mono<?> putAll(Map<ID, RRC> map) {
                return putAll.apply(map);
            }

            @Override
            public Mono<?> removeAll(Map<ID, RRC> map) {
                return removeAll.apply(map);
            }

            @Override
            public Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
                return ofNullable(updateAll)
                        .orElse(Cache.super::updateAll)
                        .apply(mapToAdd, mapToRemove);
            }
        };
    }
}
