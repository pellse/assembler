package io.github.pellse.reactive.assembler.caching;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;

import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtil.*;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.concat;

@FunctionalInterface
public interface MergeStrategy<ID, R> {

    Map<ID, Collection<R>> merge(Map<ID, Collection<R>> subMapFromCache, Map<ID, Collection<R>> map);

    static <ID, R> MergeStrategy<ID, R> replace() {
        return (mapFromCache, map) -> also(mapFromCache, m -> m.putAll(map));
    }

    static <ID, R> MergeStrategy<ID, R> append() {

        return (mapFromCache, map) -> {
            mapFromCache.replaceAll((id, coll) ->
                    concat(toStream(coll), toStream(map.get(id)))
                            .collect(toCollection(LinkedHashSet::new)));

            return mergeMaps(mapFromCache, readAll(intersect(map.keySet(), mapFromCache.keySet()), map));
        };
    }
}
