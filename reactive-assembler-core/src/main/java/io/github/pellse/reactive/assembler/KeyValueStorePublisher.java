package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Flux.fromStream;

public interface KeyValueStorePublisher {

    static <R, ID> Function<List<ID>, Publisher<R>> asKeyValueStore(
            Publisher<R> publisher,
            Function<R, ID> idExtractor) {
        return asKeyValueStore(publisher, idExtractor, ConcurrentHashMap::new);
    }

    static <R, ID> Function<List<ID>, Publisher<R>> asKeyValueStore(
            Publisher<R> publisher,
            Function<R, ID> idExtractor,
            Supplier<Map<ID, R>> storeSupplier) {

        var store = storeSupplier.get();
        from(publisher).subscribe(value -> store.put(idExtractor.apply(value), value));
        return entityIds -> fromStream(
                entityIds.stream()
                        .map(store::get)
                        .filter(Objects::nonNull)
        );
    }
}
