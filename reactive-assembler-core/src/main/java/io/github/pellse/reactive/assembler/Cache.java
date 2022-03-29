package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.function.Function;

@FunctionalInterface
public interface Cache <ID, R> {

    Publisher<Map<ID, R>> get(Iterable<ID> ids, Function<Iterable<ID>, Publisher<Map<ID, R>>> mappingFunction);
}
