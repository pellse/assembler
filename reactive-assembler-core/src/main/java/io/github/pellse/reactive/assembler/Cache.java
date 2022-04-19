package io.github.pellse.reactive.assembler;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

@FunctionalInterface
public interface Cache<ID, R> {
    Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids);
}
