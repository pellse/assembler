package io.github.pellse.assembler;

import reactor.core.publisher.Flux;

import java.util.function.Function;

@FunctionalInterface
public interface WindowingStrategy<R> extends Function<Flux<R>, Flux<Flux<R>>> {
}
