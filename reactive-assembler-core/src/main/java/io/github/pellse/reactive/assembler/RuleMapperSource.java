package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.function.Function;

@FunctionalInterface
public interface RuleMapperSource<ID, IDC extends Collection<ID>, R> extends Function<Function<R, ID>, Function<IDC, Publisher<R>>> {

    static <ID, IDC extends Collection<ID>, T, R> RuleMapperSource<ID, IDC, R> call(Function<IDC, Publisher<R>> queryFunction) {
        return idExtractor -> queryFunction;
    }
}
