package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.function.Function;

@FunctionalInterface
public interface RuleMapperSource<ID, IDC extends Collection<ID>, R> extends Function<RuleContext<ID, IDC, R>, Function<IDC, Publisher<R>>> {

    static <ID, IDC extends Collection<ID>, R> RuleMapperSource<ID, IDC, R> call(Function<IDC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }
}
