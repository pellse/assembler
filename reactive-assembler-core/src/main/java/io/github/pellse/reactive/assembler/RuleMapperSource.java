package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.function.Function;

/**
 *
 * @param <ID> Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. List<ID>, Set<ID>)
 * @param <R> Type of the publisher elements returned from <code>queryFunction</code>
 * @param <RRC> Either R or collection of R (e.g. R vs. List<R>)
 */
@FunctionalInterface
public interface RuleMapperSource<ID, IDC extends Collection<ID>, R, RRC> extends Function<RuleContext<ID, IDC, R, RRC>, Function<IDC, Publisher<R>>> {

    static <ID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, IDC, R, RRC> call(Function<IDC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }
}
