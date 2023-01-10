package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Function;

import static java.util.Arrays.stream;

/**
 * @param <ID>  Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapperSource<ID, EID, IDC extends Collection<ID>, R, RRC>
        extends Function<RuleMapperContext<ID, EID, IDC, R, RRC>, Function<IDC, Publisher<R>>> {

    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> call(Function<IDC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> emptyQuery() {
        return ruleContext -> ids -> Mono.empty();
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> compose(
            RuleMapperSource<ID, EID, IDC, R, RRC> mapper,
            Function<? super RuleMapperSource<ID, EID, IDC, R, RRC>, ? extends RuleMapperSource<ID, EID, IDC, R, RRC>>... mappingFunctions) {
        return stream(mappingFunctions)
                .reduce(mapper,
                        (ruleMapperSource, mappingFunction) -> mappingFunction.apply(ruleMapperSource),
                        (ruleMapperSource1, ruleMapperSource2) -> ruleMapperSource2);
    }
}
