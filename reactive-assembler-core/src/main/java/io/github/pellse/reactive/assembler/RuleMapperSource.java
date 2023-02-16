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

    interface RuleMapperBuilder<ID, IDC extends Collection<ID>, R> {
        RuleMapperBuilder<ID, IDC, R> pipe(
                Function<? super RuleMapperSource<ID, ?, IDC, R, ?>, ? extends RuleMapperSource<ID, ?, IDC, R, ?>> mappingFunction);

        <EID, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> get();

        default <EID, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> pipeAndGet(
                Function<? super RuleMapperSource<ID, ?, IDC, R, ?>, ? extends RuleMapperSource<ID, ?, IDC, R, ?>> mappingFunction) {
            return pipe(mappingFunction).get();
        }
    }

    class Builder<ID, IDC extends Collection<ID>, R> implements RuleMapperBuilder<ID, IDC, R> {

        private RuleMapperSource<ID, ?, IDC, R, ?> source;

        public Builder(RuleMapperSource<ID, ?, IDC, R, ?> source) {
            this.source = source;
        }

        @Override
        public RuleMapperBuilder<ID, IDC, R> pipe(
                Function<? super RuleMapperSource<ID, ?, IDC, R, ?>, ? extends RuleMapperSource<ID, ?, IDC, R, ?>> mappingFunction) {

            this.source = mappingFunction.apply(source);
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <EID, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> get() {
            return (RuleMapperSource<ID, EID, IDC, R, RRC>) this.source;
        }
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapperBuilder<ID, IDC, R> from(Function<IDC, Publisher<R>> queryFunction) {
        return from(call(queryFunction));
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapperBuilder<ID, IDC, R> from(RuleMapperSource<ID, ?, IDC, R, ?> source) {
        return new Builder<>(source);
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> call(Function<IDC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> emptyQuery() {
        return ruleContext -> ids -> Mono.empty();
    }

    @SafeVarargs
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> pipe(
            RuleMapperSource<ID, EID, IDC, R, RRC> mapper,
            Function<? super RuleMapperSource<ID, EID, IDC, R, RRC>, ? extends RuleMapperSource<ID, EID, IDC, R, RRC>>... mappingFunctions) {
        return stream(mappingFunctions)
                .reduce(mapper,
                        (ruleMapperSource, mappingFunction) -> mappingFunction.apply(ruleMapperSource),
                        (ruleMapperSource1, ruleMapperSource2) -> ruleMapperSource2);
    }
}
