/*
 * Copyright 2023 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.reactive.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Function;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNullElse;

/**
 * @param <ID>  Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapperSource<ID, EID, IDC extends Collection<ID>, R, RRC>
        extends Function<RuleMapperContext<ID, EID, IDC, R, RRC>, Function<IDC, Publisher<R>>> {

    RuleMapperSource<?, ?, ? extends Collection<Object>, ?, ?> EMPTY_QUERY = ruleContext -> ids -> Mono.empty();

    static <ID, IDC extends Collection<ID>, R> RuleMapperBuilder<ID, IDC, R> from(Function<IDC, Publisher<R>> queryFunction) {
        return from(call(queryFunction));
    }

    static <ID, IDC extends Collection<ID>, R> RuleMapperBuilder<ID, IDC, R> from(RuleMapperSource<ID, ?, IDC, R, ?> source) {
        return new Builder<>(source);
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> call(Function<IDC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }

    @SuppressWarnings("unchecked")
    static <ID, EID, IDC extends Collection<ID>, R, RRC> RuleMapperSource<ID, EID, IDC, R, RRC> emptyQuery() {
        return (RuleMapperSource<ID, EID, IDC, R, RRC>) EMPTY_QUERY;
    }

    static <ID, EID, IDC extends Collection<ID>, R, RRC> Function<IDC, Publisher<R>> toQueryFunction(
            RuleMapperSource<ID, EID, IDC, R, RRC> ruleMapperSource,
            RuleMapperContext<ID, EID, IDC, R, RRC> ruleMapperContext) {

        return requireNonNullElse(ruleMapperSource, RuleMapperSource.<ID, EID, IDC, R, RRC>emptyQuery())
                .apply(ruleMapperContext);
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

        private Builder(RuleMapperSource<ID, ?, IDC, R, ?> source) {
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
}
