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
import java.util.List;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtil.toStream;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNullElse;

/**
 * @param <ID>  Correlation Id type
 * @param <TC>  Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapperSource<T, TC extends Collection<T>, ID, EID, R, RRC>
        extends Function<RuleMapperContext<T, TC, ID, EID, R, RRC>, Function<TC, Publisher<R>>> {

    RuleMapperSource<?, ? extends Collection<Object>, ?, ?, ?, ?> EMPTY_SOURCE = ruleContext -> ids -> Mono.empty();

    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> toQueryFunction(Function<TC, Publisher<R>> queryFunction) {
        return ruleContext -> queryFunction;
    }

    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> call(Function<List<ID>, Publisher<R>> queryFunction) {
        return ruleContext -> RuleMapperSource.<T, TC, ID, EID, R, RRC, ID>call(ruleContext.topLevelIdResolver(), queryFunction).apply(ruleContext);
    }

    static <T, TC extends Collection<T>, ID, EID, R, RRC, K> RuleMapperSource<T, TC, ID, EID, R, RRC> call(
            Function<T, K> idResolver,
            Function<List<K>, Publisher<R>> queryFunction) {
        return ruleContext -> entities -> queryFunction.apply(toStream(entities).map(idResolver).toList());
    }

    @SuppressWarnings("unchecked")
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> emptySource() {
        return (RuleMapperSource<T, TC, ID, EID, R, RRC>) EMPTY_SOURCE;
    }

    static <T, TC extends Collection<T>, ID, EID, R, RRC> boolean isEmptySource(RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource) {
        return emptySource().equals(nullToEmptySource(ruleMapperSource));
    }

    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> nullToEmptySource(
            RuleMapperSource<T, TC, ID, EID, R, RRC> ruleMapperSource) {
        return requireNonNullElse(ruleMapperSource, RuleMapperSource.<T, TC, ID, EID, R, RRC>emptySource());
    }

    @SafeVarargs
    static <T, TC extends Collection<T>, ID, EID, R, RRC> RuleMapperSource<T, TC, ID, EID, R, RRC> pipe(
            RuleMapperSource<T, TC, ID, EID, R, RRC> mapper,
            Function<? super RuleMapperSource<T, TC, ID, EID, R, RRC>, ? extends RuleMapperSource<T, TC, ID, EID, R, RRC>>... mappingFunctions) {

        return stream(mappingFunctions)
                .reduce(mapper,
                        (ruleMapperSource, mappingFunction) -> mappingFunction.apply(ruleMapperSource),
                        (ruleMapperSource1, ruleMapperSource2) -> ruleMapperSource2);
    }
}
