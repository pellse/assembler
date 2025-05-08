/*
 * Copyright 2024 Sebastien Pelletier
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

package io.github.pellse.assembler;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;

import static io.github.pellse.util.collection.CollectionUtils.toStream;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNullElse;

@FunctionalInterface
public interface RuleMapperSource<T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>>
        extends Function<CTX, Function<List<T>, Publisher<R>>> {

    RuleMapperSource<?, ?, ?, ?, ?, ?, RuleMapperContext<Object, Object, Object, Object, Object, Object>> EMPTY_SOURCE = ruleContext -> ids -> Mono.empty();

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> from(Function<List<T>, Publisher<R>> queryFunction) {
        return __ -> queryFunction;
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> call(Function<List<ID>, Publisher<R>> queryFunction) {
        return ruleContext -> RuleMapperSource.<T, K, ID, EID, R, RRC, CTX>call(ruleContext.outerIdResolver(), queryFunction).apply(ruleContext);
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> call(
            Function<T, ID> idResolver,
            Function<List<ID>, Publisher<R>> queryFunction) {

        return __ -> entities -> queryFunction.apply(toStream(entities).map(idResolver).toList());
    }

    @SuppressWarnings("unchecked")
    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> emptySource() {
        return (RuleMapperSource<T, K, ID, EID, R, RRC, CTX>) EMPTY_SOURCE;
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> boolean isEmptySource(RuleMapperSource<T, K, ID, EID, R, RRC, CTX> ruleMapperSource) {
        return emptySource().equals(nullToEmptySource(ruleMapperSource));
    }

    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> nullToEmptySource(
            RuleMapperSource<T, K, ID, EID, R, RRC, CTX> ruleMapperSource) {

        return requireNonNullElse(ruleMapperSource, RuleMapperSource.<T, K, ID, EID, R, RRC, CTX>emptySource());
    }

    @SafeVarargs
    static <T, K, ID, EID, R, RRC, CTX extends RuleMapperContext<T, K, ID, EID, R, RRC>> RuleMapperSource<T, K, ID, EID, R, RRC, CTX> pipe(
            RuleMapperSource<T, K, ID, EID, R, RRC, CTX> mapper,
            Function<? super RuleMapperSource<T, K, ID, EID, R, RRC, CTX>, ? extends RuleMapperSource<T, K, ID, EID, R, RRC, CTX>>... mappingFunctions) {

        return stream(mappingFunctions)
                .reduce(mapper,
                        (ruleMapperSource, mappingFunction) -> mappingFunction.apply(ruleMapperSource),
                        (ruleMapperSource1, ruleMapperSource2) -> ruleMapperSource2);
    }
}
