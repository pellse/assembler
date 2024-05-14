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

package io.github.pellse.assembler;

import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.assembler.QueryUtils.*;
import static io.github.pellse.assembler.RuleMapperSource.*;
import static java.util.Comparator.comparing;

/**
 * @param <ID>  Correlation Id type
 * @param <TC>  Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R>   Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
@FunctionalInterface
public interface RuleMapper<T, TC extends Collection<T>, ID, R, RRC>
        extends Function<RuleContext<T, TC, ID, R, RRC>, Function<Iterable<T>, Mono<Map<ID, RRC>>>> {

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne() {
        return oneToOne(emptySource(), id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(Function<TC, Publisher<R>> queryFunction) {
        return oneToOne(toRuleMapperSource(queryFunction), id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> ruleMapperSource) {
        return oneToOne(ruleMapperSource, id -> null);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(
            Function<TC, Publisher<R>> queryFunction,
            Function<ID, R> defaultResultProvider) {

        return oneToOne(toRuleMapperSource(queryFunction), defaultResultProvider);
    }

    static <T, TC extends Collection<T>, ID, R> RuleMapper<T, TC, ID, R, R> oneToOne(
            RuleMapperSource<T, TC, ID, ID, R, R, OneToOneContext<T, TC, ID, R>> ruleMapperSource,
            Function<ID, R> defaultResultProvider) {

        return createRuleMapper(
                ruleMapperSource,
                ctx -> new OneToOneContext<>(ctx, defaultResultProvider));
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(Function<R, EID> idResolver) {
        return oneToMany(idResolver, emptySource(), ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction) {

        return oneToMany(idResolver, toRuleMapperSource(queryFunction), ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R> RuleMapper<T, TC, ID, R, List<R>> oneToMany(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, List<R>, OneToManyContext<T, TC, ID, EID, R, List<R>>> ruleMapperSource) {

        return oneToMany(idResolver, ruleMapperSource, ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R> RuleMapper<T, TC, ID, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction) {

        return oneToMany(idResolver, toRuleMapperSource(queryFunction), HashSet::new);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R> RuleMapper<T, TC, ID, R, Set<R>> oneToManyAsSet(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, Set<R>, OneToManyContext<T, TC, ID, EID, R, Set<R>>> ruleMapperSource) {

        return oneToMany(idResolver, ruleMapperSource, HashSet::new);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R, RC extends Collection<R>> RuleMapper<T, TC, ID, R, RC> oneToMany(
            Function<R, EID> idResolver,
            Function<TC, Publisher<R>> queryFunction,
            Supplier<RC> collectionFactory) {

        return oneToMany(idResolver, toRuleMapperSource(queryFunction), collectionFactory);
    }

    static <T, TC extends Collection<T>, ID, EID extends Comparable<EID>, R, RC extends Collection<R>> RuleMapper<T, TC, ID, R, RC> oneToMany(
            Function<R, EID> idResolver,
            RuleMapperSource<T, TC, ID, EID, R, RC, OneToManyContext<T, TC, ID, EID, R, RC>> ruleMapperSource,
            Supplier<RC> collectionFactory) {

        return createRuleMapper(
                ruleMapperSource,
                ctx -> new OneToManyContext<>(ctx, idResolver, comparing(idResolver), collectionFactory));
    }

    private static <T, TC extends Collection<T>, ID, EID, R, RRC, CTX extends RuleMapperContext<T, TC, ID, EID, R, RRC>> RuleMapper<T, TC, ID, R, RRC> createRuleMapper(
            RuleMapperSource<T, TC, ID, EID, R, RRC, CTX> ruleMapperSource,
            Function<RuleContext<T, TC, ID, R, RRC>, CTX> ruleMapperContextProvider) {

        return ruleContext -> buildQueryFunction(ruleMapperSource, ruleMapperContextProvider.apply(ruleContext));
    }
}
