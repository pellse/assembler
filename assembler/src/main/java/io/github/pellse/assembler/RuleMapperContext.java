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

import io.github.pellse.util.collection.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.github.pellse.assembler.QueryUtils.toSupplier;
import static io.github.pellse.util.collection.CollectionUtils.toStream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public sealed interface RuleMapperContext<T, TC extends Collection<T>, ID, EID, R, RRC> extends RuleContext<T, TC, ID, R, RRC> {

    Function<R, EID> idResolver();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<List<R>, RRC> fromListConverter();

    Function<RRC, List<R>> toListConverter();

    record OneToOneRuleMapperContext<T, TC extends Collection<T>, ID, R>(
            Function<T, ID> topLevelIdResolver,
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, R> mapFactory,
            Function<ID, R> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector,
            Function<List<R>, R> fromListConverter,
            Function<R, List<R>> toListConverter) implements RuleMapperContext<T, TC, ID, ID, R, R> {

        public OneToOneRuleMapperContext(
                RuleContext<T, TC, ID, R, R> ruleContext,
                Function<ID, R> defaultResultProvider) {

            this(ruleContext.topLevelIdResolver(),
                    ruleContext.correlationIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    defaultResultProvider,
                    initialMapCapacity -> toMap(
                            ruleContext.correlationIdResolver(),
                            identity(),
                            (u1, u2) -> u2,
                            toSupplier(validate(initialMapCapacity), ruleContext.mapFactory())),
                    CollectionUtils::first,
                    Collections::singletonList);
        }

        @Override
        public Function<R, ID> idResolver() {
            return correlationIdResolver();
        }
    }

    record OneToManyRuleMapperContext<T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>>(
            Function<R, EID> idResolver,
            Function<T, ID> topLevelIdResolver,
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RC> mapFactory,
            Function<ID, RC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector,
            Function<List<R>, RC> fromListConverter,
            Function<RC, List<R>> toListConverter) implements RuleMapperContext<T, TC, ID, EID, R, RC> {

        public OneToManyRuleMapperContext(
                RuleContext<T, TC, ID, R, RC> ruleContext,
                Function<R, EID> idResolver,
                Supplier<RC> collectionFactory) {

            this(idResolver,
                    ruleContext.topLevelIdResolver(),
                    ruleContext.correlationIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    id -> collectionFactory.get(),
                    initialMapCapacity -> groupingBy(
                            ruleContext.correlationIdResolver(),
                            toSupplier(validate(initialMapCapacity), ruleContext.mapFactory()),
                            toCollection(collectionFactory)),
                    list -> toStream(list).collect(toCollection(collectionFactory)),
                    List::copyOf);
        }
    }

    private static int validate(int initialCapacity) {
        return Math.max(initialCapacity, 0);
    }
}
