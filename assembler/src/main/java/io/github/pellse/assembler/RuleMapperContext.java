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

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.github.pellse.assembler.QueryUtils.toMapSupplier;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public sealed interface RuleMapperContext<T, TC extends Collection<T>, K, ID, EID, R, RRC> extends RuleContext<T, TC, K, ID, R, RRC> {

    Function<R, EID> idResolver();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<Stream<RRC>, Stream<R>> streamFlattener();

    record OneToOneContext<T, TC extends Collection<T>, K, ID, R>(
            Function<T, K> topLevelIdResolver,
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, R> mapFactory,
            Function<ID, R> defaultResultProvider) implements RuleMapperContext<T, TC, K, ID, ID, R, R> {

        public OneToOneContext(
                RuleContext<T, TC, K, ID, R, R> ruleContext,
                Function<ID, R> defaultResultProvider) {

            this(ruleContext.topLevelIdResolver(),
                    ruleContext.innerIdResolver(),
                    ruleContext.outerIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    defaultResultProvider);
        }

        @Override
        public Function<R, ID> idResolver() {
            return innerIdResolver();
        }

        @Override
        public IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector() {
            return initialMapCapacity -> toMap(
                    innerIdResolver(),
                    identity(),
                    (u1, u2) -> u2,
                    toMapSupplier(validate(initialMapCapacity), mapFactory()));
        }

        @Override
        public Function<Stream<R>, Stream<R>> streamFlattener() {
            return identity();
        }
    }

    record OneToManyContext<T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>>(
            Function<T, K> topLevelIdResolver,
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RC> mapFactory,
            Function<R, EID> idResolver,
            Comparator<R> idComparator,
            Supplier<RC> collectionFactory,
            Class<RC> collectionType) implements RuleMapperContext<T, TC, K, ID, EID, R, RC> {

        @SuppressWarnings("unchecked")
        public static <T, TC extends Collection<T>, K, ID, EID, R, RC extends Collection<R>> OneToManyContext<T, TC, K, ID, EID, R, RC> oneToManyContext(
                RuleContext<T, TC, K, ID, R, RC> ruleContext,
                Function<R, EID> idResolver,
                Comparator<R> idComparator,
                Supplier<RC> collectionFactory) {

            return new OneToManyContext<>(ruleContext.topLevelIdResolver(),
                    ruleContext.innerIdResolver(),
                    ruleContext.outerIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    idResolver,
                    idComparator,
                    collectionFactory,
                    (Class<RC>) collectionFactory.get().getClass());
        }

        @Override
        public Function<ID, RC> defaultResultProvider() {
            return id -> collectionFactory.get();
        }

        @Override
        public IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector() {
            return initialMapCapacity -> groupingBy(
                    innerIdResolver(),
                    toMapSupplier(validate(initialMapCapacity), mapFactory()),
                    toCollection(collectionFactory));
        }

        @Override
        public Function<Stream<RC>, Stream<R>> streamFlattener() {
            return stream -> stream.flatMap(Collection::stream);
        }
    }

    private static int validate(int initialCapacity) {
        return Math.max(initialCapacity, 0);
    }
}
