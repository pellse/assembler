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

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.github.pellse.assembler.QueryUtils.toSupplier;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public sealed interface RuleMapperContext<T, TC extends Collection<T>, ID, EID, R, RRC> extends RuleContext<T, TC, ID, R, RRC> {

    Function<R, EID> idResolver();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<Stream<RRC>, Stream<R>> streamFlattener();

    record OneToOneContext<T, TC extends Collection<T>, ID, R>(
            Function<T, ID> topLevelIdResolver,
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, R> mapFactory,
            Function<ID, R> defaultResultProvider) implements RuleMapperContext<T, TC, ID, ID, R, R> {

        public OneToOneContext(
                RuleContext<T, TC, ID, R, R> ruleContext,
                Function<ID, R> defaultResultProvider) {

            this(ruleContext.topLevelIdResolver(),
                    ruleContext.correlationIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    defaultResultProvider);
        }

        @Override
        public Function<R, ID> idResolver() {
            return correlationIdResolver();
        }

        @Override
        public IntFunction<Collector<R, ?, Map<ID, R>>> mapCollector() {
            return initialMapCapacity -> toMap(
                    correlationIdResolver(),
                    identity(),
                    (u1, u2) -> u2,
                    toSupplier(validate(initialMapCapacity), mapFactory()));
        }

        @Override
        public Function<Stream<R>, Stream<R>> streamFlattener() {
            return identity();
        }
    }

    record OneToManyContext<T, TC extends Collection<T>, ID, EID, R, RC extends Collection<R>>(
            Function<T, ID> topLevelIdResolver,
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RC> mapFactory,
            Function<R, EID> idResolver,
            Supplier<RC> collectionFactory) implements RuleMapperContext<T, TC, ID, EID, R, RC> {

        public OneToManyContext(
                RuleContext<T, TC, ID, R, RC> ruleContext,
                Function<R, EID> idResolver,
                Supplier<RC> collectionFactory) {

            this(ruleContext.topLevelIdResolver(),
                    ruleContext.correlationIdResolver(),
                    ruleContext.topLevelCollectionFactory(),
                    ruleContext.mapFactory(),
                    idResolver,
                    collectionFactory);
        }

        @Override
        public Function<ID, RC> defaultResultProvider() {
            return id -> collectionFactory.get();
        }

        @Override
        public IntFunction<Collector<R, ?, Map<ID, RC>>> mapCollector() {
            return initialMapCapacity -> groupingBy(
                    correlationIdResolver(),
                    toSupplier(validate(initialMapCapacity), mapFactory()),
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
