/*
 * Copyright 2018 Sebastien Pelletier
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

import io.github.pellse.util.function.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.pellse.reactive.assembler.FluxAdapter.fluxAdapter;
import static io.github.pellse.util.collection.CollectionUtil.toStream;

public interface AssemblerBuilder {

    static <R> WithCorrelationIdExtractorBuilder<R> assemblerOf(@SuppressWarnings("unused") Class<R> outputClass) {
        return new WithCorrelationIdExtractorBuilderImpl<>();
    }

    @FunctionalInterface
    interface WithCorrelationIdExtractorBuilder<R> {
        <T, ID> WithAssemblerRulesBuilder<T, ID, R> withCorrelationIdExtractor(Function<T, ID> correlationIdExtractor);
    }

    @FunctionalInterface
    interface WithAssemblerRulesBuilder<T, ID, R> {

        @SuppressWarnings("unchecked")
        default <E1> AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule,
                BiFunction<T, E1, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule), (t, s) -> assemblerFunction.apply(t, (E1) s[0]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Function3<T, E1, E2, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2), (t, s) -> assemblerFunction.apply(t, (E1) s[0], (E2) s[1]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Function4<T, E1, E2, E3, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3),
                    (t, s) -> assemblerFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Function5<T, E1, E2, E3, E4, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4),
                    (t, s) -> assemblerFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Function6<T, E1, E2, E3, E4, E5, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5),
                    (t, s) -> assemblerFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Function7<T, E1, E2, E3, E4, E5, E6, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6),
                    (t, s) -> assemblerFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Rule<ID, E7> rule7,
                Function8<T, E1, E2, E3, E4, E5, E6, E7, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7),
                    (t, s) -> assemblerFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Rule<ID, E7> rule7,
                Rule<ID, E8> rule8,
                Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8),
                    (t, s) -> assemblerFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Rule<ID, E7> rule7,
                Rule<ID, E8> rule8,
                Rule<ID, E9> rule9,
                Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9),
                    (t, s) -> assemblerFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Rule<ID, E7> rule7,
                Rule<ID, E8> rule8,
                Rule<ID, E9> rule9,
                Rule<ID, E10> rule10,
                Function11<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10),
                    (t, s) -> assemblerFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11>
        AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                Rule<ID, E1> rule1,
                Rule<ID, E2> rule2,
                Rule<ID, E3> rule3,
                Rule<ID, E4> rule4,
                Rule<ID, E5> rule5,
                Rule<ID, E6> rule6,
                Rule<ID, E7> rule7,
                Rule<ID, E8> rule8,
                Rule<ID, E9> rule9,
                Rule<ID, E10> rule10,
                Rule<ID, E11> rule11,
                Function12<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> assemblerFunction) {

            return withAssemblerRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10, rule11),
                    (t, s) -> assemblerFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9], (E11) s[10]));
        }

        AssembleUsingBuilder<T, ID, R> withAssemblerRules(List<Rule<ID, ?>> rules,
                                                          BiFunction<T, Object[], R> aggregationFunction);
    }

    interface AssembleUsingBuilder<T, ID, R> {

        Assembler<T, Flux<R>> build();

        Assembler<T, Flux<R>> build(Scheduler scheduler);

        <RC> Assembler<T, RC> build(AssemblerAdapter<T, ID, R, RC> adapter);
    }

    class WithCorrelationIdExtractorBuilderImpl<R> implements WithCorrelationIdExtractorBuilder<R> {

        private WithCorrelationIdExtractorBuilderImpl() {
        }

        @Override
        public <T, ID> WithAssemblerRulesBuilder<T, ID, R> withCorrelationIdExtractor(Function<T, ID> correlationIdExtractor) {
            return new WithAssemblerRulesBuilderImpl<>(correlationIdExtractor);
        }
    }

    class WithAssemblerRulesBuilderImpl<T, ID, R> implements WithAssemblerRulesBuilder<T, ID, R> {

        private final Function<T, ID> correlationIdExtractor;

        private WithAssemblerRulesBuilderImpl(Function<T, ID> correlationIdExtractor) {
            this.correlationIdExtractor = correlationIdExtractor;
        }

        @Override
        public AssembleUsingBuilder<T, ID, R> withAssemblerRules(
                List<Rule<ID, ?>> rules,
                BiFunction<T, Object[], R> aggregationFunction) {
            return new AssembleUsingBuilderImpl<>(correlationIdExtractor, rules, aggregationFunction);
        }
    }

    class AssembleUsingBuilderImpl<T, ID, R> implements AssembleUsingBuilder<T, ID, R> {

        private final Function<T, ID> correlationIdExtractor;
        private final BiFunction<T, Object[], R> aggregationFunction;
        private final List<Rule<ID, ?>> rules;

        private AssembleUsingBuilderImpl(
                Function<T, ID> correlationIdExtractor,
                List<Rule<ID, ?>> rules,
                BiFunction<T, Object[], R> aggregationFunction) {

            this.correlationIdExtractor = correlationIdExtractor;

            this.aggregationFunction = aggregationFunction;
            this.rules = rules;
        }

        @Override
        public Assembler<T, Flux<R>> build() {
            return build(fluxAdapter());
        }

        @Override
        public Assembler<T, Flux<R>> build(Scheduler scheduler) {
            return build(fluxAdapter(scheduler));
        }


        @Override
        public <RC> Assembler<T, RC> build(AssemblerAdapter<T, ID, R, RC> assemblerAdapter) {
            return new AssemblerImpl<>(correlationIdExtractor, rules, aggregationFunction, assemblerAdapter);
        }
    }

    class AssemblerImpl<T, ID, R, RC> implements Assembler<T, RC> {

        private final AssemblerAdapter<T, ID, R, RC> assemblerAdapter;
        private final Function<Iterable<T>, Stream<Publisher<? extends Map<ID, ?>>>> subQueryMapperBuilder;
        private final BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder;

        private AssemblerImpl(
                Function<T, ID> correlationIdExtractor,
                List<Rule<ID, ?>> rules,
                BiFunction<T, Object[], R> aggregationFunction,
                AssemblerAdapter<T, ID, R, RC> assemblerAdapter) {

            this.assemblerAdapter = assemblerAdapter;

            this.subQueryMapperBuilder = topLevelEntities -> buildSubQueryMappersFromRules(topLevelEntities, correlationIdExtractor, rules);

            BiFunction<T, List<Map<ID, ?>>, R> joinMapperResultsFunction =
                    (topLevelEntity, listOfMapperResults) -> aggregationFunction.apply(topLevelEntity,
                            listOfMapperResults.stream()
                                    .map(mapperResult -> mapperResult.get(correlationIdExtractor.apply(topLevelEntity)))
                                    .toArray());

            this.aggregateStreamBuilder =
                    (topLevelEntities, mapperResults) -> toStream(topLevelEntities)
                            .filter(Objects::nonNull)
                            .map(topLevelEntity -> joinMapperResultsFunction.apply(topLevelEntity, mapperResults));
        }

        private static <T, ID> Stream<Publisher<? extends Map<ID, ?>>> buildSubQueryMappersFromRules(
                Iterable<T> topLevelEntities,
                Function<T, ID> correlationIdExtractor,
                List<Rule<ID, ?>> rules) {

            List<ID> entityIDs = toStream(topLevelEntities)
                    .filter(Objects::nonNull)
                    .map(correlationIdExtractor)
                    .toList();

            return rules.stream()
                    .map(rule -> rule.apply(entityIDs));
        }

        @Override
        public RC assemble(Publisher<T> topLevelEntitiesProvider) {
            return assemblerAdapter.convertSubQueryMappers(topLevelEntitiesProvider, subQueryMapperBuilder, aggregateStreamBuilder);
        }
    }
}
