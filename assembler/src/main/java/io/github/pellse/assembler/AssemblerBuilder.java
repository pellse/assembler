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

import io.github.pellse.util.function.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.pellse.util.ObjectUtils.get;
import static io.github.pellse.util.collection.CollectionUtils.toStream;
import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.zip;
import static reactor.core.scheduler.Schedulers.newBoundedElastic;

public interface AssemblerBuilder {

    static <R> WithCorrelationIdResolverBuilder<R> assemblerOf(@SuppressWarnings("unused") Class<R> outputClass) {
        return AssemblerBuilder::withCorrelationIdResolver;
    }

    static <T, K, R> WithRulesBuilder<T, K, R> withCorrelationIdResolver(Function<T, K> correlationIdResolver) {

        return (rules, aggregationFunction) -> scheduler -> {

            final var queryFunctions = rules.stream()
                    .map(rule -> rule.apply(correlationIdResolver, mono -> mono
                            .subscribeOn(requireNonNullElseGet(scheduler, Schedulers::immediate))
                            .onErrorResume(RejectedExecutionException.class, get(Mono::empty))))
                    .toList();

            final Function<Iterable<T>, Stream<Publisher<? extends Map<K, ?>>>> subQueryMapperBuilder = topLevelEntities -> queryFunctions.stream()
                    .map(queryFunction -> queryFunction.apply(topLevelEntities));

            final BiFunction<T, List<Map<K, ?>>, R> joinMapperResultsFunction =
                    (topLevelEntity, listOfMapperResults) -> aggregationFunction.apply(topLevelEntity,
                            listOfMapperResults.stream()
                                    .map(mapperResult -> mapperResult.get(correlationIdResolver.apply(topLevelEntity)))
                                    .toArray());

            final BiFunction<Iterable<T>, List<Map<K, ?>>, Stream<R>> aggregateStreamBuilder =
                    (topLevelEntities, mapperResults) -> toStream(topLevelEntities)
                            .filter(Objects::nonNull)
                            .map(topLevelEntity -> joinMapperResultsFunction.apply(topLevelEntity, mapperResults));

            return topLevelEntitiesProvider -> Flux.from(topLevelEntitiesProvider)
                    .collectList()
                    .flatMapMany(entities ->
                            zip(subQueryMapperBuilder.apply(entities).toList(),
                                    mapperResults -> aggregateStreamBuilder.apply(entities, toMapperResultList(mapperResults))))
                    .flatMapSequential(Flux::fromStream);
        };
    }

    @SuppressWarnings("unchecked")
    private static <K> List<Map<K, ?>> toMapperResultList(Object[] mapperResults) {

        return Stream.of(mapperResults)
                .map(mapResult -> (Map<K, ?>) mapResult)
                .collect(toList());
    }

    @FunctionalInterface
    interface WithCorrelationIdResolverBuilder<R> {

        <T, K> WithRulesBuilder<T, K, R> withCorrelationIdResolver(Function<T, K> correlationIdResolver);

        default <T, T2, K> WithRulesBuilder<T, K, R> withCorrelationIdResolver(Function<T, T2> mapper, Function<T2, K> correlationIdResolver) {
            return withCorrelationIdResolver(mapper.andThen(correlationIdResolver));
        }

        default <T, T2, T3, K> WithRulesBuilder<T, K, R> withCorrelationIdResolver(Function<T, T2> mapper1, Function<T2, T3> mapper2, Function<T3, K> correlationIdResolver) {
            return withCorrelationIdResolver(mapper1.andThen(mapper2).andThen(correlationIdResolver));
        }
    }

    @FunctionalInterface
    interface WithRulesBuilder<T, K, R> {

        @SuppressWarnings("unchecked")
        default <E1> Builder<T, R> withRules(
                Rule<T, K, E1> rule,
                BiFunction<T, E1, R> aggregationFunction) {

            return withRules(List.of(rule), (t, s) -> aggregationFunction.apply(t, (E1) s[0]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Function3<T, E1, E2, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2), (t, s) -> aggregationFunction.apply(t, (E1) s[0], (E2) s[1]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Function4<T, E1, E2, E3, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3),
                    (t, s) -> aggregationFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Function5<T, E1, E2, E3, E4, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4),
                    (t, s) -> aggregationFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Function6<T, E1, E2, E3, E4, E5, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5),
                    (t, s) -> aggregationFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Function7<T, E1, E2, E3, E4, E5, E6, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6),
                    (t, s) -> aggregationFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Rule<T, K, E7> rule7,
                Function8<T, E1, E2, E3, E4, E5, E6, E7, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7),
                    (t, s) -> aggregationFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Rule<T, K, E7> rule7,
                Rule<T, K, E8> rule8,
                Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8),
                    (t, s) -> aggregationFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Rule<T, K, E7> rule7,
                Rule<T, K, E8> rule8,
                Rule<T, K, E9> rule9,
                Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9),
                    (t, s) -> aggregationFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Rule<T, K, E7> rule7,
                Rule<T, K, E8> rule8,
                Rule<T, K, E9> rule9,
                Rule<T, K, E10> rule10,
                Function11<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10),
                    (t, s) -> aggregationFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11> Builder<T, R> withRules(
                Rule<T, K, E1> rule1,
                Rule<T, K, E2> rule2,
                Rule<T, K, E3> rule3,
                Rule<T, K, E4> rule4,
                Rule<T, K, E5> rule5,
                Rule<T, K, E6> rule6,
                Rule<T, K, E7> rule7,
                Rule<T, K, E8> rule8,
                Rule<T, K, E9> rule9,
                Rule<T, K, E10> rule10,
                Rule<T, K, E11> rule11,
                Function12<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> aggregationFunction) {

            return withRules(List.of(rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8, rule9, rule10, rule11),
                    (t, s) -> aggregationFunction.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9], (E11) s[10]));
        }

        Builder<T, R> withRules(List<Rule<T, K, ?>> rules, BiFunction<T, Object[], R> aggregationFunction);
    }

    @FunctionalInterface
    interface Builder<T, R> {

        default Assembler<T, R> build() {
            return build(null);
        }

        default Assembler<T, R> build(int queuedRuleAggregationTaskCap) {
            return build(getRuntime().availableProcessors(), queuedRuleAggregationTaskCap);
        }

        default Assembler<T, R> build(int ruleAggregationThreadCap, int ruleAggregationQueuedTaskCap) {
            return build(newBoundedElastic(ruleAggregationThreadCap, ruleAggregationQueuedTaskCap, "Rule-Aggregation-Scheduler"));
        }

        Assembler<T, R> build(Scheduler scheduler);
    }
}
