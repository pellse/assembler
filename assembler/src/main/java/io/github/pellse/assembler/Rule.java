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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.assembler.RuleContext.ruleContext;
import static io.github.pellse.util.collection.CollectionUtils.toStream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static reactor.core.publisher.Flux.fromIterable;

@FunctionalInterface
public interface Rule<T, ID, RRC> extends Function<Function<T, ID>, Function<Iterable<T>, Mono<Map<ID, RRC>>>> {

    static <T, ID, R, RRC> Rule<T, ID, RRC> rule(
            Function<R, ID> correlationIdResolver,
            RuleMapper<T, List<T>, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver), mapper);
    }

    static <T, TC extends Collection<T>, ID, R, RRC> Rule<T, ID, RRC> rule(
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            RuleMapper<T, TC, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver, topLevelCollectionFactory), mapper);
    }

    static <T, TC extends Collection<T>, ID, R, RRC> Rule<T, ID, RRC> rule(
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            RuleMapper<T, TC, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver, topLevelCollectionFactory, mapFactory), mapper);
    }

    private static <T, TC extends Collection<T>, ID, R, RRC> Rule<T, ID, RRC> ruleBuilder(
            Function<Function<T, ID>, RuleContext<T, TC, ID, R, RRC>> ruleContextBuilder,
            RuleMapper<T, TC, ID, R, RRC> mapper) {

        return topLevelIdResolver -> mapper.apply(ruleContextBuilder.apply(topLevelIdResolver));
    }

    static <T, ID> BatchRuleBuilder<T, ID> withIdResolver(Function<T, ID> idResolver) {

        return new BatchRuleBuilder<>() {

            @Override
            public <R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, ID> correlationIdResolver,
                    RuleMapper<T, List<T>, ID, R, RRC> mapper) {

                return createBatchRule(ruleContext(correlationIdResolver), mapper);
            }

            @Override
            public <TC extends Collection<T>, R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, ID> correlationIdResolver,
                    Supplier<TC> topLevelCollectionFactory,
                    RuleMapper<T, TC, ID, R, RRC> mapper) {

                return createBatchRule(ruleContext(correlationIdResolver, topLevelCollectionFactory), mapper);
            }

            @Override
            public <TC extends Collection<T>, R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, ID> correlationIdResolver,
                    Supplier<TC> topLevelCollectionFactory,
                    MapFactory<ID, RRC> mapFactory,
                    RuleMapper<T, TC, ID, R, RRC> mapper) {

                return createBatchRule(ruleContext(correlationIdResolver, topLevelCollectionFactory, mapFactory), mapper);
            }

            private <TC extends Collection<T>, R, RRC> BatchRule<T, RRC> createBatchRule(
                    Function<Function<T, ID>, RuleContext<T, TC, ID, R, RRC>> ruleContextBuilder,
                    RuleMapper<T, TC, ID, R, RRC> mapper) {

                return wrap(idResolver, ruleBuilder(ruleContextBuilder, mapper));
            }
        };
    }

    private static <T, ID, RRC> BatchRule<T, RRC> wrap(Function<T, ID> idResolver, Rule<T, ID, RRC> rule) {

        final var queryFunction = rule.apply(idResolver);

        return entities -> {
            final Map<ID, T> entityMap = toStream(entities)
                    .collect(toMap(idResolver, identity(), (o, o2) -> o2, LinkedHashMap::new));

            return queryFunction.apply(entities)
                    .map(resultMap -> resultMap.entrySet()
                            .stream()
                            .collect(toMap(m -> entityMap.get(m.getKey()), Entry::getValue, (o, o2) -> o2, LinkedHashMap::new)));
        };
    }

    @FunctionalInterface
    interface BatchRule<T, RRC> {

        Mono<Map<T, RRC>> executeToMono(Iterable<T> entities);

        default Flux<RRC> executeToFlux(Iterable<T> entities) {
            return executeToMono(entities)
                    .flatMapMany(resultMap -> fromIterable(resultMap.values()));
        }
    }

    interface BatchRuleBuilder<T, ID> {

        <R, RRC> BatchRule<T, RRC> createRule(
                Function<R, ID> correlationIdResolver,
                RuleMapper<T, List<T>, ID, R, RRC> mapper);

        <TC extends Collection<T>, R, RRC> BatchRule<T, RRC> createRule(
                Function<R, ID> correlationIdResolver,
                Supplier<TC> topLevelCollectionFactory,
                RuleMapper<T, TC, ID, R, RRC> mapper);

        <TC extends Collection<T>, R, RRC> BatchRule<T, RRC> createRule(
                Function<R, ID> correlationIdResolver,
                Supplier<TC> topLevelCollectionFactory,
                MapFactory<ID, RRC> mapFactory,
                RuleMapper<T, TC, ID, R, RRC> mapper);
    }
}
