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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static io.github.pellse.assembler.Rule.ruleBuilder;
import static io.github.pellse.assembler.RuleContext.ruleContext;
import static io.github.pellse.util.collection.CollectionUtils.size;
import static io.github.pellse.util.collection.CollectionUtils.toStream;
import static java.util.LinkedHashMap.newLinkedHashMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static reactor.core.publisher.Flux.fromIterable;

@FunctionalInterface
public interface BatchRule<T, RRC> {

    Mono<Map<T, RRC>> toMono(Iterable<T> entities);

    default Flux<RRC> toFlux(Iterable<T> entities) {
        return toMono(entities)
                .flatMapMany(resultMap -> fromIterable(resultMap.values()));
    }

    interface BatchRuleBuilder<T, K> {

        <R, RRC> BatchRule<T, RRC> createRule(
                Function<R, K> correlationIdResolver,
                RuleMapper<T, K, K, R, RRC> mapper);

        <R, RRC> BatchRule<T, RRC> createRule(
                Function<R, K> correlationIdResolver,
                MapFactory<K, RRC> mapFactory,
                RuleMapper<T, K, K, R, RRC> mapper);

        <ID, R, RRC> BatchRule<T, RRC> createRule(
                Function<R, ID> innerIdResolver,
                Function<T, ID> outerIdResolver,
                RuleMapper<T, K, ID, R, RRC> mapper);

        <ID, R, RRC> BatchRule<T, RRC> createRule(
                Function<R, ID> innerIdResolver,
                Function<T, ID> outerIdResolver,
                MapFactory<ID, RRC> mapFactory,
                RuleMapper<T, K, ID, R, RRC> mapper);
    }

    static <T, K> BatchRuleBuilder<T, K> withIdResolver(Function<T, K> idResolver) {

        return new BatchRuleBuilder<>() {

            @Override
            public <R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, K> correlationIdResolver,
                    RuleMapper<T, K, K, R, RRC> mapper) {

                return createBatchRule(ruleContext(correlationIdResolver), mapper);
            }

            @Override
            public <R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, K> correlationIdResolver,
                    MapFactory<K, RRC> mapFactory,
                    RuleMapper<T, K, K, R, RRC> mapper) {

                return createBatchRule(ruleContext(correlationIdResolver, mapFactory), mapper);
            }

            @Override
            public <ID, R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, ID> innerIdResolver,
                    Function<T, ID> outerIdResolver,
                    RuleMapper<T, K, ID, R, RRC> mapper) {

                return createBatchRule(ruleContext(innerIdResolver, outerIdResolver), mapper);
            }

            @Override
            public <ID, R, RRC> BatchRule<T, RRC> createRule(
                    Function<R, ID> innerIdResolver,
                    Function<T, ID> outerIdResolver,
                    MapFactory<ID, RRC> mapFactory,
                    RuleMapper<T, K, ID, R, RRC> mapper) {

                return createBatchRule(ruleContext(innerIdResolver, outerIdResolver, mapFactory), mapper);
            }

            private <ID, R, RRC> BatchRule<T, RRC> createBatchRule(
                    Function<Function<T, K>, RuleContext<T, K, ID, R, RRC>> ruleContextBuilder,
                    RuleMapper<T, K, ID, R, RRC> mapper) {

                return wrap(idResolver, ruleBuilder(ruleContextBuilder, mapper));
            }
        };
    }

    private static <T, K, RRC> BatchRule<T, RRC> wrap(Function<T, K> idResolver, Rule<T, K, RRC> rule) {

        final var queryFunction = rule.apply(idResolver);

        return entities -> {
            final var size = size(entities);

            final Map<K, T> entityMap = toStream(entities)
                    .collect(toMap(idResolver, identity(), (o, o2) -> o2, () -> newLinkedHashMap(size)));

            return queryFunction.apply(entities)
                    .map(resultMap -> resultMap.entrySet()
                            .stream()
                            .collect(toMap(m -> entityMap.get(m.getKey()), Entry::getValue, (o, o2) -> o2, () -> newLinkedHashMap(size))));
        };
    }
}