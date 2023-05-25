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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.RuleContext.ruleContext;
import static io.github.pellse.util.ObjectUtils.then;
import static io.github.pellse.util.collection.CollectionUtil.toStream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static reactor.core.publisher.Flux.fromIterable;

@FunctionalInterface
public interface Rule<ID, RRC> extends Function<Iterable<ID>, Mono<Map<ID, RRC>>> {

    static <ID, R, RRC> Rule<ID, RRC> rule(
            Function<R, ID> correlationIdExtractor,
            RuleMapper<ID, List<ID>, R, RRC> mapper) {
        return rule(ruleContext(correlationIdExtractor), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> Rule<ID, RRC> rule(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            RuleMapper<ID, IDC, R, RRC> mapper) {
        return rule(ruleContext(correlationIdExtractor, idCollectionFactory), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> Rule<ID, RRC> rule(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            RuleMapper<ID, IDC, R, RRC> mapper) {
        return rule(ruleContext(correlationIdExtractor, idCollectionFactory, mapFactory), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> Rule<ID, RRC> rule(
            RuleContext<ID, IDC, R, RRC> ruleContext,
            RuleMapper<ID, IDC, R, RRC> mapper) {
        return then(mapper.apply(ruleContext), queryFunction -> queryFunction::apply);
    }

    static <ID, R, RRC> BatchRuleBuilder<ID, RRC> batchRuleBuilder(
            Function<R, ID> correlationIdExtractor,
            RuleMapper<ID, List<ID>, R, RRC> mapper) {
        return batchRuleBuilder(ruleContext(correlationIdExtractor), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> BatchRuleBuilder<ID, RRC> batchRuleBuilder(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            RuleMapper<ID, IDC, R, RRC> mapper) {
        return batchRuleBuilder(ruleContext(correlationIdExtractor, idCollectionFactory), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> BatchRuleBuilder<ID, RRC> batchRuleBuilder(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            RuleMapper<ID, IDC, R, RRC> mapper) {
        return batchRuleBuilder(ruleContext(correlationIdExtractor, idCollectionFactory, mapFactory), mapper);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> BatchRuleBuilder<ID, RRC> batchRuleBuilder(
            RuleContext<ID, IDC, R, RRC> ruleContext,
            RuleMapper<ID, IDC, R, RRC> mapper) {

        return new BatchRuleBuilder<>() {

            @Override
            public <T> BatchRule<T, RRC> withIdExtractor(Function<T, ID> idExtractor) {
                return wrap(idExtractor, rule(ruleContext, mapper));
            }
        };
    }

    private static <T, ID, RRC> BatchRule<T, RRC> wrap(Function<T, ID> idExtractor, Rule<ID, RRC> rule) {

        return entities -> {
            final Map<ID, T> entityMap = toStream(entities)
                    .collect(toMap(idExtractor, identity(), (o, o2) -> o2, LinkedHashMap::new));

            return rule.apply(entityMap.keySet())
                    .map(resultMap -> resultMap.entrySet()
                            .stream()
                            .collect(toMap(m -> entityMap.get(m.getKey()), Entry::getValue, (o, o2) -> o2, LinkedHashMap::new)));
        };
    }

    @FunctionalInterface
    interface BatchRule<T, RRC> {

        Mono<Map<T, RRC>> toMono(Iterable<T> entities);

        default Flux<RRC> toFlux(Iterable<T> entities) {
            return toMono(entities)
                    .flatMapMany(resultMap -> fromIterable(resultMap.values()));
        }
    }

    interface BatchRuleBuilder<ID, RRC> {
        <T> BatchRule<T, RRC> withIdExtractor(Function<T, ID> idExtractor);
    }
}
