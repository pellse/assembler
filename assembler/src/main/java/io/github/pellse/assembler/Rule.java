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

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static io.github.pellse.assembler.RuleContext.ruleContext;
import static io.github.pellse.util.reactive.ReactiveUtils.subscribeMonoOn;

@FunctionalInterface
public interface Rule<T, K, RRC> extends Function<Function<T, K>, Function<Iterable<T>, Mono<Map<K, RRC>>>> {

    default Function<Iterable<T>, Mono<Map<K, RRC>>> apply(Function<T, K> keyMapper, Scheduler scheduler) {
       return apply(keyMapper, subscribeMonoOn(scheduler));
    }

    default Function<Iterable<T>, Mono<Map<K, RRC>>> apply(Function<T, K> keyMapper, UnaryOperator<Mono<Map<K, RRC>>> transformer) {
        var queryFunction = apply(keyMapper);
        return entities -> queryFunction.apply(entities).transform(transformer);
    }

    static <T, K, R, RRC> Rule<T, K, RRC> rule(
            Function<R, K> correlationIdResolver,
            RuleMapper<T, List<T>, K, K, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver), mapper);
    }

    static <T, TC extends Collection<T>, K, R, RRC> Rule<T, K, RRC> rule(
            Function<R, K> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            RuleMapper<T, TC, K, K, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver, topLevelCollectionFactory), mapper);
    }

    static <T, TC extends Collection<T>, K, R, RRC> Rule<T, K, RRC> rule(
            Function<R, K> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<K, RRC> mapFactory,
            RuleMapper<T, TC, K, K, R, RRC> mapper) {

        return ruleBuilder(ruleContext(correlationIdResolver, topLevelCollectionFactory, mapFactory), mapper);
    }

    static <T, K, ID, R, RRC> Rule<T, K, RRC> rule(
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            RuleMapper<T, List<T>, K, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(innerIdResolver, outerIdResolver), mapper);
    }

    static <T, TC extends Collection<T>, K, ID, R, RRC> Rule<T, K, RRC> rule(
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            RuleMapper<T, TC, K, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(innerIdResolver, outerIdResolver, topLevelCollectionFactory), mapper);
    }

    static <T, TC extends Collection<T>, K, ID, R, RRC> Rule<T, K, RRC> rule(
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            RuleMapper<T, TC, K, ID, R, RRC> mapper) {

        return ruleBuilder(ruleContext(innerIdResolver, outerIdResolver, topLevelCollectionFactory, mapFactory), mapper);
    }

    static <T, TC extends Collection<T>, K, ID, R, RRC> Rule<T, K, RRC> ruleBuilder(
            Function<Function<T, K>, RuleContext<T, TC, K, ID, R, RRC>> ruleContextBuilder,
            RuleMapper<T, TC, K, ID, R, RRC> mapper) {

        return topLevelIdResolver -> mapper.apply(ruleContextBuilder.apply(topLevelIdResolver));
    }
}
