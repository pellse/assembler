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

import java.util.function.Function;

import static io.github.pellse.assembler.MapFactory.defaultMapFactory;

public interface RuleContext<T, K, ID, R, RRC> {

    Function<T, K> topLevelIdResolver();

    Function<R, ID> innerIdResolver();

    Function<T, ID> outerIdResolver();

    MapFactory<ID, RRC> mapFactory();

    record DefaultRuleContext<T, K, ID, R, RRC>(
            Function<T, K> topLevelIdResolver,
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            MapFactory<ID, RRC> mapFactory) implements RuleContext<T, K, ID, R, RRC> {
    }

    static <T, K, R, RRC> Function<Function<T, K>, RuleContext<T, K, K, R, RRC>> ruleContext(
            Function<R, K> innerIdResolver) {

        return ruleContext(innerIdResolver, defaultMapFactory());
    }

    static <T, K, R, RRC> Function<Function<T, K>, RuleContext<T, K, K, R, RRC>> ruleContext(
            Function<R, K> innerIdResolver,
            MapFactory<K, RRC> mapFactory) {

        return topLevelIdResolver -> new DefaultRuleContext<>(topLevelIdResolver, innerIdResolver, topLevelIdResolver, mapFactory);
    }

    static <T, K, ID, R, RRC> Function<Function<T, K>, RuleContext<T, K, ID, R, RRC>> ruleContext(
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver) {

        return ruleContext(innerIdResolver, outerIdResolver, defaultMapFactory());
    }

    static <T, K, ID, R, RRC> Function<Function<T, K>, RuleContext<T, K, ID, R, RRC>> ruleContext(
            Function<R, ID> innerIdResolver,
            Function<T, ID> outerIdResolver,
            MapFactory<ID, RRC> mapFactory) {

        return topLevelIdResolver -> new DefaultRuleContext<>(topLevelIdResolver, innerIdResolver, outerIdResolver, mapFactory);
    }
}
