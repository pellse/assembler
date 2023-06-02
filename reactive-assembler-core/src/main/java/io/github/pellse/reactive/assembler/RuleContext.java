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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.MapFactory.defaultMapFactory;

public interface RuleContext<T, TC extends Collection<T>, ID, R, RRC> {

    static <T, ID, R, RRC> Function<Function<T, ID>, RuleContext<T, List<T>, ID, R, RRC>> ruleContext(Function<R, ID> correlationIdExtractor) {
        return ruleContext(correlationIdExtractor, ArrayList::new);
    }

    static <T, TC extends Collection<T>, ID, R, RRC> Function<Function<T, ID>, RuleContext<T, TC, ID, R, RRC>> ruleContext(
            Function<R, ID> correlationIdExtractor,
            Supplier<TC> topLevelCollectionFactory) {
        return ruleContext(correlationIdExtractor, topLevelCollectionFactory, defaultMapFactory());
    }

    static <T, TC extends Collection<T>, ID, R, RRC> Function<Function<T, ID>, RuleContext<T, TC, ID, R, RRC>> ruleContext(
            Function<R, ID> correlationIdExtractor,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RRC> mapFactory) {
        return topLevelIdExtractor -> new DefaultRuleContext<>(topLevelIdExtractor, correlationIdExtractor, topLevelCollectionFactory, mapFactory);
    }

    Function<T, ID> topLevelIdExtractor();

    Function<R, ID> correlationIdExtractor();

    Supplier<TC> topLevelCollectionFactory();

    MapFactory<ID, RRC> mapFactory();

    record DefaultRuleContext<T, TC extends Collection<T>, ID, R, RRC>(
            Function<T, ID> topLevelIdExtractor,
            Function<R, ID> correlationIdExtractor,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RRC> mapFactory) implements RuleContext<T, TC, ID, R, RRC> {
    }
}

interface IdAwareRuleContext<T, TC extends Collection<T>, ID, EID, R, RRC> extends RuleContext<T, TC, ID, R, RRC> {

    static <T, TC extends Collection<T>, ID, EID, R, RRC> IdAwareRuleContext<T, TC, ID, EID, R, RRC> toIdAwareRuleContext(
            Function<R, EID> idExtractor,
            RuleContext<T, TC, ID, R, RRC> ruleContext) {

        return new IdAwareRuleContext<>() {

            @Override
            public Function<T, ID> topLevelIdExtractor() {
                return ruleContext.topLevelIdExtractor();
            }

            @Override
            public Function<R, EID> idExtractor() {
                return idExtractor;
            }

            @Override
            public Function<R, ID> correlationIdExtractor() {
                return ruleContext.correlationIdExtractor();
            }

            @Override
            public Supplier<TC> topLevelCollectionFactory() {
                return ruleContext.topLevelCollectionFactory();
            }

            @Override
            public MapFactory<ID, RRC> mapFactory() {
                return ruleContext.mapFactory();
            }
        };
    }

    Function<R, EID> idExtractor();
}
