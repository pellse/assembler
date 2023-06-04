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

package io.github.pellse.cohereflux;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

public interface RuleMapperContext<T, TC extends Collection<T>, ID, EID, R, RRC> extends RuleContext<T, TC, ID, R, RRC> {

    static <T, TC extends Collection<T>, ID, EID, R, RRC> DefaultRuleMapperContext<T, TC, ID, EID, R, RRC> toRuleMapperContext(
            IdAwareRuleContext<T, TC, ID, EID, R, RRC> ruleContext,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        return new DefaultRuleMapperContext<>(
                ruleContext.idResolver(),
                ruleContext.topLevelIdResolver(),
                ruleContext.correlationIdResolver(),
                ruleContext.topLevelCollectionFactory(),
                ruleContext.mapFactory(),
                defaultResultProvider,
                mapCollector,
                fromListConverter,
                toListConverter);
    }

    Function<R, EID> idResolver();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<List<R>, RRC> fromListConverter();

    Function<RRC, List<R>> toListConverter();

    record DefaultRuleMapperContext<T, TC extends Collection<T>, ID, EID, R, RRC>(
            Function<R, EID> idResolver,
            Function<T, ID> topLevelIdResolver,
            Function<R, ID> correlationIdResolver,
            Supplier<TC> topLevelCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) implements RuleMapperContext<T, TC, ID, EID, R, RRC> {
    }
}
