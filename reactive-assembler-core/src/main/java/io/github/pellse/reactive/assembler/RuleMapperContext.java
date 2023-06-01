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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

public interface RuleMapperContext<T, ID, EID, IDC extends Collection<ID>, R, RRC> extends RuleContext<T, ID, IDC, R, RRC> {

    static <T, ID, EID, IDC extends Collection<ID>, R, RRC> DefaultRuleMapperContext<T, ID, EID, IDC, R, RRC> toRuleMapperContext(
            IdAwareRuleContext<T, ID, EID, IDC, R, RRC> ruleContext,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        return new DefaultRuleMapperContext<>(
                ruleContext.idExtractor(),
                ruleContext.topLevelIdExtractor(),
                ruleContext.correlationIdExtractor(),
                ruleContext.idCollectionFactory(),
                ruleContext.mapFactory(),
                defaultResultProvider,
                mapCollector,
                fromListConverter,
                toListConverter);
    }

    Function<R, EID> idExtractor();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<List<R>, RRC> fromListConverter();

    Function<RRC, List<R>> toListConverter();

    record DefaultRuleMapperContext<T, ID, EID, IDC extends Collection<ID>, R, RRC>(
            Function<R, EID> idExtractor,
            Function<T, ID> topLevelIdExtractor,
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) implements RuleMapperContext<T, ID, EID, IDC, R, RRC> {
    }
}
