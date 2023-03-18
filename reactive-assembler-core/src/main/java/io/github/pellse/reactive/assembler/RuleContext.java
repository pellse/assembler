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

public interface RuleContext<ID, IDC extends Collection<ID>, R, RRC> {

    static <ID, R, RRC> RuleContext<ID, List<ID>, R, RRC> ruleContext(Function<R, ID> correlationIdExtractor) {
        return ruleContext(correlationIdExtractor, ArrayList::new);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleContext<ID, IDC, R, RRC> ruleContext(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory) {
        return ruleContext(correlationIdExtractor, idCollectionFactory, defaultMapFactory());
    }

    static <ID, IDC extends Collection<ID>, R, RRC> RuleContext<ID, IDC, R, RRC> ruleContext(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory) {
        return new DefaultRuleContext<>(correlationIdExtractor, idCollectionFactory, mapFactory);
    }

    Function<R, ID> correlationIdExtractor();

    Supplier<IDC> idCollectionFactory();

    MapFactory<ID, RRC> mapFactory();

    record DefaultRuleContext<ID, IDC extends Collection<ID>, R, RRC>(
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory) implements RuleContext<ID, IDC, R, RRC> {
    }
}

interface IdAwareRuleContext<ID, EID, IDC extends Collection<ID>, R, RRC> extends RuleContext<ID, IDC, R, RRC> {

    static <ID, EID, IDC extends Collection<ID>, R, RRC> IdAwareRuleContext<ID, EID, IDC, R, RRC> toIdAwareRuleContext(
            Function<R, EID> idExtractor,
            RuleContext<ID, IDC, R, RRC> ruleContext) {

        return new IdAwareRuleContext<>() {
            @Override
            public Function<R, EID> idExtractor() {
                return idExtractor;
            }

            @Override
            public Function<R, ID> correlationIdExtractor() {
                return ruleContext.correlationIdExtractor();
            }

            @Override
            public Supplier<IDC> idCollectionFactory() {
                return ruleContext.idCollectionFactory();
            }

            @Override
            public MapFactory<ID, RRC> mapFactory() {
                return ruleContext.mapFactory();
            }
        };
    }

    Function<R, EID> idExtractor();
}
