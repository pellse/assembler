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

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.RuleContext.ruleContext;
import static io.github.pellse.util.ObjectUtils.then;

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
}
