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

package io.github.pellse.assembler.caching;

import io.github.pellse.util.function.Function3;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.Map;
import java.util.function.Function;

import static io.github.pellse.util.reactive.ReactiveUtils.subscribeMonoOn;

public interface Cache<ID, RRC> {

    @FunctionalInterface
    interface MergeFunction<ID, RRC> extends Function3<ID, RRC, RRC, RRC> {}

    @FunctionalInterface
    interface FetchFunction<ID, RRC> extends Function<Iterable<? extends ID>, Mono<Map<ID, RRC>>> {

        default Mono<Map<ID, RRC>> apply(Iterable<? extends ID> ids, Scheduler scheduler) {
            return apply(ids).transform(subscribeMonoOn(scheduler));
        }
    }

    Mono<Map<ID, RRC>> getAll(Iterable<ID> ids);

    Mono<Map<ID, RRC>> computeAll(Iterable<ID> ids, FetchFunction<ID, RRC> fetchFunction);

    Mono<?> putAll(Map<ID, RRC> map);

    Mono<?> removeAll(Map<ID, RRC> map);

    default Mono<?> updateAll(Map<ID, RRC> mapToAdd, Map<ID, RRC> mapToRemove) {
        return putAll(mapToAdd).then(removeAll(mapToRemove));
    }
}
