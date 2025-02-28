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

package io.github.pellse.util.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.util.collection.CollectionUtils.toLinkedHashMap;
import static java.lang.Math.toIntExact;
import static java.util.List.copyOf;
import static java.util.function.Function.identity;
import static reactor.core.publisher.Flux.concat;
import static reactor.core.publisher.Mono.*;
import static reactor.core.scheduler.Schedulers.*;

public interface ReactiveUtils {

    static <T, RRC> Mono<Map<T, RRC>> resolve(Map<T, Mono<RRC>> monoMap) {

        final var monoLinkedMap = toLinkedHashMap(monoMap);
        final var keys = copyOf(monoLinkedMap.keySet());

        return concat(monoLinkedMap.values())
                .index()
                .collectMap(tuple2 -> keys.get(toIntExact(tuple2.getT1())), Tuple2::getT2);
    }

    static <T, RRC> Map<T, Sinks.One<RRC>> createSinkMap(Iterable<T> iterable) {
        return toLinkedHashMap(iterable, identity(), __ -> Sinks.one());
    }

    static <T> Mono<T> nullToEmpty(T value) {
        return value != null ? just(value) : empty();
    }

    static <T> Mono<T> nullToEmpty(Supplier<T> defaultValueProvider) {
        return defaultValueProvider != null ? fromSupplier(defaultValueProvider) : empty();
    }

    static boolean isVirtualThreadSupported() {
        return DEFAULT_BOUNDED_ELASTIC_ON_VIRTUAL_THREADS;
    }

    static <T> Function<Flux<T>, Flux<T>> subscribeFluxOn(Scheduler scheduler) {
        return scheduleFluxOn(scheduler, Flux::subscribeOn);
    }

    static <T> Function<Flux<T>, Flux<T>> publishFluxOn(Scheduler scheduler) {
        return scheduleFluxOn(scheduler, Flux::publishOn);
    }

    static <T> Function<Flux<T>, Flux<T>> scheduleFluxOn(Scheduler scheduler, BiFunction<Flux<T>, Scheduler, Flux<T>> scheduleFunction) {
        return flux -> scheduler != null ? scheduleFunction.apply(flux, scheduler) : flux;
    }

    static <T> Function<Mono<T>, Mono<T>> subscribeMonoOn(Scheduler scheduler) {
        return scheduleMonoOn(scheduler, Mono::subscribeOn);
    }

    static <T> Function<Mono<T>, Mono<T>> publishMonoOn(Scheduler scheduler) {
        return scheduleMonoOn(scheduler, Mono::publishOn);
    }

    static <T> Function<Mono<T>, Mono<T>> scheduleMonoOn(Scheduler scheduler, BiFunction<Mono<T>, Scheduler, Mono<T>> scheduleFunction) {
        return mono -> scheduler != null ? scheduleFunction.apply(mono, scheduler) : mono;
    }

    static Scheduler defaultScheduler() {
        return scheduler(Schedulers::parallel);
    }

    static  Scheduler scheduler(Supplier<Scheduler> schedulerSupplier) {
        return isVirtualThreadSupported() ? boundedElastic() : schedulerSupplier.get();
    }
}
