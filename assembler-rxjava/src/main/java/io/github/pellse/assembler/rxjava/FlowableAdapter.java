/*
 * Copyright 2018 Sebastien Pelletier
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

package io.github.pellse.assembler.rxjava;

import io.github.pellse.assembler.AssemblerAdapter;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.reactivex.rxjava3.core.Flowable.fromCallable;
import static io.reactivex.rxjava3.core.Flowable.fromIterable;
import static io.reactivex.rxjava3.schedulers.Schedulers.computation;
import static io.reactivex.rxjava3.schedulers.Schedulers.from;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class FlowableAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, Flowable<R>> {

    private final Scheduler scheduler;

    private FlowableAdapter(Scheduler scheduler) {
        this.scheduler = requireNonNull(scheduler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flowable<R> convertMapperSources(CheckedSupplier<Iterable<T>, Throwable> topLevelEntitiesProvider,
                                            Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                            BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        return toFlowable(topLevelEntitiesProvider)
                .flatMap(entities -> Flowable.zip(mapperSourcesBuilder.apply(entities).map(this::toFlowable).collect(toList()),
                        mapperResults -> aggregateStreamBuilder.apply(entities, Stream.of(mapperResults)
                                .map(mapResult -> (Map<ID, ?>) mapResult)
                                .collect(toList())))
                        .flatMap(stream -> fromIterable(stream::iterator)));
    }

    private <U> Flowable<U> toFlowable(Supplier<U> mapperSource) {
        return fromCallable(mapperSource::get).subscribeOn(scheduler);
    }

    public static <T, ID, R> FlowableAdapter<T, ID, R> flowableAdapter() {
        return flowableAdapter(computation());
    }

    public static <T, ID, R> FlowableAdapter<T, ID, R> flowableAdapter(Executor executor) {
        return flowableAdapter(from(executor));
    }

    public static <T, ID, R> FlowableAdapter<T, ID, R> flowableAdapter(Scheduler scheduler) {
        return new FlowableAdapter<>(scheduler);
    }
}
