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
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.reactivex.rxjava3.core.Observable.fromCallable;
import static io.reactivex.rxjava3.core.Observable.fromIterable;
import static io.reactivex.rxjava3.schedulers.Schedulers.computation;
import static io.reactivex.rxjava3.schedulers.Schedulers.from;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ObservableAdapter<ID, R> implements AssemblerAdapter<ID, R, Observable<R>> {

    private final Scheduler scheduler;

    private ObservableAdapter(Scheduler scheduler) {
        this.scheduler = requireNonNull(scheduler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> mapperSourceSuppliers,
                                        Function<List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        List<Observable<? extends Map<ID, ?>>> observables = mapperSourceSuppliers
                .map(this::toObservable)
                .collect(toList());

        return Observable.zip(observables,
                mapperResults -> aggregateStreamBuilder.apply(Stream.of(mapperResults)
                        .map(mapResult -> (Map<ID, ?>) mapResult)
                        .collect(toList())))
                .flatMap(stream -> fromIterable(stream::iterator));
    }

    private Observable<? extends Map<ID, ?>> toObservable(Supplier<Map<ID, ?>> mapperSource) {
        return fromCallable(mapperSource::get).subscribeOn(scheduler);
    }

    public static <ID, R> ObservableAdapter<ID, R> observableAdapter() {
        return observableAdapter(computation());
    }

    public static <ID, R> ObservableAdapter<ID, R> observableAdapter(Executor executor) {
        return observableAdapter(from(executor));
    }

    public static <ID, R> ObservableAdapter<ID, R> observableAdapter(Scheduler scheduler) {
        return new ObservableAdapter<>(scheduler);
    }
}
