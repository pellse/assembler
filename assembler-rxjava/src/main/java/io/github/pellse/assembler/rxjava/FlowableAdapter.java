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
import io.reactivex.Flowable;
import io.reactivex.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.reactivex.Flowable.fromIterable;
import static io.reactivex.schedulers.Schedulers.computation;
import static io.reactivex.schedulers.Schedulers.from;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class FlowableAdapter<ID, R> implements AssemblerAdapter<ID, R, Flowable<R>> {

    private final Scheduler scheduler;

    private FlowableAdapter(Scheduler scheduler) {
        this.scheduler = requireNonNull(scheduler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flowable<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> sources,
                                            Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder) {

        List<Flowable<? extends Map<ID, ?>>> flowables = sources
                .map(mappingSupplier -> Flowable.fromCallable(mappingSupplier::get).subscribeOn(scheduler))
                .collect(toList());

        return Flowable.zip(flowables,
                mapperResults -> domainObjectStreamBuilder.apply(Stream.of(mapperResults)
                        .map(mapResult -> (Map<ID, ?>) mapResult)
                        .collect(toList())))
                .flatMap(stream -> fromIterable(stream::iterator));
    }

    public static <ID, R> FlowableAdapter<ID, R> flowableAdapter() {
        return flowableAdapter(computation());
    }

    public static <ID, R> FlowableAdapter<ID, R> flowableAdapter(Executor executor) {
        return flowableAdapter(from(executor));
    }

    public static <ID, R> FlowableAdapter<ID, R> flowableAdapter(Scheduler scheduler) {
        return new FlowableAdapter<>(scheduler);
    }
}
