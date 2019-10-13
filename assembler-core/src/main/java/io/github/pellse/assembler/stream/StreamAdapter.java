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

package io.github.pellse.assembler.stream;

import io.github.pellse.assembler.AssemblerAdapter;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class StreamAdapter<T, ID, R> implements AssemblerAdapter<T, ID, R, Stream<R>> {

    private final boolean parallel;

    private StreamAdapter(boolean parallel) {
        this.parallel = parallel;
    }

    @Override
    public Stream<R> convertMapperSources(Supplier<Iterable<T>> topLevelEntitiesProvider,
                                          Function<Iterable<T>, Stream<Supplier<Map<ID, ?>>>> mapperSourcesBuilder,
                                          BiFunction<Iterable<T>, List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder) {

        Iterable<T> entities = topLevelEntitiesProvider.get();
        List<Map<ID, ?>> mappers = convertSources(mapperSourcesBuilder.apply(entities))
                .map(Supplier::get)
                .collect(toList());

        return aggregateStreamBuilder.apply(entities, mappers);
    }

    private <U> Stream<Supplier<U>> convertSources(Stream<Supplier<U>> sources) {
        return parallel ? sources.collect(toList()).parallelStream() : sources;
    }

    public static <T, ID, R> StreamAdapter<T, ID, R> streamAdapter() {
        return streamAdapter(false);
    }

    public static <T, ID, R> StreamAdapter<T, ID, R> streamAdapter(boolean parallel) {
        return new StreamAdapter<>(parallel);
    }
}
