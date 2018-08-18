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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class StreamAdapter<ID, R> implements AssemblerAdapter<ID, R, Stream<R>> {

    private final boolean parallel;

    private StreamAdapter(boolean parallel) {
        this.parallel = parallel;
    }

    @Override
    public Stream<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> mapperSources,
                                          Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder) {
        return domainObjectStreamBuilder.apply(convertSources(mapperSources)
                .map(Supplier::get)
                .collect(toList()));
    }

    private Stream<Supplier<Map<ID, ?>>> convertSources(Stream<Supplier<Map<ID, ?>>> sources) {
        return parallel ? sources.collect(toList()).parallelStream() : sources;
    }

    public static <ID, R> StreamAdapter<ID, R> streamAdapter() {
        return streamAdapter(false);
    }

    public static <ID, R> StreamAdapter<ID, R> streamAdapter(boolean parallel) {
        return new StreamAdapter<>(parallel);
    }
}
