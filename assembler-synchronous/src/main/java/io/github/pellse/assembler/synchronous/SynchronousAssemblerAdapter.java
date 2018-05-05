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

package io.github.pellse.assembler.synchronous;

import io.github.pellse.assembler.AssemblerAdapter;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.stream.Collectors.toList;

class SynchronousAssemblerAdapter<ID> implements AssemblerAdapter<ID, Supplier<Map<ID, ?>>, Stream<?>> {

    @Override
    public Supplier<Map<ID, ?>> convertMapperSupplier(Supplier<Map<ID, ?>> mapperSupplier) {
        return mapperSupplier;
    }

    @Override
    public <R> Stream<R> convertMapperSources(List<Supplier<Map<ID, ?>>> sources,
                                              Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder,
                                              Function<Throwable, RuntimeException> errorConverter) {
        try {
            return domainObjectStreamBuilder.apply(sources.stream()
                    .map(Supplier::get)
                    .collect(toList()));
        } catch(Throwable t) {
            return sneakyThrow(errorConverter.apply(t));
        }
    }
}
