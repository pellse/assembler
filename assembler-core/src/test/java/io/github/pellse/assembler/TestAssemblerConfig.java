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

package io.github.pellse.assembler;

import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class TestAssemblerConfig<T, ID, R> extends CoreAssemblerConfig<T, ID, List<T>, List<ID>, R, Stream<R>> {

    private TestAssemblerConfig(CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
                               Function<T, ID> idExtractor) {

        super(topLevelEntitiesProvider, idExtractor, ArrayList::new, UncheckedException::new,
                TestAssemblerConfig::convertMapperSources);
    }

    public static <T, ID, R> TestAssemblerConfig<T, ID, R> from(CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
                                                                Function<T, ID> idExtractor) {
        return new TestAssemblerConfig<>(topLevelEntitiesProvider, idExtractor);
    }

    static <ID, R> Stream<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> sources,
                                                          Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder,
                                                          Function<Throwable, RuntimeException> errorConverter) {
        try {
            return domainObjectStreamBuilder.apply(sources
                    .map(Supplier::get)
                    .collect(toList()));
        } catch (Throwable t) {
            throw errorConverter.apply(t);
        }
    }
}
