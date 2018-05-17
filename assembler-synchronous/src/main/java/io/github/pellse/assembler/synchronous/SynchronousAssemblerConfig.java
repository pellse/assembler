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

import io.github.pellse.assembler.CoreAssemblerConfig;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class SynchronousAssemblerConfig<T, ID, C extends Collection<T>, IDC extends Collection<ID>, R>
        extends CoreAssemblerConfig<T, ID, C, IDC, R, Stream<R>> {

    private SynchronousAssemblerConfig(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                                       Function<T, ID> idExtractor,
                                       Supplier<IDC> idCollectionFactory,
                                       Function<Throwable, RuntimeException> errorConverter) {

        super(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter,
                SynchronousAssemblerConfig::convertMapperSources);
    }

    public static <T, ID, R> SynchronousAssemblerConfig<T, ID, List<T>, List<ID>, R> from(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor) {
        return from(() -> topLevelEntities, idExtractor);
    }

    public static <T, ID, R> SynchronousAssemblerConfig<T, ID, List<T>, List<ID>, R> from(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {
        return from(topLevelEntitiesProvider, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>, R> SynchronousAssemblerConfig<T, ID, C, IDC, R> from(
            C topLevelEntities,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter) {
        return new SynchronousAssemblerConfig<>(() -> topLevelEntities, idExtractor, idCollectionFactory, errorConverter);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>, R> SynchronousAssemblerConfig<T, ID, C, IDC, R> from(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter) {
        return new SynchronousAssemblerConfig<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter);
    }

    private static <ID, R> Stream<R> convertMapperSources(Stream<Supplier<Map<ID, ?>>> sources,
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
