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

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class CoreAssemblerConfig<T, ID, C extends Collection<T>, R, RC>
        implements AssemblerConfig<T, ID, C, R, RC> {

    private final CheckedSupplier<C, Throwable> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Function<Throwable, RuntimeException> errorConverter;
    private final AssemblerAdapter<ID, R, RC> assemblerAdapter;

    public CoreAssemblerConfig(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                               Function<T, ID> idExtractor,
                               Function<Throwable, RuntimeException> errorConverter,
                               AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        this.topLevelEntitiesProvider = requireNonNull(topLevelEntitiesProvider);
        this.idExtractor = requireNonNull(idExtractor);
        this.errorConverter = errorConverter != null ? errorConverter : UncheckedException::new;
        this.assemblerAdapter = requireNonNull(assemblerAdapter);
    }

    public CheckedSupplier<C, Throwable> getTopLevelEntitiesProvider() {
        return topLevelEntitiesProvider;
    }

    public Function<T, ID> getIdExtractor() {
        return idExtractor;
    }

    public Function<Throwable, RuntimeException> getErrorConverter() {
        return errorConverter;
    }

    public AssemblerAdapter<ID, R, RC> getAssemblerAdapter() {
        return assemblerAdapter;
    }

    public static <T, ID, R, RC> CoreAssemblerConfig<T, ID, List<T>, R, RC> from(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return from(topLevelEntities, idExtractor, assemblerAdapter);
    }

    public static <T, ID, C extends Collection<T>, R, RC> CoreAssemblerConfig<T, ID, C, R, RC> from(
            C topLevelEntities,
            Function<T, ID> idExtractor,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return from(topLevelEntities, idExtractor, UncheckedException::new, assemblerAdapter);
    }

    public static <T, ID, C extends Collection<T>, R, RC> CoreAssemblerConfig<T, ID, C, R, RC> from(
            C topLevelEntities,
            Function<T, ID> idExtractor,
            Function<Throwable, RuntimeException> errorConverter,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return new CoreAssemblerConfig<>(() -> topLevelEntities, idExtractor, errorConverter, assemblerAdapter);
    }

    public static <T, ID, R, RC> CoreAssemblerConfig<T, ID, List<T>, R, RC> fromSupplier(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return from(topLevelEntitiesProvider, idExtractor, UncheckedException::new, assemblerAdapter);
    }

    public static <T, ID, C extends Collection<T>, R, RC> CoreAssemblerConfig<T, ID, C, R, RC> from(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return from(topLevelEntitiesProvider, idExtractor, UncheckedException::new, assemblerAdapter);
    }

    public static <T, ID, C extends Collection<T>, R, RC> CoreAssemblerConfig<T, ID, C, R, RC> from(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Function<Throwable, RuntimeException> errorConverter,
            AssemblerAdapter<ID, R, RC> assemblerAdapter) {

        return new CoreAssemblerConfig<>(topLevelEntitiesProvider, idExtractor, errorConverter, assemblerAdapter);
    }
}
