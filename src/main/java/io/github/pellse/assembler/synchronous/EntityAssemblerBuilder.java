/*
 * Copyright 2017 Sebastien Pelletier
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

import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.assembler.synchronous.SynchronousEntityAssembler.entityAssembler;
import static io.github.pellse.util.ExceptionUtils.sneakyThrow;

public class EntityAssemblerBuilder<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {

    @FunctionalInterface
    interface TopLevelEntitiesSupplier<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {
        IdExtractorFunction<T, ID, C, IDC> entitiesProvider(CheckedSupplier<C, Throwable> topLevelEntitiesProvider);
    }

    @FunctionalInterface
    interface IdExtractorFunction<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {
        CollectionFactoryFunction<T, ID, C, IDC> entityIdExtractor(Function<T, ID> idExtractor);
    }

    @FunctionalInterface
    interface CollectionFactoryFunction<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {
        EntityAssemblerBuilder<T, ID, C, IDC> idCollectionFactory(Supplier<IDC> idCollectionFactory);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>> TopLevelEntitiesSupplier<T, ID, C, IDC> builder() {
        return entitiesProvider -> entityIdExtractor -> idCollectionFactory -> new EntityAssemblerBuilder<>(entitiesProvider, entityIdExtractor, idCollectionFactory);
    }

    private final CheckedSupplier<C, Throwable> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Supplier<IDC> idCollectionFactory;

    private Consumer<Throwable> errorHandler = e -> sneakyThrow(new UncheckedException(e));

    private EntityAssemblerBuilder(CheckedSupplier<C, Throwable> topLevelEntitiesProvider, Function<T, ID> idExtractor, Supplier<IDC> idCollectionFactory) {
        this.topLevelEntitiesProvider = topLevelEntitiesProvider;
        this.idExtractor = idExtractor;
        this.idCollectionFactory = idCollectionFactory;
    }

    public EntityAssemblerBuilder<T, ID, C, IDC> errorHandler(Consumer<Throwable> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    public SynchronousEntityAssembler<T, ID, C, IDC> build() {
        return entityAssembler(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorHandler);
    }
}