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

import io.github.pellse.assembler.Assembler;
import io.github.pellse.assembler.core.CoreAssembler;
import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import io.github.pellse.util.query.Mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SynchronousAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>>
        implements Assembler<T, ID, IDC, Stream<?>> {

    private final CoreAssembler<T, ID, C, IDC, Supplier<Map<ID, ?>>, Stream<?>> coreAssembler;

    private SynchronousAssembler(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                                Function<T, ID> idExtractor,
                                Supplier<IDC> idCollectionFactory,
                                Function<Throwable, RuntimeException> errorConverter) {

        coreAssembler = CoreAssembler.of(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter,
                new SynchronousAssemblerAdapter<>());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper,
                                      BiFunction<T, E1, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                          Mapper<ID, E2, IDC, Throwable> mapper2,
                                          Function3<T, E1, E2, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                              Mapper<ID, E2, IDC, Throwable> mapper2,
                                              Mapper<ID, E3, IDC, Throwable> mapper3,
                                              Function4<T, E1, E2, E3, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                  Mapper<ID, E2, IDC, Throwable> mapper2,
                                                  Mapper<ID, E3, IDC, Throwable> mapper3,
                                                  Mapper<ID, E4, IDC, Throwable> mapper4,
                                                  Function5<T, E1, E2, E3, E4, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                      Mapper<ID, E2, IDC, Throwable> mapper2,
                                                      Mapper<ID, E3, IDC, Throwable> mapper3,
                                                      Mapper<ID, E4, IDC, Throwable> mapper4,
                                                      Mapper<ID, E5, IDC, Throwable> mapper5,
                                                      Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                          Mapper<ID, E2, IDC, Throwable> mapper2,
                                                          Mapper<ID, E3, IDC, Throwable> mapper3,
                                                          Mapper<ID, E4, IDC, Throwable> mapper4,
                                                          Mapper<ID, E5, IDC, Throwable> mapper5,
                                                          Mapper<ID, E6, IDC, Throwable> mapper6,
                                                          Function7<T, E1, E2, E3, E4, E5, E6, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                              Mapper<ID, E2, IDC, Throwable> mapper2,
                                                              Mapper<ID, E3, IDC, Throwable> mapper3,
                                                              Mapper<ID, E4, IDC, Throwable> mapper4,
                                                              Mapper<ID, E5, IDC, Throwable> mapper5,
                                                              Mapper<ID, E6, IDC, Throwable> mapper6,
                                                              Mapper<ID, E7, IDC, Throwable> mapper7,
                                                              Function8<T, E1, E2, E3, E4, E5, E6, E7, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                                  Mapper<ID, E2, IDC, Throwable> mapper2,
                                                                  Mapper<ID, E3, IDC, Throwable> mapper3,
                                                                  Mapper<ID, E4, IDC, Throwable> mapper4,
                                                                  Mapper<ID, E5, IDC, Throwable> mapper5,
                                                                  Mapper<ID, E6, IDC, Throwable> mapper6,
                                                                  Mapper<ID, E7, IDC, Throwable> mapper7,
                                                                  Mapper<ID, E8, IDC, Throwable> mapper8,
                                                                  Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                                      Mapper<ID, E2, IDC, Throwable> mapper2,
                                                                      Mapper<ID, E3, IDC, Throwable> mapper3,
                                                                      Mapper<ID, E4, IDC, Throwable> mapper4,
                                                                      Mapper<ID, E5, IDC, Throwable> mapper5,
                                                                      Mapper<ID, E6, IDC, Throwable> mapper6,
                                                                      Mapper<ID, E7, IDC, Throwable> mapper7,
                                                                      Mapper<ID, E8, IDC, Throwable> mapper8,
                                                                      Mapper<ID, E9, IDC, Throwable> mapper9,
                                                                      Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                                           Mapper<ID, E2, IDC, Throwable> mapper2,
                                                                           Mapper<ID, E3, IDC, Throwable> mapper3,
                                                                           Mapper<ID, E4, IDC, Throwable> mapper4,
                                                                           Mapper<ID, E5, IDC, Throwable> mapper5,
                                                                           Mapper<ID, E6, IDC, Throwable> mapper6,
                                                                           Mapper<ID, E7, IDC, Throwable> mapper7,
                                                                           Mapper<ID, E8, IDC, Throwable> mapper8,
                                                                           Mapper<ID, E9, IDC, Throwable> mapper9,
                                                                           Mapper<ID, E10, IDC, Throwable> mapper10,
                                                                           Function11<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> Stream<R> assemble(Mapper<ID, E1, IDC, Throwable> mapper1,
                                                                                Mapper<ID, E2, IDC, Throwable> mapper2,
                                                                                Mapper<ID, E3, IDC, Throwable> mapper3,
                                                                                Mapper<ID, E4, IDC, Throwable> mapper4,
                                                                                Mapper<ID, E5, IDC, Throwable> mapper5,
                                                                                Mapper<ID, E6, IDC, Throwable> mapper6,
                                                                                Mapper<ID, E7, IDC, Throwable> mapper7,
                                                                                Mapper<ID, E8, IDC, Throwable> mapper8,
                                                                                Mapper<ID, E9, IDC, Throwable> mapper9,
                                                                                Mapper<ID, E10, IDC, Throwable> mapper10,
                                                                                Mapper<ID, E11, IDC, Throwable> mapper11,
                                                                                Function12<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> domainObjectBuilder) {
        return (Stream<R>) coreAssembler.assemble(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10, mapper11, domainObjectBuilder);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Stream<R> assemble(BiFunction<T, ? super Object[], R> domainObjectBuilder, List<Mapper<ID, ?, IDC, Throwable>> mappers) {
        return (Stream<R>) coreAssembler.assemble(domainObjectBuilder, mappers);
    }

    // Static factory methods

    public static <T, ID> SynchronousAssembler<T, ID, List<T>, List<ID>> of(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor) {

        return of(() -> topLevelEntities, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID> SynchronousAssembler<T, ID, List<T>, List<ID>> of(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {

        return of(topLevelEntitiesProvider, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>> SynchronousAssembler<T, ID, C, IDC> of(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter) {

        return new SynchronousAssembler<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter);
    }
}
