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

package io.github.pellse.assembler.core;

import io.github.pellse.assembler.Assembler;
import io.github.pellse.assembler.AssemblerAdapter;
import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import io.github.pellse.util.query.Mapper;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class CoreAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>, M, RC>
        implements Assembler<T, ID, IDC, RC> {

    private final Supplier<C> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Supplier<IDC> idCollectionFactory;
    private final Function<Throwable, RuntimeException> errorConverter;

    private final AssemblerAdapter<ID, M, RC> assemblerAdapter;

    private CoreAssembler(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                          Function<T, ID> idExtractor,
                          Supplier<IDC> idCollectionFactory,
                          Function<Throwable, RuntimeException> errorConverter,
                          AssemblerAdapter<ID, M, RC> assemblerAdapter) {

        this.topLevelEntitiesProvider = requireNonNull(topLevelEntitiesProvider);
        this.idExtractor = requireNonNull(idExtractor);
        this.idCollectionFactory = requireNonNull(idCollectionFactory);
        this.errorConverter = errorConverter != null ? errorConverter : UncheckedException::new;
        this.assemblerAdapter = requireNonNull(assemblerAdapter);
    }

    @SuppressWarnings("unchecked")
    public <E1, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper,
            BiFunction<T, E1, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(t, (E1) s[0]), List.of(mapper));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Function3<T, E1, E2, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1]), List.of(mapper1, mapper2));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Function4<T, E1, E2, E3, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2]),
                List.of(mapper1, mapper2, mapper3));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Function5<T, E1, E2, E3, E4, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3]),
                List.of(mapper1, mapper2, mapper3, mapper4));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Function7<T, E1, E2, E3, E4, E5, E6, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Function8<T, E1, E2, E3, E4, E5, E6, E7, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Mapper<ID, E8, IDC, Throwable> mapper8,
            Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Mapper<ID, E8, IDC, Throwable> mapper8,
            Mapper<ID, E9, IDC, Throwable> mapper9,
            Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> domainObjectBuilder) {

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
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

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10));
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> RC assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
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

        return assemble((t, s) -> domainObjectBuilder.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9], (E11) s[10]),
                List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10, mapper11));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> RC assemble(BiFunction<T, ? super Object[], R> domainObjectBuilder,
                           List<Mapper<ID, ?, IDC, Throwable>> mappers) {

        C topLevelEntities = topLevelEntitiesProvider.get();
        IDC entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toCollection(idCollectionFactory));

        List<M> sources = mappers.stream()
                .map(mapper -> assemblerAdapter.convertMapperSupplier(unchecked(() -> mapper.map(entityIDs))))
                .collect(toList());

        return assemblerAdapter.convertMapperSources(sources,
                mapperResults -> buildDomainObjectStream(topLevelEntities,
                        (t, id) -> domainObjectBuilder.apply(t,
                                mapperResults.stream()
                                        .map(mapperResult -> mapperResult.get(id))
                                        .toArray())),
                errorConverter);
    }

    private <R> Stream<R> buildDomainObjectStream(C topLevelEntities, BiFunction<T, ID, R> domainObjectBuilder) {

        return topLevelEntities.stream()
                .map(e -> domainObjectBuilder.apply(e, idExtractor.apply(e)));
    }

    // Static factory methods

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>, M, RC> CoreAssembler<T, ID, C, IDC, M, RC> of(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter,
            AssemblerAdapter<ID, M, RC> assemblerAdapter) {

        return new CoreAssembler<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter, assemblerAdapter);
    }
}
