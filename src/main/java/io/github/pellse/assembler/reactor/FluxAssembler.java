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

package io.github.pellse.assembler.reactor;

import io.github.pellse.assembler.Assembler;
import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import io.github.pellse.util.query.Mapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class FluxAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>>
        implements Assembler<T, ID, IDC, Flux<? extends List<?>>> {

    private final Supplier<C> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Supplier<IDC> idCollectionFactory;
    private final Function<Throwable, RuntimeException> errorConverter;

    private FluxAssembler(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                          Function<T, ID> idExtractor,
                          Supplier<IDC> idCollectionFactory,
                          Function<Throwable, RuntimeException> errorConverter) {

        this.topLevelEntitiesProvider = topLevelEntitiesProvider;
        this.idExtractor = idExtractor;
        this.idCollectionFactory = idCollectionFactory;
        this.errorConverter = errorConverter;
    }

    @SuppressWarnings("unchecked")
    public <E1, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper,
            BiFunction<T, E1, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(t, (E1) s[0]), mapper);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Function3<T, E1, E2, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(t, (E1) s[0], (E2) s[1]), mapper1, mapper2);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Function4<T, E1, E2, E3, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2]),
                mapper1, mapper2, mapper3);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Function5<T, E1, E2, E3, E4, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3]),
                mapper1, mapper2, mapper3, mapper4);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4]),
                mapper1, mapper2, mapper3, mapper4, mapper5);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Function7<T, E1, E2, E3, E4, E5, E6, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Function8<T, E1, E2, E3, E4, E5, E6, E7, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Mapper<ID, E8, IDC, Throwable> mapper8,
            Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Mapper<ID, E8, IDC, Throwable> mapper8,
            Mapper<ID, E9, IDC, Throwable> mapper9,
            Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> Flux<List<R>> assemble(
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
            Function11<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10);
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> Flux<List<R>> assemble(
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
            Function12<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> domainObjectBuilderFunction) {

        return assemble((t, s) -> domainObjectBuilderFunction.apply(
                t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9], (E11) s[10]),
                mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10, mapper11);
    }

    @SuppressWarnings("unchecked")
    public <R> Flux<List<R>> assemble(BiFunction<T, ? super Object[], R> domainObjectBuilderFunction,
                                      Mapper<ID, ?, IDC, Throwable>... mappers) {

        return privateAssemble((topLevelEntities, entityIDs) -> {

            Iterable<? extends Publisher<?>> sources = Stream.of(mappers)
                    .map(mapper -> Mono.fromSupplier(unchecked(() -> mapper.map(entityIDs))))
                    .collect(toList());

            return Flux.zip(sources,
                    maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    Stream.of(maps)
                                            .map(mapperResult -> ((Map<ID, ?>) mapperResult).get(id))
                                            .toArray()))
                            .collect(toList()));
        });
    }

    private <R> Flux<R> privateAssemble(BiFunction<C, IDC, Flux<R>> domainObjectBuilder) {

        C topLevelEntities = topLevelEntitiesProvider.get();
        IDC entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toCollection(idCollectionFactory));

        return domainObjectBuilder.apply(topLevelEntities, entityIDs)
                .doOnError(e -> sneakyThrow(errorConverter.apply(e)));
    }

    private <R> Stream<R> buildDomainObjectStream(C topLevelEntities, BiFunction<T, ID, R> domainObjectBuilderFunction) {

        return topLevelEntities.stream()
                .map(e -> domainObjectBuilderFunction.apply(e, idExtractor.apply(e)));
    }

    // Static factory methods

    public static <T, ID> FluxAssembler<T, ID, List<T>, List<ID>> entityAssembler(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor) {

        return entityAssembler(() -> topLevelEntities, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID> FluxAssembler<T, ID, List<T>, List<ID>> entityAssembler(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {

        return entityAssembler(topLevelEntitiesProvider, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>> FluxAssembler<T, ID, C, IDC>
    entityAssembler(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter) {

        return new FluxAssembler<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter);
    }
}
