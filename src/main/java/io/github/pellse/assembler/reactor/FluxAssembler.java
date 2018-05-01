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

import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import io.github.pellse.util.query.Mapper;
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

public class FluxAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {

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
            Mapper<ID, E1, IDC, Throwable> mapper1,
            BiFunction<T, E1, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));

            return Flux.zip(
                    List.of(m1), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id)))
                            .collect(toList())
            );
        });
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Function3<T, E1, E2, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id)))
                            .collect(toList())
            );
        });
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Function4<T, E1, E2, E3, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id)))
                            .collect(toList())
            );
        });
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Function5<T, E1, E2, E3, E4, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id)))
                            .collect(toList())
            );
        });
    }

    @SuppressWarnings("unchecked")
    public <E1, E2, E3, E4, E5, R> Flux<List<R>> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));
            Mono<Map<ID, E7>> m7 = Mono.fromSupplier(unchecked(() -> mapper7.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6, m7), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id),
                                    ((Map<ID, E7>) maps[6]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));
            Mono<Map<ID, E7>> m7 = Mono.fromSupplier(unchecked(() -> mapper7.map(entityIDs)));
            Mono<Map<ID, E8>> m8 = Mono.fromSupplier(unchecked(() -> mapper8.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6, m7, m8), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id),
                                    ((Map<ID, E7>) maps[6]).get(id),
                                    ((Map<ID, E8>) maps[7]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));
            Mono<Map<ID, E7>> m7 = Mono.fromSupplier(unchecked(() -> mapper7.map(entityIDs)));
            Mono<Map<ID, E8>> m8 = Mono.fromSupplier(unchecked(() -> mapper8.map(entityIDs)));
            Mono<Map<ID, E9>> m9 = Mono.fromSupplier(unchecked(() -> mapper9.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6, m7, m8, m9), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id),
                                    ((Map<ID, E7>) maps[6]).get(id),
                                    ((Map<ID, E8>) maps[7]).get(id),
                                    ((Map<ID, E9>) maps[8]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));
            Mono<Map<ID, E7>> m7 = Mono.fromSupplier(unchecked(() -> mapper7.map(entityIDs)));
            Mono<Map<ID, E8>> m8 = Mono.fromSupplier(unchecked(() -> mapper8.map(entityIDs)));
            Mono<Map<ID, E9>> m9 = Mono.fromSupplier(unchecked(() -> mapper9.map(entityIDs)));
            Mono<Map<ID, E10>> m10 = Mono.fromSupplier(unchecked(() -> mapper10.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id),
                                    ((Map<ID, E7>) maps[6]).get(id),
                                    ((Map<ID, E8>) maps[7]).get(id),
                                    ((Map<ID, E9>) maps[8]).get(id),
                                    ((Map<ID, E10>) maps[9]).get(id)))
                            .collect(toList())
            );
        });
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

        return assemble((topLevelEntities, entityIDs) -> {

            Mono<Map<ID, E1>> m1 = Mono.fromSupplier(unchecked(() -> mapper1.map(entityIDs)));
            Mono<Map<ID, E2>> m2 = Mono.fromSupplier(unchecked(() -> mapper2.map(entityIDs)));
            Mono<Map<ID, E3>> m3 = Mono.fromSupplier(unchecked(() -> mapper3.map(entityIDs)));
            Mono<Map<ID, E4>> m4 = Mono.fromSupplier(unchecked(() -> mapper4.map(entityIDs)));
            Mono<Map<ID, E5>> m5 = Mono.fromSupplier(unchecked(() -> mapper5.map(entityIDs)));
            Mono<Map<ID, E6>> m6 = Mono.fromSupplier(unchecked(() -> mapper6.map(entityIDs)));
            Mono<Map<ID, E7>> m7 = Mono.fromSupplier(unchecked(() -> mapper7.map(entityIDs)));
            Mono<Map<ID, E8>> m8 = Mono.fromSupplier(unchecked(() -> mapper8.map(entityIDs)));
            Mono<Map<ID, E9>> m9 = Mono.fromSupplier(unchecked(() -> mapper9.map(entityIDs)));
            Mono<Map<ID, E10>> m10 = Mono.fromSupplier(unchecked(() -> mapper10.map(entityIDs)));
            Mono<Map<ID, E11>> m11 = Mono.fromSupplier(unchecked(() -> mapper11.map(entityIDs)));

            return Flux.zip(
                    List.of(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id),
                                    ((Map<ID, E3>) maps[2]).get(id),
                                    ((Map<ID, E4>) maps[3]).get(id),
                                    ((Map<ID, E5>) maps[4]).get(id),
                                    ((Map<ID, E6>) maps[5]).get(id),
                                    ((Map<ID, E7>) maps[6]).get(id),
                                    ((Map<ID, E8>) maps[7]).get(id),
                                    ((Map<ID, E9>) maps[8]).get(id),
                                    ((Map<ID, E10>) maps[9]).get(id),
                                    ((Map<ID, E11>) maps[10]).get(id)))
                            .collect(toList())
            );
        });
    }

    private <R> Flux<R> assemble(BiFunction<C, IDC, Flux<R>> domainObjectBuilder) {

        C topLevelEntities = topLevelEntitiesProvider.get();
        IDC entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toCollection(idCollectionFactory));

        return domainObjectBuilder.apply(topLevelEntities, entityIDs)
                .doOnError(e -> sneakyThrow(errorConverter.apply(e)));
    }

    private <R> Stream<R> buildDomainObjectStream(C topLevelEntities, BiFunction<T, ID, R> function) {

        return topLevelEntities.stream()
                .map(e -> function.apply(e, idExtractor.apply(e)));
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
