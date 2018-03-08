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

import io.github.pellse.assembler.Assembler;
import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedFunction2;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import io.github.pellse.util.query.Mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.empty;

public class SynchronousAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>>
        implements Assembler<T, ID, IDC, Stream<?>> {

    private final CheckedSupplier<C, Throwable> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Supplier<IDC> idCollectionFactory;
    private final Consumer<Throwable> errorHandler;

    private SynchronousAssembler(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                                 Function<T, ID> idExtractor,
                                 Supplier<IDC> idCollectionFactory,
                                 Consumer<Throwable> errorHandler) {

        this.topLevelEntitiesProvider = topLevelEntitiesProvider;
        this.idExtractor = idExtractor;
        this.idCollectionFactory = idCollectionFactory;
        this.errorHandler = errorHandler;
    }

    public <E1, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            BiFunction<T, E1, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities, (t, id) -> domainObjectBuilderFunction.apply(t,
                    mapE1.get(id)));
        });
    }

    public <E1, E2, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Function3<T, E1, E2, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id)));
        });
    }

    public <E1, E2, E3, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Function4<T, E1, E2, E3, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id)));
        });
    }

    public <E1, E2, E3, E4, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Function5<T, E1, E2, E3, E4, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Function7<T, E1, E2, E3, E4, E5, E6, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, E7, R> Stream<R> assemble(
            Mapper<ID, E1, IDC, Throwable> mapper1,
            Mapper<ID, E2, IDC, Throwable> mapper2,
            Mapper<ID, E3, IDC, Throwable> mapper3,
            Mapper<ID, E4, IDC, Throwable> mapper4,
            Mapper<ID, E5, IDC, Throwable> mapper5,
            Mapper<ID, E6, IDC, Throwable> mapper6,
            Mapper<ID, E7, IDC, Throwable> mapper7,
            Function8<T, E1, E2, E3, E4, E5, E6, E7, R> domainObjectBuilderFunction) {

        return assemble((topLevelEntities, entityIDs) -> {
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);
            Map<ID, E7> mapE7 = mapper7.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id), mapE7.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, E7, E8, R> Stream<R> assemble(
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
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);
            Map<ID, E7> mapE7 = mapper7.map(entityIDs);
            Map<ID, E8> mapE8 = mapper8.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id), mapE7.get(id), mapE8.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, R> Stream<R> assemble(
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
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);
            Map<ID, E7> mapE7 = mapper7.map(entityIDs);
            Map<ID, E8> mapE8 = mapper8.map(entityIDs);
            Map<ID, E9> mapE9 = mapper9.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id), mapE7.get(id), mapE8.get(id), mapE9.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> Stream<R> assemble(
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
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);
            Map<ID, E7> mapE7 = mapper7.map(entityIDs);
            Map<ID, E8> mapE8 = mapper8.map(entityIDs);
            Map<ID, E9> mapE9 = mapper9.map(entityIDs);
            Map<ID, E10> mapE10 = mapper10.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id), mapE7.get(id), mapE8.get(id), mapE9.get(id),
                            mapE10.get(id)));
        });
    }

    public <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> Stream<R> assemble(
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
            Map<ID, E1> mapE1 = mapper1.map(entityIDs);
            Map<ID, E2> mapE2 = mapper2.map(entityIDs);
            Map<ID, E3> mapE3 = mapper3.map(entityIDs);
            Map<ID, E4> mapE4 = mapper4.map(entityIDs);
            Map<ID, E5> mapE5 = mapper5.map(entityIDs);
            Map<ID, E6> mapE6 = mapper6.map(entityIDs);
            Map<ID, E7> mapE7 = mapper7.map(entityIDs);
            Map<ID, E8> mapE8 = mapper8.map(entityIDs);
            Map<ID, E9> mapE9 = mapper9.map(entityIDs);
            Map<ID, E10> mapE10 = mapper10.map(entityIDs);
            Map<ID, E11> mapE11 = mapper11.map(entityIDs);

            return buildDomainObjectStream(topLevelEntities,
                    (t, id) -> domainObjectBuilderFunction.apply(t, mapE1.get(id), mapE2.get(id), mapE3.get(id),
                            mapE4.get(id), mapE5.get(id), mapE6.get(id), mapE7.get(id), mapE8.get(id), mapE9.get(id),
                            mapE10.get(id), mapE11.get(id)));
        });
    }

    private <R> Stream<R> assemble(CheckedFunction2<C, IDC, Stream<R>, Throwable> domainObjectBuilder) {

        try {
            C topLevelEntities = topLevelEntitiesProvider.checkedGet();
            IDC entityIDs = topLevelEntities.stream()
                    .map(idExtractor)
                    .collect(toCollection(idCollectionFactory));

            return unchecked(domainObjectBuilder).apply(topLevelEntities, entityIDs);
        } catch (Throwable e) {
            errorHandler.accept(e);
            return empty();
        }
    }

    private <R> Stream<R> buildDomainObjectStream(C topLevelEntities, BiFunction<T, ID, R> function) {

        return topLevelEntities.stream()
                .map(e -> function.apply(e, idExtractor.apply(e)));
    }

    // Static factory methods

    public static <T, ID>
    SynchronousAssembler<T, ID, List<T>, List<ID>> entityAssembler(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {

        return entityAssembler(topLevelEntitiesProvider, idExtractor, ArrayList::new,
                SynchronousAssembler::throwUncheckedException);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>>
    SynchronousAssembler<T, ID, C, IDC> entityAssembler(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory) {

        return entityAssembler(topLevelEntitiesProvider, idExtractor, idCollectionFactory,
                SynchronousAssembler::throwUncheckedException);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>>
    SynchronousAssembler<T, ID, C, IDC> entityAssembler(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Consumer<Throwable> errorHandler) {

        return new SynchronousAssembler<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory,
                errorHandler);
    }

    private static <R> R throwUncheckedException(Throwable t) {
        return sneakyThrow(new UncheckedException(t));
    }
}
