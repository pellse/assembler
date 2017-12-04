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

import io.github.pellse.util.function.Function3;
import io.github.pellse.util.function.checked.CheckedFunction2;
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

public class FluxEntityAssembler<T, ID, C extends Collection<T>, IDC extends Collection<ID>> {

    private final Supplier<C> topLevelEntitiesProvider;
    private final Function<T, ID> idExtractor;
    private final Supplier<IDC> idCollectionFactory;
    private final Function<Throwable, RuntimeException> errorConverter;

    private FluxEntityAssembler(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
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

            return Flux.combineLatest(
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

            return Flux.combineLatest(
                    List.of(m1, m2), maps -> buildDomainObjectStream(topLevelEntities,
                            (t, id) -> domainObjectBuilderFunction.apply(t,
                                    ((Map<ID, E1>) maps[0]).get(id),
                                    ((Map<ID, E2>) maps[1]).get(id)))
                            .collect(toList())
            );
        });
    }

    private <R> Flux<R> assemble(CheckedFunction2<C, IDC, Flux<R>, Throwable> domainObjectBuilder) {

        C topLevelEntities = topLevelEntitiesProvider.get();
        IDC entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toCollection(idCollectionFactory));

        return unchecked(domainObjectBuilder).apply(topLevelEntities, entityIDs)
                .doOnError(e -> sneakyThrow(e, errorConverter));
    }

    private <R> Stream<R> buildDomainObjectStream(C topLevelEntities, BiFunction<T, ID, R> function) {

        return topLevelEntities.stream()
                .map(e -> function.apply(e, idExtractor.apply(e)));
    }

    // Static factory methods

    public static <T, ID> FluxEntityAssembler<T, ID, List<T>, List<ID>> entityAssembler(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor) {

        return entityAssembler(() -> topLevelEntities, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID> FluxEntityAssembler<T, ID, List<T>, List<ID>> entityAssembler(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {

        return entityAssembler(topLevelEntitiesProvider, idExtractor, ArrayList::new, UncheckedException::new);
    }

    public static <T, ID, C extends Collection<T>, IDC extends Collection<ID>> FluxEntityAssembler<T, ID, C, IDC>
    entityAssembler(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            Function<Throwable, RuntimeException> errorConverter) {

        return new FluxEntityAssembler<>(topLevelEntitiesProvider, idExtractor, idCollectionFactory, errorConverter);
    }
}
