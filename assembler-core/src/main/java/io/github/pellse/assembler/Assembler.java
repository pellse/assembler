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
import io.github.pellse.util.query.Mapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.stream.Collectors.toList;

public interface Assembler<T, RC> {

    default <C extends Collection<T>> RC assembleFromSupplier(CheckedSupplier<C, Throwable> topLevelEntitiesProvider) {
        return assemble(topLevelEntitiesProvider.get());
    }

    <C extends Collection<T>> RC assemble(C topLevelEntities);

    static <T, ID, C extends Collection<T>, R, RC>
    RC assemble(C topLevelEntities,
                Function<T, ID> idExtractor,
                List<Mapper<ID, ?, ? extends Throwable>> mappers,
                BiFunction<T, ? super Object[], R> assemblerFunction,
                AssemblerAdapter<ID, R, RC> assemblerAdapter,
                Function<Throwable, RuntimeException> errorConverter) {

        List<ID> entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toList());

        Stream<Supplier<Map<ID, ?>>> mapperSources = mappers.stream()
                .map(mapper -> unchecked(() -> mapper.map(entityIDs), errorConverter));

        BiFunction<T, List<Map<ID, ?>>, R> joinMapperResultsFunction =
                (topLevelEntity, listOfMapperResults) -> assemblerFunction.apply(topLevelEntity,
                        listOfMapperResults.stream()
                                .map(mapperResult -> mapperResult.get(idExtractor.apply(topLevelEntity)))
                                .toArray());

        Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder = mapperResults -> topLevelEntities.stream()
                .map(topLevelEntity -> joinMapperResultsFunction.apply(topLevelEntity, mapperResults));

        return assemblerAdapter.convertMapperSources(mapperSources, domainObjectStreamBuilder);
    }
}
