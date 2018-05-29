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

public interface Assembler {

    static <T, ID, C extends Collection<T>, R, RC>
    RC assemble(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                Function<T, ID> idExtractor,
                List<Mapper<ID, ?, Throwable>> mappers,
                BiFunction<T, ? super Object[], R> domainObjectBuilder,
                AssemblerAdapter<ID, R, RC> assemblerAdapter,
                Function<Throwable, RuntimeException> errorConverter) {

        C topLevelEntities = topLevelEntitiesProvider.get();

        List<ID> entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toList());

        Stream<Supplier<Map<ID, ?>>> sources = mappers.stream()
                .map(mapper -> unchecked(() -> mapper.map(entityIDs)));

        Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder = mapperResults -> topLevelEntities.stream()
                .map(e -> domainObjectBuilder.apply(e,
                        mapperResults.stream()
                                .map(mapperResult -> mapperResult.get(idExtractor.apply(e)))
                                .toArray()));

        return assemblerAdapter.convertMapperSources(sources, domainObjectStreamBuilder, errorConverter);
    }
}
