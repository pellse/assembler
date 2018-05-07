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

package io.github.pellse.assembler.flux;

import io.github.pellse.assembler.AssemblerAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.stream.Collectors.toList;

class FluxAssemblerAdapter<ID> implements AssemblerAdapter<ID, Mono<Map<ID, ?>>, Flux<?>> {

    @Override
    public Mono<Map<ID, ?>> convertMapperSupplier(Supplier<Map<ID, ?>> mapperSupplier) {
        return Mono.fromSupplier(mapperSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> Flux<R> convertMapperSources(List<Mono<Map<ID, ?>>> sources,
                                            Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder,
                                            Function<Throwable, RuntimeException> errorConverter) {

        return Flux.zip(sources, mapperResults -> domainObjectStreamBuilder.apply(cast(mapperResults)))
                .flatMap(Flux::fromStream)
                .doOnError(e -> sneakyThrow(errorConverter.apply(e)));
    }

    private List<Map<ID, ?>> cast(Object[] mapperResults) {
        return Stream.of(mapperResults)
                .map(this::cast)
                .collect(toList());
    }

    @SuppressWarnings("unchecked")
    private Map<ID, ?> cast(Object mapResult) {
        return (Map<ID, ?>) mapResult;
    }
}
