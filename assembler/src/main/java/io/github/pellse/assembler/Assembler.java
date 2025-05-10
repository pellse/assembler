/*
 * Copyright 2024 Sebastien Pelletier
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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.publisher.Flux.fromStream;

@FunctionalInterface
public interface Assembler<T, R> {

    Flux<Flux<R>> assemblerWindow(Publisher<T> topLevelEntities);

    default Flux<R> assemble(Iterable<T> topLevelEntities) {
        return assemble(fromIterable(topLevelEntities));
    }

    default Flux<R> assemble(Stream<T> topLevelEntities) {
        return assemble(fromStream(topLevelEntities));
    }

    default Flux<R> assemble(Publisher<T> topLevelEntities) {
        return assemblerWindow(topLevelEntities)
                .flatMapSequential(identity());
    }

    default <U> Assembler<T, U> pipeWith(Assembler<R, U> after) {
        return entities -> assemblerWindow(entities)
                .flatMapSequential(after::assemblerWindow);
    }

    static <T, R, V> Function<List<T>, Publisher<V>> assemble(Function<List<T>, Publisher<R>> queryFunction, Assembler<R, V> assembler) {
        return entities -> assembler.assemble(queryFunction.apply(entities));
    }
}
