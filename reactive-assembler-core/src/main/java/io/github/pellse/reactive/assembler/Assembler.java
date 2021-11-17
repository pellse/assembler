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

package io.github.pellse.reactive.assembler;

import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;

/**
 * @param <T>  Type for Top Level Entity e.g. {@code Customer}
 * @param <RC> Output Type e.g. {@code Stream<Transaction>} or {@code Flux<Transaction>}
 */
public interface Assembler<T, RC> {
    @NotNull
    RC assemble(Publisher<@NotNull T> topLevelEntities);
}
