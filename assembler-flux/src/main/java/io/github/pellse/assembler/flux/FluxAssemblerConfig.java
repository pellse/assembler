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

import io.github.pellse.assembler.CoreAssemblerConfig;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static io.github.pellse.assembler.flux.FluxAssemblerAdapter.fluxAssemblerAdapter;

public final class FluxAssemblerConfig<T, ID, C extends Collection<T>, R>
        extends CoreAssemblerConfig<T, ID, C, R, Flux<R>> {

    private FluxAssemblerConfig(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                                Function<T, ID> idExtractor,
                                Function<Throwable, RuntimeException> errorConverter,
                                Scheduler scheduler) {

        super(topLevelEntitiesProvider, idExtractor, errorConverter, fluxAssemblerAdapter(scheduler));
    }

    public static <T, ID, R> FluxAssemblerConfig<T, ID, List<T>, R> from(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor) {
        return from(() -> topLevelEntities, idExtractor);
    }

    public static <T, ID, R> FluxAssemblerConfig<T, ID, List<T>, R> from(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor) {
        return from(topLevelEntitiesProvider, idExtractor, Schedulers.parallel());
    }

    public static <T, ID, R> FluxAssemblerConfig<T, ID, List<T>, R> from(
            List<T> topLevelEntities,
            Function<T, ID> idExtractor,
            Scheduler scheduler) {
        return from(() -> topLevelEntities, idExtractor, scheduler);
    }

    public static <T, ID, R> FluxAssemblerConfig<T, ID, List<T>, R> from(
            CheckedSupplier<List<T>, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Scheduler scheduler) {
        return from(topLevelEntitiesProvider, idExtractor, UncheckedException::new, scheduler);
    }

    public static <T, ID, C extends Collection<T>, R> FluxAssemblerConfig<T, ID, C, R> from(
            C topLevelEntities,
            Function<T, ID> idExtractor,
            Function<Throwable, RuntimeException> errorConverter) {
        return from(() -> topLevelEntities, idExtractor, errorConverter, Schedulers.parallel());
    }

    public static <T, ID, C extends Collection<T>, R> FluxAssemblerConfig<T, ID, C, R> from(
            CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
            Function<T, ID> idExtractor,
            Function<Throwable, RuntimeException> errorConverter,
            Scheduler scheduler) {
        return new FluxAssemblerConfig<>(topLevelEntitiesProvider, idExtractor, errorConverter, scheduler);
    }
}
