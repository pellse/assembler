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

import io.github.pellse.util.function.*;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
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

    static <R> FromSourceBuilder<R> assemblerOf(Class<R> outputClass) {
        return new FromSourceBuilderImpl<>();
    }

    @FunctionalInterface
    interface FromSourceBuilder<R> {

        default <T, ID, C extends Collection<T>>
        WithAssemblerRulesBuilder<T, ID, R> fromSourceSupplier(CheckedSupplier<C, Throwable> topLevelEntitiesProvider,
                                                               Function<T, ID> idExtractor) {
            return fromSource(topLevelEntitiesProvider.get(), idExtractor);
        }

        <T, ID, C extends Collection<T>>
        WithAssemblerRulesBuilder<T, ID, R> fromSource(C topLevelEntities, Function<T, ID> idExtractor);
    }

    @FunctionalInterface
    interface WithAssemblerRulesBuilder<T, ID, R> {

        @SuppressWarnings("unchecked")
        default <E1> AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper,
                BiFunction<T, E1, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper), (t, s) -> domainObjectBuilder.apply(t, (E1) s[0]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Function3<T, E1, E2, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2), (t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Function4<T, E1, E2, E3, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3),
                    (t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Function5<T, E1, E2, E3, E4, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4),
                    (t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Function6<T, E1, E2, E3, E4, E5, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5),
                    (t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Function7<T, E1, E2, E3, E4, E5, E6, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6),
                    (t, s) -> domainObjectBuilder.apply(t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Mapper<ID, E7, Throwable> mapper7,
                Function8<T, E1, E2, E3, E4, E5, E6, E7, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7),
                    (t, s) -> domainObjectBuilder.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Mapper<ID, E7, Throwable> mapper7,
                Mapper<ID, E8, Throwable> mapper8,
                Function9<T, E1, E2, E3, E4, E5, E6, E7, E8, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8),
                    (t, s) -> domainObjectBuilder.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Mapper<ID, E7, Throwable> mapper7,
                Mapper<ID, E8, Throwable> mapper8,
                Mapper<ID, E9, Throwable> mapper9,
                Function10<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9),
                    (t, s) -> domainObjectBuilder.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Mapper<ID, E7, Throwable> mapper7,
                Mapper<ID, E8, Throwable> mapper8,
                Mapper<ID, E9, Throwable> mapper9,
                Mapper<ID, E10, Throwable> mapper10,
                Function11<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10),
                    (t, s) -> domainObjectBuilder.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9]));
        }

        @SuppressWarnings("unchecked")
        default <E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11>
        AssembleUsingBuilder<ID, R> withAssemblerRules(
                Mapper<ID, E1, Throwable> mapper1,
                Mapper<ID, E2, Throwable> mapper2,
                Mapper<ID, E3, Throwable> mapper3,
                Mapper<ID, E4, Throwable> mapper4,
                Mapper<ID, E5, Throwable> mapper5,
                Mapper<ID, E6, Throwable> mapper6,
                Mapper<ID, E7, Throwable> mapper7,
                Mapper<ID, E8, Throwable> mapper8,
                Mapper<ID, E9, Throwable> mapper9,
                Mapper<ID, E10, Throwable> mapper10,
                Mapper<ID, E11, Throwable> mapper11,
                Function12<T, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, R> domainObjectBuilder) {

            return withAssemblerRules(List.of(mapper1, mapper2, mapper3, mapper4, mapper5, mapper6, mapper7, mapper8, mapper9, mapper10, mapper11),
                    (t, s) -> domainObjectBuilder.apply(
                            t, (E1) s[0], (E2) s[1], (E3) s[2], (E4) s[3], (E5) s[4], (E6) s[5], (E7) s[6], (E8) s[7], (E9) s[8], (E10) s[9], (E11) s[10]));
        }

        AssembleUsingBuilder<ID, R> withAssemblerRules(List<Mapper<ID, ?, Throwable>> mappers,
                                                       BiFunction<T, ? super Object[], R> domainObjectBuilder);
    }

    interface AssembleUsingBuilder<ID, R> {
        AssembleUsingBuilder<ID, R> withErrorConverter(Function<Throwable, RuntimeException> errorConverter);

        <RC> RC assembleUsing(AssemblerAdapter<ID, R, RC> adapter);
    }

    class FromSourceBuilderImpl<R> implements FromSourceBuilder<R> {

        private FromSourceBuilderImpl() {
        }

        @Override
        public <T, ID, C extends Collection<T>> WithAssemblerRulesBuilder<T, ID, R> fromSource(C topLevelEntities,
                                                                                               Function<T, ID> idExtractor) {
            return new WithAssemblerRulesBuilderImpl<>(topLevelEntities, idExtractor);
        }
    }

    class WithAssemblerRulesBuilderImpl<T, ID, C extends Collection<T>, R> implements WithAssemblerRulesBuilder<T, ID, R> {

        private final C topLevelEntities;
        private final Function<T, ID> idExtractor;

        private WithAssemblerRulesBuilderImpl(C topLevelEntities,
                                              Function<T, ID> idExtractor) {

            this.topLevelEntities = topLevelEntities;
            this.idExtractor = idExtractor;
        }

        @Override
        public AssembleUsingBuilder<ID, R> withAssemblerRules(List<Mapper<ID, ?, Throwable>> mappers,
                                                              BiFunction<T, ? super Object[], R> domainObjectBuilder) {
            return new AssembleUsingBuilderImpl<>(topLevelEntities, idExtractor, mappers, domainObjectBuilder);
        }
    }

    class AssembleUsingBuilderImpl<T, ID, C extends Collection<T>, R> implements AssembleUsingBuilder<ID, R> {

        private final C topLevelEntities;
        private final Function<T, ID> idExtractor;
        private BiFunction<T, ? super Object[], R> domainObjectBuilder;
        private List<Mapper<ID, ?, Throwable>> mappers;

        private Function<Throwable, RuntimeException> errorConverter = UncheckedException::new;

        private AssembleUsingBuilderImpl(C topLevelEntities,
                                         Function<T, ID> idExtractor,
                                         List<Mapper<ID, ?, Throwable>> mappers,
                                         BiFunction<T, ? super Object[], R> domainObjectBuilder) {

            this.topLevelEntities = topLevelEntities;
            this.idExtractor = idExtractor;

            this.domainObjectBuilder = domainObjectBuilder;
            this.mappers = mappers;
        }

        @Override
        public AssembleUsingBuilder<ID, R> withErrorConverter(Function<Throwable, RuntimeException> errorConverter) {
            this.errorConverter = errorConverter;
            return this;
        }

        @Override
        public <RC> RC assembleUsing(AssemblerAdapter<ID, R, RC> assemblerAdapter) {

            return assemble(topLevelEntities, idExtractor,
                    mappers, domainObjectBuilder, assemblerAdapter, errorConverter);
        }
    }

    static <T, ID, C extends Collection<T>, R, RC>
    RC assemble(C topLevelEntities,
                Function<T, ID> idExtractor,
                List<Mapper<ID, ?, Throwable>> mappers,
                BiFunction<T, ? super Object[], R> domainObjectBuilder,
                AssemblerAdapter<ID, R, RC> assemblerAdapter,
                Function<Throwable, RuntimeException> errorConverter) {

        List<ID> entityIDs = topLevelEntities.stream()
                .map(idExtractor)
                .collect(toList());

        Stream<Supplier<Map<ID, ?>>> sources = mappers.stream()
                .map(mapper -> unchecked(() -> mapper.map(entityIDs), errorConverter));

        Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder = mapperResults -> topLevelEntities.stream()
                .map(topLevelEntity -> domainObjectBuilder.apply(topLevelEntity,
                        mapperResults.stream()
                                .map(mapperResult -> mapperResult.get(idExtractor.apply(topLevelEntity)))
                                .toArray()));

        return assemblerAdapter.convertMapperSources(sources, domainObjectStreamBuilder);
    }
}
