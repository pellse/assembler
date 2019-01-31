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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.function.checked.Unchecked.unchecked;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * @param <T>  Type for Top Level Entity e.g. {@code Customer}
 * @param <RC> Output Type e.g. {@code Stream<Transaction>} or {@code Flux<Transaction>}
 */
public interface Assembler<T, RC> {

    default <C extends Iterable<T>> RC assembleFromSupplier(CheckedSupplier<C, Throwable> topLevelEntitiesProvider) {
        return assemble(topLevelEntitiesProvider.get());
    }

    <C extends Iterable<T>> RC assemble(C topLevelEntities);

    /**
     * @param topLevelEntities    e.g. {@code List<Customer>}
     * @param idExtractor         e.g. {@code Customer::getCustomerId}
     * @param subQueryMappers     e.g. {@code [ Mapper<Long, BillingInfo>, Mapper<Long, List<OrderItem>> ]}
     * @param aggregationFunction e.g. {@code buildTransaction(customer, [ billingInfo, orderItemList ])}
     * @param assemblerAdapter    Pluggable execution engine for invoking top and sub queries (e.g. Project Reactor, RxJava)
     * @param errorConverter      Converts any exception thrown into a user defined {@link RuntimeException}
     * @param <T>                 e.g. {@code <Customer>}
     * @param <ID>                e.g. {@code <Long>}
     * @param <C>                 e.g. {@code List<Customer>}
     * @param <R>                 e.g. {@code <Transaction>}
     * @param <RC>                e.g. {@code Stream<Transaction>} or {@code Flux<Transaction>}
     * @return A list of aggregated objects e.g. {@code Stream<Transaction>} or {@code Flux<Transaction>}
     * as specified by the assemblerAdapter return type
     */
    static <T, ID, C extends Iterable<T>, R, RC>
    RC assemble(C topLevelEntities,
                Function<T, ID> idExtractor,
                List<Mapper<ID, ?, ?>> subQueryMappers,
                BiFunction<T, Object[], R> aggregationFunction,
                AssemblerAdapter<ID, R, RC> assemblerAdapter,
                Function<Throwable, RuntimeException> errorConverter) {

        // We extract the IDs from the collection of top level entities e.g. from List<Customer> to List<Long>
        List<ID> entityIDs = stream(topLevelEntities.spliterator(), false)
                .map(idExtractor)
                .collect(toList());

        // Conversion from Mapper to java.util.function.Supplier<java.util.Map>,
        // the list of IDs (e.g. list of Customer ids) now captured in closure
        // so no need for the assemblerAdapter to know anything about IDs
        // when it will internally call "Map<ID, ?> mapperResult = mapperSource.get()"
        // and transform Supplier of Maps (lazy) to List of Maps (materialized) by executing
        // the sub queries represented by subQueryMappers (oneToOne, oneToMany, etc.).
        //
        // To summarize, we transform 1 argument functions into 0 argument functions
        Stream<Supplier<Map<ID, ?>>> mapperSourceSuppliers = subQueryMappers.stream()
                .map(mapper -> unchecked(() -> mapper.apply(entityIDs, ids -> new HashMap<>(ids.size())), errorConverter));

        // We create a function that takes 2 arguments:
        // 1- a topLevelEntity from our main query
        //    e.g. Customer, with customerID of type Long
        // 2- a list of Maps returned from our sub queries
        //    e.g. [ Map<Long, BillingInfo>, Map<Long, List<OrderItem>> ]
        //
        // and return e.g. an instance of Transaction
        BiFunction<T, List<Map<ID, ?>>, R> joinMapperResultsFunction =
                (topLevelEntity, listOfMapperResults) -> aggregationFunction.apply(topLevelEntity,
                        listOfMapperResults.stream()
                                .map(mapperResult -> mapperResult.get(idExtractor.apply(topLevelEntity)))
                                .toArray());

        // We create another function that takes a list of Map returned from our sub queries
        // e.g. [ Map<Long, BillingInfo>, Map<Long, List<OrderItem>> ]
        // and return a stream of aggregated objects e.g. Stream<Transaction>,
        // the function iterate over the list of topLevelEntities e.g. List<Customer>
        // for each topLevelEntity apply the joinMapperResultsFunction defined above
        Function<List<Map<ID, ?>>, Stream<R>> aggregateStreamBuilder =
                mapperResults -> stream(topLevelEntities.spliterator(), false)
                        .map(topLevelEntity -> joinMapperResultsFunction.apply(topLevelEntity, mapperResults));


        // Notice the signature of mapperSourceSuppliers above, it is a supplier of Map<ID, ?>
        // aggregateStreamBuilder takes a list of Map<ID, ?>, so we are injecting the join algorithm
        // into our adapter and the data to pass to the join algorithm
        return assemblerAdapter.convertMapperSources(mapperSourceSuppliers, aggregateStreamBuilder);
    }
}
