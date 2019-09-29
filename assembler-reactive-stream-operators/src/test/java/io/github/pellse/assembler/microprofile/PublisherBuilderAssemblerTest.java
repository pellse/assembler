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

package io.github.pellse.assembler.microprofile;

import io.github.pellse.assembler.*;
import io.reactivex.rxjava3.core.Flowable;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.microprofile.PublisherBuilderAdapter.publisherAdapter;
import static io.github.pellse.assembler.microprofile.PublisherBuilderAdapter.publisherBuilderAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Sebastien Pelletier
 */
class PublisherBuilderAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3, customer1, customer2);
    }

    @Test
    void testAssemblerBuilderWithPublisherBuilder() throws Exception {

        PublisherBuilder<Transaction> transactionPublisherBuilder = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .using(publisherBuilderAdapter())
                .assembleFromSupplier(this::getCustomers);

        assertThat(transactionPublisherBuilder.collect(toList()).run().toCompletableFuture().get(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test
    public void testAssemblerBuilderWithErrorWithPublisherBuilder() {

        assertThrows(UserDefinedRuntimeException.class, () -> {
            PublisherBuilder<Transaction> transactionPublisherBuilder = assemblerOf(Transaction.class)
                    .withIdExtractor(Customer::getCustomerId)
                    .withAssemblerRules(
                            oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::getCustomerId, BillingInfo::new),
                            oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::getCustomerId),
                            Transaction::new)
                    .withErrorConverter(UserDefinedRuntimeException::new)
                    .using(publisherBuilderAdapter())
                    .assembleFromSupplier(this::getCustomers);

            try {
                transactionPublisherBuilder.collect(toList()).run().toCompletableFuture().get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    void testAssemblerBuilderWithPublisherBuilderWithBufferingFlux() {

        StepVerifier.create(Flux.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(customers -> assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::getCustomerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                                Transaction::new)
                        .using(publisherAdapter())
                        .assemble(customers)))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2)
                .expectComplete()
                .verify();
    }

    @Test
    void testReusableAssemblerBuilderWithPublisherBuilderWithBufferingFlux() {

        Assembler<Customer, PublisherBuilder<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .using(publisherBuilderAdapter());

        StepVerifier.create(Flux.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(customer -> assembler.assemble(customer).buildRs()))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2)
                .expectComplete()
                .verify();
    }

    @Test
    void testAssemblerBuilderWithPublisherBuilderWithBufferingRxJava() {

        final Flowable<Transaction> transactionFlowable = Flowable.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(customers -> assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::getCustomerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                                Transaction::new)
                        .using(publisherBuilderAdapter())
                        .assemble(customers)
                        .buildRs());

        assertThat(transactionFlowable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test
    void testReusableAssemblerWithPublisherBuilderWithBufferingRxJava() {

        Assembler<Customer, Publisher<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .using(publisherAdapter());

        Flowable<Transaction> transactionFlowable = Flowable.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(assembler::assemble);

        assertThat(transactionFlowable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }
}
