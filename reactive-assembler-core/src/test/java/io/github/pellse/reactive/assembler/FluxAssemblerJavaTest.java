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

import io.github.pellse.assembler.BillingInfo;
import io.github.pellse.assembler.Customer;
import io.github.pellse.assembler.OrderItem;
import io.github.pellse.assembler.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.FluxAdapter.fluxAdapter;
import static io.github.pellse.reactive.assembler.KeyValueStorePublisher.asKeyValueStore;
import static io.github.pellse.reactive.assembler.Mapper.*;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.scheduler.Schedulers.immediate;

public class FluxAssemblerJavaTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Publisher<BillingInfo> getBillingInfos(List<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrders(List<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
    }

    private Flux<Customer> getCustomers() {
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3);
    }

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);
    }

    @Test
    public void testAssemblerBuilderWithFlux() {

        StepVerifier.create(
                        assemblerOf(Transaction.class)
                                .withIdExtractor(Customer::customerId)
                                .withAssemblerRules(
                                        oneToOne(ReactiveAssemblerTestUtils::getBillingInfos, BillingInfo::customerId, BillingInfo::new),
                                        oneToMany(ReactiveAssemblerTestUtils::getAllOrders, OrderItem::customerId),
                                        Transaction::new)
                                .build(fluxAdapter())
                                .assemble(getCustomers())
                )
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();
    }

    @Test
    public void testAssemblerBuilderWithFluxWithError() {

        StepVerifier.create(
                        assemblerOf(Transaction.class)
                                .withIdExtractor(Customer::customerId)
                                .withAssemblerRules(
                                        oneToOne(ReactiveAssemblerTestUtils::throwSQLException, BillingInfo::customerId, BillingInfo::new),
                                        oneToMany(ReactiveAssemblerTestUtils::throwSQLException, OrderItem::customerId),
                                        Transaction::new)
                                .build(fluxAdapter(immediate()))
                                .assemble(getCustomers())
                )
                .expectSubscription()
                .expectError(SQLException.class)
                .verify();
    }

    @Test
    public void testAssemblerBuilderWithFluxWithBuffering() {

        StepVerifier.create(
                        getCustomers()
                                .window(3)
                                .flatMapSequential(customers -> assemblerOf(Transaction.class)
                                        .withIdExtractor(Customer::customerId)
                                        .withAssemblerRules(
                                                oneToOne(ReactiveAssemblerTestUtils::getBillingInfos, BillingInfo::customerId, BillingInfo::new),
                                                oneToMany(ReactiveAssemblerTestUtils::getAllOrders, OrderItem::customerId),
                                                Transaction::new)
                                        .build(fluxAdapter())
                                        .assemble(customers))
                )
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithBuffering() {

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(this::getBillingInfos, BillingInfo::customerId, BillingInfo::new),
                        oneToMany(this::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithBuffering2() {

        var billingInfoPublisher = asKeyValueStore(getBillingInfos(of(1L, 2L, 3L)), BillingInfo::customerId);

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(billingInfoPublisher, BillingInfo::customerId, BillingInfo::new),
                        oneToMany(this::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .build(fluxAdapter());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(oneToOne(this::getBillingInfos, BillingInfo::customerId, BillingInfo::new)),
                        cached(oneToMany(this::getAllOrders, OrderItem::customerId)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }
}
