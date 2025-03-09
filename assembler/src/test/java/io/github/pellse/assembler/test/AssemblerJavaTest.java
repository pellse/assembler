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

package io.github.pellse.assembler.test;

import io.github.pellse.assembler.Assembler;
import io.github.pellse.assembler.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.QueryUtils.toPublisher;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.RuleMapperSource.from;
import static io.github.pellse.assembler.test.AssemblerTestUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.transform;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.scheduler.Schedulers.immediate;
import static reactor.core.scheduler.Schedulers.parallel;

public class AssemblerJavaTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Flux<BillingInfo> getBillingInfo(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Flux<OrderItem> getAllOrders(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
    }

    private Flux<OrderItem> getAllOrdersWithErrorOn2ndOrderItemOf1stCustomer(List<Customer> customers) {
        return getAllOrders(customers)
                .flatMap(orderItem -> !orderItem.equals(orderItem12) ? Flux.just(orderItem) : Flux.error(new Exception()));
    }

    private List<BillingInfo> getBillingInfoNonReactive(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        var list = Stream.of(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .toList();

        billingInvocationCount.incrementAndGet();
        return list;
    }

    private List<OrderItem> getAllOrdersNonReactive(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        var list = Stream.of(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .toList();

        ordersInvocationCount.incrementAndGet();
        return list;
    }

    private Flux<Customer> getCustomers() {
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3);
    }

    private List<Customer> getCustomersNonReactive() {
        return List.of(customer1, customer2, customer3, customer1, customer2, customer3);
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
                                .withCorrelationIdResolver(Customer::customerId)
                                .withRules(
                                        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                                        Transaction::new)
                                .build(parallel())
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
                                .withCorrelationIdResolver(Customer::customerId)
                                .withRules(
                                        rule(BillingInfo::customerId, oneToOne(AssemblerTestUtils::errorBillingInfos, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(OrderItem::id, AssemblerTestUtils::errorOrderItems)),
                                        Transaction::new)
                                .build(immediate())
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
                                        .withCorrelationIdResolver(Customer::customerId)
                                        .withRules(
                                                rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                                rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                                                Transaction::new)
                                        .build(parallel())
                                        .assemble(customers))
                )
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithBuffering() {

        Assembler<Customer, Transaction> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(from(this::getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
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
    public void testReusableAssemblerBuilderWithErrorOn2ndOrderItemOf1stCustomer() {

        Transaction transaction1 = new Transaction(customer1, billingInfo1, List.of(orderItem11, orderItem13));

        Assembler<Customer, Transaction> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(from(this::getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrdersWithErrorOn2ndOrderItemOf1stCustomer)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(customers -> assembler.assemble(customers).onErrorContinue((error, o) -> {
                        })))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithEmptyReplies() {

        Transaction transaction1 = new Transaction(customer1, null, emptyList());
        Transaction transaction2 = new Transaction(customer2, null, emptyList());
        Transaction transaction3 = new Transaction(customer3, null, emptyList());

        Assembler<Customer, Transaction> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne()),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithLists() {

        Assembler<Customer, Transaction> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(toPublisher(this::getBillingInfoNonReactive), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, toPublisher(this::getAllOrdersNonReactive))),
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
}
