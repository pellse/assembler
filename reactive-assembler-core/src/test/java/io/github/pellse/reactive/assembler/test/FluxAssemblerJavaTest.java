/*
 * Copyright 2023 Sebastien Pelletier
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

package io.github.pellse.reactive.assembler.test;

import io.github.pellse.assembler.*;
import io.github.pellse.reactive.assembler.Assembler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.FluxAdapter.fluxAdapter;
import static io.github.pellse.reactive.assembler.QueryCache.cache;
import static io.github.pellse.reactive.assembler.QueryCache.cached;
import static io.github.pellse.reactive.assembler.QueryUtils.toPublisher;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.reactive.assembler.RuleMapperSource.emptyQuery;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.scheduler.Schedulers.immediate;

public class FluxAssemblerJavaTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Flux<BillingInfo> getBillingInfo(List<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Flux<OrderItem> getAllOrders(List<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
    }

    private Flux<OrderItem> getAllOrdersWithErrorOn2ndOrderItemOf1stCustomer(List<Long> customerIds) {
        return getAllOrders(customerIds)
                .flatMap(orderItem -> !orderItem.equals(orderItem12) ? Flux.just(orderItem) : Flux.error(new Exception()));
    }

    private List<BillingInfo> getBillingInfoNonReactive(List<Long> customerIds) {
        var list = Stream.of(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .toList();

        billingInvocationCount.incrementAndGet();
        return list;
    }

    private List<OrderItem> getAllOrdersNonReactive(List<Long> customerIds) {
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
                                .withCorrelationIdExtractor(Customer::customerId)
                                .withAssemblerRules(
                                        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
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
                                .withCorrelationIdExtractor(Customer::customerId)
                                .withAssemblerRules(
                                        rule(BillingInfo::customerId, oneToOne(ReactiveAssemblerTestUtils::errorBillingInfos, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(OrderItem::id, ReactiveAssemblerTestUtils::errorOrderItems)),
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
                                        .withCorrelationIdExtractor(Customer::customerId)
                                        .withAssemblerRules(
                                                rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                                rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
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
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(call(this::getBillingInfo), BillingInfo::new)),
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

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(call(this::getBillingInfo), BillingInfo::new)),
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

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(emptyQuery())),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, emptyQuery())),
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

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
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

    @Test
    public void testReusableAssemblerBuilderWithCaching() {

        var billingInfoCache = new HashMap<Iterable<Long>, Mono<Map<Long, BillingInfo>>>();

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)), billingInfoCache::computeIfAbsent),
                        cached(rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)), cache(HashMap::new)),
                        Transaction::new)
                .build(Schedulers.immediate());

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

    @Test
    public void testReusableAssemblerBuilderWithCachingSet() {

        var billingInfoCache = new HashMap<Iterable<Long>, Mono<Map<Long, BillingInfo>>>();

        var assembler = assemblerOf(TransactionSet.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)), billingInfoCache::computeIfAbsent),
                        cached(rule(OrderItem::customerId, oneToManyAsSet(OrderItem::id, this::getAllOrders)), cache(HashMap::new)),
                        TransactionSet::new)
                .build(Schedulers.immediate());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }
}
