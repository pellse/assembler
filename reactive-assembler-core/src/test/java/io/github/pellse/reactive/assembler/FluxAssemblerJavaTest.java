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

import io.github.pellse.assembler.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.FluxAdapter.fluxAdapter;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.QueryCache.cache;
import static io.github.pellse.reactive.assembler.QueryCache.cached;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.scheduler.Schedulers.immediate;

public class FluxAssemblerJavaTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Publisher<BillingInfo> getBillingInfo(List<Long> customerIds) {
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
                                        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(this::getAllOrders)),
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
                                        rule(BillingInfo::customerId, oneToOne(ReactiveAssemblerTestUtils::errorBillingInfos, BillingInfo::new)),
                                        rule(OrderItem::customerId, oneToMany(ReactiveAssemblerTestUtils::errorOrderItems)),
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
                                                rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                                                rule(OrderItem::customerId, oneToMany(this::getAllOrders)),
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
                        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(this::getAllOrders)),
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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)), billingInfoCache::computeIfAbsent),
                        cached(rule(OrderItem::customerId, oneToMany(this::getAllOrders)), cache(HashMap::new)),
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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new)), billingInfoCache::computeIfAbsent),
                        cached(rule(OrderItem::customerId, oneToManyAsSet(this::getAllOrders)), cache(HashMap::new)),
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
