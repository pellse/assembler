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
import io.github.pellse.assembler.Rule;
import io.github.pellse.assembler.caching.factory.CacheContext.OneToOneCacheContext;
import io.github.pellse.assembler.caching.factory.CacheFactory;
import io.github.pellse.assembler.caching.factory.CacheFactory.CacheTransformer;
import io.github.pellse.assembler.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.LifeCycleEventBroadcaster.lifeCycleEventBroadcaster;
import static io.github.pellse.assembler.QueryUtils.toPublisher;
import static io.github.pellse.assembler.Rule.RuleResolver.with;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.*;
import static io.github.pellse.assembler.RuleMapperSource.call;
import static io.github.pellse.assembler.RuleMapperSource.resolve;
import static io.github.pellse.assembler.caching.DefaultCache.cache;
import static io.github.pellse.assembler.caching.factory.StreamTableFactoryBuilder.streamTableBuilder;
import static io.github.pellse.assembler.caching.factory.CacheFactory.*;
import static io.github.pellse.assembler.caching.factory.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.assembler.test.CDCAdd.cdcAdd;
import static io.github.pellse.assembler.test.CDCDelete.cdcDelete;
import static io.github.pellse.assembler.test.AssemblerTestUtils.*;
import static io.github.pellse.util.ObjectUtils.run;
import static io.github.pellse.util.collection.CollectionUtils.transform;
import static io.github.pellse.util.reactive.ReactiveUtils.scheduler;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Thread.currentThread;
import static java.time.Duration.*;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.publisher.Mono.error;
import static reactor.core.scheduler.Schedulers.*;

sealed interface CDC<T> {
    T item();
}

record MyOtherEvent<T>(T value, boolean isAddEvent) {
}

record CDCAdd<T>(T item) implements CDC<T> {
    static <T> CDC<T> cdcAdd(T item) {
        return new CDCAdd<>(item);
    }
}

record CDCDelete<T>(T item) implements CDC<T> {
    static <T> CDC<T> cdcDelete(T item) {
        return new CDCDelete<>(item);
    }
}

public class CacheTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Publisher<BillingInfo> getBillingInfo(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrders(List<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
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
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3, customer1, customer2, customer3);
    }

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);
    }

    @Test
    @Timeout(60)
    public void testLongRunningAutoCachingEvents() throws InterruptedException {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        final AtomicInteger billingInvocationCount = new AtomicInteger();
        final AtomicInteger ordersInvocationCount = new AtomicInteger();

        var customerList = List.of(customer1, customer2, customer3);

        var billingInfoList = List.of(cdcAdd(billingInfo1), cdcAdd(billingInfo2), cdcAdd(updatedBillingInfo2), cdcAdd(billingInfo3));

        var orderItemList = List.of(cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(updatedOrderItem22),
                cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                cdcDelete(orderItem31), cdcDelete(orderItem32), cdcAdd(updatedOrderItem11));

        var customerScheduler = scheduler(() -> newBoundedElastic(4, MAX_VALUE, "Customer-Scheduler"));
        var billingInfoScheduler = scheduler(() -> newBoundedElastic(4, MAX_VALUE, "BillingInfo-Write-Scheduler"));
        var orderItemScheduler = scheduler(() -> newBoundedElastic(4, MAX_VALUE, "OrderItem-Write-Scheduler"));

        var customerFlux = fromIterable(customerList).repeat();
        var billingInfoFlux = fromIterable(billingInfoList).repeat().delayElements(ofMillis(1), billingInfoScheduler);
        var orderItemFlux = fromIterable(orderItemList).repeat().delayElements(ofMillis(1), orderItemScheduler);

        Function<String, Consumer<Object>> simulateIO = tag -> __ -> {
            parkNanos(ofMillis(600).toNanos()); // Simulate blocking I/O
            System.out.println(currentThread().getName() + ": " + tag);
        };

        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            return Flux.just(billingInfo1, billingInfo2, billingInfo3)
                    .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                    .doOnSubscribe(simulateIO.apply("BillingInfo"))
                    .doOnComplete(billingInvocationCount::incrementAndGet);
        };

        Function<List<Customer>, Publisher<OrderItem>> getOrderItems = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnSubscribe(simulateIO.apply("OrderItem"))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo,
                                streamTableBuilder(billingInfoFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .concurrent()
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(getOrderItems,
                                streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .concurrent()
                                        .build()))),
                        Transaction::new)
                .build(1_000);

        var transactionFlux = customerFlux
                .delayElements(ofMillis(1), customerScheduler)
                .window(3)
                .flatMapSequential(assembler::assemble)
                .take(1_000);

        var initialCount = 500;
        var latch = new CountDownLatch(initialCount);

        for (int i = 0; i < initialCount; i++) {
            transactionFlux.doFinally(signalType -> latch.countDown())
                    .subscribe();
        }
        latch.await();

        System.out.println("getBillingInfo invocation count: " + billingInvocationCount.get() + ", getOrderItems invocation count: " + ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithCaching() {

        Function<List<Long>, Publisher<BillingInfo>> getBillingInfo = customerIds ->
                Flux.just(billingInfo1, billingInfo3)
                        .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                        .doOnComplete(billingInvocationCount::incrementAndGet);

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(call(getBillingInfo)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get(), "BillingInfo error");
        assertEquals(1, ordersInvocationCount.get(), "OrderItem error");
    }

    @Test
    public void testReusableAssemblerBuilderWithConcurrentCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, concurrent()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, cache(), concurrent()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithFaultyCache() {

        CacheFactory<Long, BillingInfo, BillingInfo, OneToOneCacheContext<Long, BillingInfo>> faultyCache = cache(
                ids -> error(new RuntimeException("Cache.getAll failed")),
                (ids, fetchFunction) -> error(new RuntimeException("Cache.computeAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(3, billingInvocationCount.get(), "BillingInfo error");
        assertEquals(1, ordersInvocationCount.get(), "OrderItem error");
    }

    @Test
    public void testReusableAssemblerBuilderWithFaultyQueryFunction() {

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.just(billingInfo1)
                .flatMap(__ -> Flux.error(new IOException()));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMap(customers ->
                                assembler.assemble(customers)
                                        .onErrorResume(IOException.class, __ -> Flux.just(defaultTransaction))))
                .expectSubscription()
                .expectNext(defaultTransaction, defaultTransaction, defaultTransaction)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithFaultyCacheAndQueryFunction() {

        CacheFactory<Long, BillingInfo, BillingInfo, OneToOneCacheContext<Long, BillingInfo>> faultyCache = cache(
                ids -> error(new RuntimeException("Cache.getAll failed")),
                (ids, fetchFunction) -> error(new RuntimeException("Cache.computeAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.error(new IOException());

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(customers ->
                                assembler.assemble(customers)
                                        .onErrorResume(IOException.class, __ -> Flux.just(defaultTransaction))))
                .expectSubscription()
                .expectNext(defaultTransaction, defaultTransaction, defaultTransaction)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithCachingNonReactive() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(toPublisher(this::getBillingInfoNonReactive)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(toPublisher(this::getAllOrdersNonReactive), cache()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithCustomCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, cache()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, cache()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(2)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCaching() {

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3)
                .subscribeOn(parallel());

        Flux<OrderItem> orderItemFlux = Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33)
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        CacheTransformer<Long, BillingInfo, BillingInfo, OneToOneCacheContext<Long, BillingInfo>> cff1 = cf -> cf;
        CacheTransformer<Long, BillingInfo, BillingInfo, OneToOneCacheContext<Long, BillingInfo>> cff2 = cf -> cf;

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(streamTableBuilder(billingInfoFlux).maxWindowSize(10).build(), cff1, cff2))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(cache(), streamTableBuilder(orderItemFlux).maxWindowSize(10).build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCaching2() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3);

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(streamTableBuilder(billingInfoFlux).maxWindowSize(4).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(3, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCaching3() {

        Flux<BillingInfo> dataSource1 = Flux.just(billingInfo1, billingInfo2, billingInfo3);
        Flux<OrderItem> dataSource2 = Flux.just(
                orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, streamTableBuilder(dataSource1).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, streamTableBuilder(dataSource2).maxWindowSize(3).build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(5)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingLifeCycleEventListener() {

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3)
                .subscribeOn(parallel());

        Flux<OrderItem> orderItemFlux = Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33)
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        var lifeCycleEventBroadcaster = lifeCycleEventBroadcaster();

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo,
                                streamTableBuilder(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, cache(),
                                streamTableBuilder(orderItemFlux)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble)
                        .doOnSubscribe(run(lifeCycleEventBroadcaster::start))
                        .doFinally(run(lifeCycleEventBroadcaster::stop)))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingError() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, null, "4540111111111111"); // null customerId, will trigger NullPointerException

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2Unknown, updatedBillingInfo2, billingInfo3);

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(
                                streamTableBuilder(billingInfoFlux)
                                        .errorHandler(error -> assertInstanceOf(NullPointerException.class, error))
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(3, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingEvents() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 1.00);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        Flux<BillingInfo> billingInfoEventFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3)
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                        cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(updatedOrderItem22),
                        cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                        cdcDelete(orderItem31), cdcDelete(orderItem32), cdcAdd(updatedOrderItem11))
                .subscribeOn(parallel());

        Transaction transaction1 = new Transaction(customer1, billingInfo1, List.of(updatedOrderItem11, orderItem12, orderItem13));
        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, updatedOrderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem33));

        var billingInfoStreamTable = streamTableBuilder(billingInfoEventFlux)
                .maxWindowSize(3)
                .build(Long.class);

        var orderItemStreamTable = streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                .maxWindowSize(3)
                .build(Long.class, String.class);

        var billingInfoRule = with(Customer.class)
                .resolve(rule(BillingInfo::customerId, oneToOne(cached(billingInfoStreamTable))));

        var orderItemRule = with(Customer.class)
                .resolve(rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(cache(), orderItemStreamTable))));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(billingInfoRule, orderItemRule, Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingMultipleEventSources() {

        var billingInfoFlux = Flux.just( // E.g. Flux coming from a CDC/Kafka source
                new MyOtherEvent<>(billingInfo1, true), new MyOtherEvent<>(billingInfo2, true),
                new MyOtherEvent<>(billingInfo2, false), new MyOtherEvent<>(billingInfo3, true));

        var orderItemFlux = Flux.just(
                new CDCAdd<>(orderItem11), new CDCAdd<>(orderItem12), new CDCAdd<>(orderItem13),
                new CDCAdd<>(orderItem21), new CDCAdd<>(orderItem22),
                new CDCAdd<>(orderItem31), new CDCAdd<>(orderItem32), new CDCAdd<>(orderItem33),
                new CDCDelete<>(orderItem31), new CDCDelete<>(orderItem32), new CDCDelete<>(orderItem33));

        var billingInfoStreamTable = streamTableBuilder(billingInfoFlux, MyOtherEvent::isAddEvent, MyOtherEvent::value)
                .build(Long.class);

        var orderItemStreamTable = streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                .build(Long.class, String.class);

        var cachedBillingInfos = resolve(cached(this::getBillingInfo, billingInfoStreamTable), Long.class);
        var cachedOrderItems = resolve(cachedMany(this::getAllOrders, orderItemStreamTable), Long.class);

        var billingInfoRule = with(Customer.class)
                .resolve(rule(BillingInfo::customerId, oneToOne(cachedBillingInfos, BillingInfo::new)));

        var orderItemRule = Rule.resolve(rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedOrderItems)), Customer.class);

        Assembler<Customer, Transaction> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(billingInfoRule, orderItemRule, Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingEvents2() {

        Function<List<Customer>, Publisher<OrderItem>> getAllOrders = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            assertEquals(List.of(3L), customerIds);
            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3)
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                        cdcAdd(orderItem21), cdcAdd(orderItem22),
                        cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                        cdcDelete(orderItem31), cdcDelete(orderItem32), cdcDelete(orderItem33))
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(
                                streamTableBuilder(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(getAllOrders,
                                streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingEventsOnSameThread() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoEventFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3, updatedBillingInfo2);

        var orderItemFlux = Flux.just(
                cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(orderItem31),
                cdcAdd(orderItem32), cdcAdd(orderItem33), cdcDelete(orderItem31),
                cdcDelete(orderItem32));

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo,
                                streamTableBuilder(billingInfoEventFlux)
                                        .maxWindowSize(3)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders,
                                streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .build()))),
                        Transaction::new)
                .build(immediate());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }
}