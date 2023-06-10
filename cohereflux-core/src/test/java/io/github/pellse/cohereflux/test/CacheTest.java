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

package io.github.pellse.cohereflux.test;

import io.github.pellse.cohereflux.CohereFlux;
import io.github.pellse.cohereflux.Rule;
import io.github.pellse.cohereflux.caching.CacheEvent;
import io.github.pellse.cohereflux.caching.CacheFactory;
import io.github.pellse.cohereflux.caching.CacheFactory.CacheTransformer;
import io.github.pellse.cohereflux.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.pellse.cohereflux.CohereFluxBuilder.cohereFluxOf;
import static io.github.pellse.cohereflux.LifeCycleEventBroadcaster.lifeCycleEventBroadcaster;
import static io.github.pellse.cohereflux.QueryUtils.toPublisher;
import static io.github.pellse.cohereflux.Rule.rule;
import static io.github.pellse.cohereflux.RuleMapper.*;
import static io.github.pellse.cohereflux.RuleMapperSource.call;
import static io.github.pellse.cohereflux.caching.AutoCacheFactory.autoCache;
import static io.github.pellse.cohereflux.caching.AutoCacheFactoryBuilder.autoCacheBuilder;
import static io.github.pellse.cohereflux.caching.AutoCacheFactoryBuilder.autoCacheEvents;
import static io.github.pellse.cohereflux.caching.CacheEvent.*;
import static io.github.pellse.cohereflux.caching.CacheFactory.cache;
import static io.github.pellse.cohereflux.caching.CacheFactory.cached;
import static io.github.pellse.cohereflux.caching.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.cohereflux.test.CDCAdd.cdcAdd;
import static io.github.pellse.cohereflux.test.CDCDelete.cdcDelete;
import static io.github.pellse.cohereflux.test.CohereFluxTestUtils.*;
import static io.github.pellse.util.ObjectUtils.run;
import static io.github.pellse.util.collection.CollectionUtils.transform;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

    private static <T> Flux<T> longRunningFlux(List<T> list) {
        return longRunningFlux(list, -1);
    }

    private static <T> Flux<T> longRunningFlux(List<T> list, int maxItems) {
        return generate(0, i -> (i + 1) % list.size(), list::get, count -> count >= maxItems && maxItems != -1);
    }

    private static <T, S> Flux<T> generate(
            S initialState,
            UnaryOperator<S> stateUpdater,
            Function<S, T> valueGenerator,
            Predicate<Long> stopCondition) {

        return Flux.create(sink -> {

            var state = new AtomicReference<>(initialState);
            var generatedValueCount = new AtomicLong();

            sink.onRequest(n -> {
                for (long i = 0; i < n; i++) {

                    if (stopCondition.test(generatedValueCount.incrementAndGet())) {
                        sink.complete();
                        return;
                    }
                    var value = valueGenerator.apply(state.getAndUpdate(stateUpdater));
                    sink.next(value);
                }
            });
        });
    }

    //    @Test
//    @Timeout(60)
//    public void testLongRunningAutoCachingEvents() throws InterruptedException {
    public static void main(String[] args) throws InterruptedException {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        final AtomicInteger billingInvocationCount = new AtomicInteger();
        final AtomicInteger ordersInvocationCount = new AtomicInteger();

        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            return Flux.just(billingInfo1, billingInfo3)
                    .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                    .doOnComplete(billingInvocationCount::incrementAndGet);
        };

        Function<List<Customer>, Publisher<OrderItem>> getAllOrders = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        var customerList = List.of(customer1, customer2, customer3);

        var billingInfoList = List.of(cdcAdd(billingInfo1), cdcAdd(billingInfo2), cdcAdd(updatedBillingInfo2), cdcAdd(billingInfo3));

        var orderItemList = List.of(cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(updatedOrderItem22),
                cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                cdcDelete(orderItem31), cdcDelete(orderItem32), cdcAdd(updatedOrderItem11));

        var customerFlux = longRunningFlux(customerList).delayElements(ofNanos(1));
        var billingInfoFlux = longRunningFlux(billingInfoList).delayElements(ofNanos(1));
        var orderItemFlux = longRunningFlux(orderItemList).delayElements(ofNanos(1));

        var lifeCycleEventBroadcaster = lifeCycleEventBroadcaster();

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo,
                                autoCacheBuilder(billingInfoFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .scheduler(boundedElastic())
                                        .backoffRetryStrategy(100, ofNanos(1), ofMillis(1))
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders,
                                autoCacheBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .scheduler(boundedElastic())
                                        .backoffRetryStrategy(100, ofNanos(1), ofMillis(1))
                                        .build()))),
                        Transaction::new)
                .build();

        var transactionFlux = customerFlux
                .window(3)
                .flatMapSequential(cohereFlux::process)
                .take(10_000)
                .doOnSubscribe(run(lifeCycleEventBroadcaster::start))
                .doFinally(run(lifeCycleEventBroadcaster::stop));

        var initialCount = 500;
        var latch = new CountDownLatch(initialCount);
        IntStream.range(0, initialCount).forEach(__ -> transactionFlux.subscribe(null, error -> latch.countDown(),
                () -> {
                    latch.countDown();
                    System.out.println("complete, count = " + latch.getCount() + ", Thread = " + Thread.currentThread().getName());
                }));
        latch.await();

        System.out.println("getBillingInfo invocation count: " + billingInvocationCount.get() + ", getOrderItems invocation count: " + ordersInvocationCount.get());
    }

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

    private Publisher<BillingInfo> getBillingInfoWithIdSet(Set<Customer> customers) {

        var customerIds = transform(customers, Customer::customerId);

        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrdersWithIdSet(Set<Customer> customers) {

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
    public void testReusableCohereFluxBuilderWithCaching() {

        Function<List<Long>,  Publisher<BillingInfo>> getBillingInfo = customerIds ->
             Flux.just(billingInfo1, billingInfo3)
                    .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                    .doOnComplete(billingInvocationCount::incrementAndGet);

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(call(getBillingInfo)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get(), "BillingInfo error");
        assertEquals(1, ordersInvocationCount.get(), "OrderItem error");
    }

    @Test
    public void testReusableCohereFluxBuilderWithConcurrentCaching() {

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, concurrent()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), concurrent()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithFaultyCache() {

        CacheFactory<Long, BillingInfo, BillingInfo> faultyCache = cache(
                (ids, fetchFunction) -> error(new RuntimeException("Cache.getAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(3, billingInvocationCount.get(), "BillingInfo error");
        assertEquals(1, ordersInvocationCount.get(), "OrderItem error");
    }

    @Test
    public void testReusableCohereFluxBuilderWithFaultyQueryFunction() {

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.just(billingInfo1)
                .flatMap(__ -> Flux.error(new IOException()));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMap(customers ->
                                cohereFlux.process(customers)
                                        .onErrorResume(IOException.class, __ -> Flux.just(defaultTransaction))))
                .expectSubscription()
                .expectNext(defaultTransaction, defaultTransaction, defaultTransaction)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableCohereFluxBuilderWithFaultyCacheAndQueryFunction() {

        CacheFactory<Long, BillingInfo, BillingInfo> faultyCache = cache(
                (ids, computeIfAbsent) -> error(new RuntimeException("Cache.getAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.error(new IOException());

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(customers ->
                                cohereFlux.process(customers)
                                        .onErrorResume(IOException.class, __ -> Flux.just(defaultTransaction))))
                .expectSubscription()
                .expectNext(defaultTransaction, defaultTransaction, defaultTransaction)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableCohereFluxBuilderWithCachingNonReactive() {

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(toPublisher(this::getBillingInfoNonReactive)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(toPublisher(this::getAllOrdersNonReactive), cache(TreeMap::new)))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithCustomCaching() {

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, cache(TreeMap::new)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(TreeMap::new)))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(2)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithCachingSet() {

        var cohereFlux = cohereFluxOf(TransactionSet.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfoWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToManyAsSet(OrderItem::id, cached(this::getAllOrdersWithIdSet))),
                        TransactionSet::new)
                .build(immediate());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithCachingWithIDsAsSet() {

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfoWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToMany(OrderItem::id, cached(this::getAllOrdersWithIdSet))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(2)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCaching() throws InterruptedException {

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3)
                .subscribeOn(parallel());

        Flux<OrderItem> orderItemFlux = Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33)
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        Function<CacheFactory<Long, BillingInfo, BillingInfo>, CacheFactory<Long, BillingInfo, BillingInfo>> cff1 = cf -> cf;
        Function<CacheFactory<Long, BillingInfo, BillingInfo>, CacheFactory<Long, BillingInfo, BillingInfo>> cff2 = cf -> cf;

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(autoCacheBuilder(billingInfoFlux).maxWindowSize(10).build(), cff1, cff2))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(cache(), autoCacheBuilder(orderItemFlux).maxWindowSize(10).build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCaching2() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3);

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(autoCacheBuilder(billingInfoFlux).maxWindowSize(4).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(3, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCaching3() {

        Flux<BillingInfo> dataSource1 = Flux.just(billingInfo1, billingInfo2, billingInfo3);
        Flux<OrderItem> dataSource2 = Flux.just(
                orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCacheBuilder(dataSource1).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCacheBuilder(dataSource2).maxWindowSize(1).build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(5)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCachingLifeCycleEventListener() {

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3)
                .subscribeOn(parallel());

        Flux<OrderItem> orderItemFlux = Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33)
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        var lifeCycleEventBroadcaster = lifeCycleEventBroadcaster();

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo,
                                autoCacheBuilder(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(),
                                autoCacheBuilder(orderItemFlux)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process)
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
    public void testReusableCohereFluxBuilderWithAutoCachingError() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, null, "4540111111111111"); // null customerId, will trigger NullPointerException

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2Unknown, updatedBillingInfo2, billingInfo3);

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(
                                autoCacheBuilder(billingInfoFlux)
                                        .errorHandler(error -> assertInstanceOf(NullPointerException.class, error))
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(3, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCachingEvents() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        Flux<Updated<BillingInfo>> billingInfoEventFlux = Flux.just(
                        updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
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

        CacheTransformer<Long, BillingInfo, BillingInfo> billingInfoAutoCache =
                autoCacheEvents(billingInfoEventFlux)
                        .maxWindowSize(3)
                        .build();

        CacheTransformer<Long, OrderItem, List<OrderItem>> orderItemAutoCache =
                autoCacheBuilder(orderItemFlux, toCacheEvent(CDCAdd.class::isInstance, CDC::item))
                        .maxWindowSize(3)
                        .build();

        var billingInfoRule = Rule.<Customer, Long, BillingInfo, BillingInfo>rule(BillingInfo::customerId, oneToOne(cached(billingInfoAutoCache)));
        var orderItemRule = Rule.<Customer, Long, OrderItem, List<OrderItem>>rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(cache(), orderItemAutoCache)));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(billingInfoRule, orderItemRule, Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCachingMultipleEventSources() {

        var billingInfoFlux = Flux.just( // E.g. Flux coming from a CDC/Kafka source
                new MyOtherEvent<>(billingInfo1, true), new MyOtherEvent<>(billingInfo2, true),
                new MyOtherEvent<>(billingInfo2, false), new MyOtherEvent<>(billingInfo3, false));

        var orderItemFlux = Flux.just(
                new CDCAdd<>(orderItem11), new CDCAdd<>(orderItem12), new CDCAdd<>(orderItem13),
                new CDCAdd<>(orderItem21), new CDCAdd<>(orderItem22),
                new CDCAdd<>(orderItem31), new CDCAdd<>(orderItem32), new CDCAdd<>(orderItem33),
                new CDCDelete<>(orderItem31), new CDCDelete<>(orderItem32), new CDCDelete<>(orderItem33));

        CacheTransformer<Long, BillingInfo, BillingInfo> billingInfoAutoCache =
                autoCache(billingInfoFlux, MyOtherEvent::isAddEvent, MyOtherEvent::value);

        CacheTransformer<Long, OrderItem, List<OrderItem>> orderItemAutoCache =
                autoCache(orderItemFlux, CDCAdd.class::isInstance, CDC::item);

        CohereFlux<Customer, Transaction> cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, billingInfoAutoCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, orderItemAutoCache))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCachingEvents2() {

        Function<List<Customer>, Publisher<OrderItem>> getAllOrders = customers -> {

            var customerIds = transform(customers, Customer::customerId);

            assertEquals(List.of(3L), customerIds);
            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3)
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                        cdcAdd(orderItem21), cdcAdd(orderItem22),
                        cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                        cdcDelete(orderItem31), cdcDelete(orderItem32), cdcDelete(orderItem33))
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(
                                autoCacheBuilder(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .maxRetryStrategy(20)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders,
                                autoCacheBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .backoffRetryStrategy(20, ofMillis(1))
                                        .build()))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(3)
                        .delayElements(ofMillis(100))
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCachingEventsOnSameThread() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<CacheEvent<BillingInfo>> billingInfoEventFlux = Flux.just(
                updated(billingInfo1), updated(billingInfo2), updated(billingInfo3), updated(updatedBillingInfo2));

        var orderItemFlux = Flux.just(
                        cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                        cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(orderItem31),
                        cdcAdd(orderItem32), cdcAdd(orderItem33), cdcDelete(orderItem31),
                        cdcDelete(orderItem32))
                .map(toCacheEvent(CDCAdd.class::isInstance, CDC::item));

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem33));

        var cohereFlux = cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCacheEvents(billingInfoEventFlux).maxWindowSize(3).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCacheEvents(orderItemFlux).maxWindowSize(3).build()))),
                        Transaction::new)
                .build(immediate());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(cohereFlux::process))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(0, billingInvocationCount.get());
        assertEquals(0, ordersInvocationCount.get());
    }
}