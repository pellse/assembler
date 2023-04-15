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

import io.github.pellse.reactive.assembler.Assembler;
import io.github.pellse.reactive.assembler.caching.CacheEvent;
import io.github.pellse.reactive.assembler.caching.CacheFactory;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import io.github.pellse.reactive.assembler.util.BillingInfo;
import io.github.pellse.reactive.assembler.util.Customer;
import io.github.pellse.reactive.assembler.util.OrderItem;
import io.github.pellse.reactive.assembler.util.Transaction;
import io.github.pellse.reactive.assembler.util.TransactionSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.LifeCycleEventBroadcaster.lifeCycleEventBroadcaster;
import static io.github.pellse.reactive.assembler.QueryUtils.toPublisher;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheBuilder;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheEvents;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.*;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCacheFactory.concurrent;
import static io.github.pellse.reactive.assembler.test.CDCAdd.cdcAdd;
import static io.github.pellse.reactive.assembler.test.CDCDelete.cdcDelete;
import static io.github.pellse.reactive.assembler.test.ReactiveAssemblerTestUtils.*;
import static io.github.pellse.util.ObjectUtils.run;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
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

    private static <T> Flux<T> longRunningFlux(List<T> list, int nsDelay, int threadPoolSize) {
        return longRunningFlux(list, null, nsDelay, threadPoolSize);
    }

    private static <T> Flux<T> longRunningFlux(List<T> list, AtomicLong counter, int nsDelay, int threadPoolSize) {
        return longRunningFlux(list, counter, nsDelay, -1, threadPoolSize);
    }

    private static <T> Flux<T> longRunningFlux(List<T> list, AtomicLong counter, int nsDelay, int maxItems, int threadPoolSize) {
        return generate(
                threadPoolSize,
                nsDelay,
                NANOSECONDS,
                0,
                i -> (i + 1) % list.size(),
                list::get,
                (count, state) -> count >= maxItems && maxItems != -1,
                (count, value) -> ofNullable(counter).ifPresent(AtomicLong::incrementAndGet));
    }

    private static <T, S> Flux<T> generate(
            int threadPoolSize,
            long nsDelay,
            TimeUnit unit,
            S initialState,
            UnaryOperator<S> stateUpdater,
            Function<S, T> valueGenerator,
            BiPredicate<Long, S> stopCondition,
            BiConsumer<Long, T> onValueGenerated) {

        var scheduledExecutorService = newScheduledThreadPool(threadPoolSize);
        var subscriberCount = new AtomicInteger();

        return Flux.create(sink -> {

            var state = new AtomicReference<>(initialState);
            var generatedValueCount = new AtomicLong();

            var scheduledFuture = new AtomicReference<ScheduledFuture<?>>();
            subscriberCount.incrementAndGet();

            scheduledFuture.set(scheduledExecutorService.scheduleAtFixedRate(() -> {

                var notCancelled = !sink.isCancelled();
                var oldState = state.getAndUpdate(stateUpdater);
                var count = generatedValueCount.getAndIncrement();

                if (notCancelled && !stopCondition.test(count, oldState)) {
                    var value = valueGenerator.apply(oldState);
                    sink.next(value);
                    onValueGenerated.accept(count, value);
                } else {
                    if (notCancelled) {
                        sink.complete();
                    }
                    scheduledFuture.get().cancel(true);
                    if (subscriberCount.decrementAndGet() == 0) {
                        scheduledExecutorService.shutdownNow();
                    }
                }
            }, 0, nsDelay, unit));
        });
    }

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

    private Publisher<BillingInfo> getBillingInfoWithIdSet(Set<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrdersWithIdSet(Set<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
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
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3, customer1, customer2, customer3);
    }

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);
    }

    @Test
    public void testReusableAssemblerBuilderWithCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
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
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, concurrent()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), concurrent()))),
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

        CacheFactory<Long, BillingInfo, BillingInfo> faultyCache = cache(
                (ids, computeIfAbsent) -> error(new RuntimeException("Cache.getAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
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
        Function<List<Long>, Publisher<BillingInfo>> getBillingInfo = ids -> Flux.just(billingInfo1)
                .flatMap(__ -> Flux.error(new IOException()));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
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

        CacheFactory<Long, BillingInfo, BillingInfo> faultyCache = cache(
                (ids, computeIfAbsent) -> error(new RuntimeException("Cache.getAll failed")),
                map -> error(new RuntimeException("Cache.putAll failed")),
                map -> error(new RuntimeException("Cache.removeAll failed")));

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Long>, Publisher<BillingInfo>> getBillingInfo = ids -> Flux.error(new IOException());

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, faultyCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
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

        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithCachingNonReactive() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(toPublisher(this::getBillingInfoNonReactive)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(toPublisher(this::getAllOrdersNonReactive), cache(TreeMap::new)))),
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
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, cache(TreeMap::new)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(TreeMap::new)))),
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
    public void testReusableAssemblerBuilderWithCachingSet() {

        var assembler = assemblerOf(TransactionSet.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfoWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToManyAsSet(OrderItem::id, cached(this::getAllOrdersWithIdSet))),
                        TransactionSet::new)
                .build(immediate());

        StepVerifier.create(getCustomers()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3)
                .expectComplete()
                .verify();

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithCachingWithIDsAsSet() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfoWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToMany(OrderItem::id, cached(this::getAllOrdersWithIdSet))),
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

        Function<CacheFactory<Long, BillingInfo, BillingInfo>, CacheFactory<Long, BillingInfo, BillingInfo>> cff1 = cf -> cf;
        Function<CacheFactory<Long, BillingInfo, BillingInfo>, CacheFactory<Long, BillingInfo, BillingInfo>> cff2 = cf -> cf;

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCacheBuilder(billingInfoFlux).maxWindowSize(10).build(), cff1, cff2))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), autoCacheBuilder(orderItemFlux).maxWindowSize(10).build()))),
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

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2, updatedBillingInfo2, billingInfo3);

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(autoCacheBuilder(billingInfoFlux).maxWindowSize(4).build()))),
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
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCacheBuilder(dataSource1).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCacheBuilder(dataSource2).maxWindowSize(1).build()))),
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
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
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

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, null, "4540111111111111"); // null customerId, will trigger NullPointerException

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2Unknown, updatedBillingInfo2, billingInfo3);

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
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

        var billingInfoRule = rule(BillingInfo::customerId, oneToOne(cached(billingInfoAutoCache)));
        var orderItemRule = rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(cache(), orderItemAutoCache)));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(billingInfoRule, orderItemRule, Transaction::new)
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

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, billingInfoAutoCache), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, orderItemAutoCache))),
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
    public void testReusableAssemblerBuilderWithAutoCachingEvents2() {

        Function<List<Long>, Publisher<OrderItem>> getAllOrders = customerIds -> {
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

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(
                                autoCacheBuilder(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .concurrency(20)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders,
                                autoCacheBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .concurrency(20, ofMillis(1))
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

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCacheEvents(billingInfoEventFlux).maxWindowSize(3).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCacheEvents(orderItemFlux).maxWindowSize(3).build()))),
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

    @Test
    @Timeout(20)
    public void testLongRunningAutoCachingEvents() throws InterruptedException {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        Function<List<Long>, Publisher<BillingInfo>> getBillingInfo = customerIds ->
                Flux.just(billingInfo1, billingInfo3)
                        .filter(billingInfo -> customerIds.contains(billingInfo.customerId()));

        Function<List<Long>, Publisher<OrderItem>> getAllOrders = customerIds ->
                Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                        .filter(orderItem -> customerIds.contains(orderItem.customerId()));

        var customerList = List.of(customer1, customer2, customer3);

        var billingInfoList = List.of(cdcAdd(billingInfo1), cdcAdd(billingInfo2), cdcAdd(updatedBillingInfo2), cdcAdd(billingInfo3));

        var orderItemList = List.of(cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(updatedOrderItem22),
                cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                cdcDelete(orderItem31), cdcDelete(orderItem32), cdcAdd(updatedOrderItem11));

        var customerFlux = longRunningFlux(customerList, 1, Runtime.getRuntime().availableProcessors());
        var billingInfoFlux = longRunningFlux(billingInfoList, 1, Runtime.getRuntime().availableProcessors());
        var orderItemFlux = longRunningFlux(orderItemList, 1, Runtime.getRuntime().availableProcessors());

        var lifeCycleEventBroadcaster = lifeCycleEventBroadcaster();

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo,
                                autoCacheBuilder(billingInfoFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .scheduler(newParallel("billing-info"))
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders,
                                autoCacheBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .scheduler(newParallel("order-item"))
                                        .build()))),
                        Transaction::new)
                .build();

        var transactionFlux = customerFlux
                .window(3)
                .delayElements(ofNanos(1))
                .flatMapSequential(assembler::assemble)
                .take(10_000)
                .doOnSubscribe(run(lifeCycleEventBroadcaster::start))
                .doFinally(run(lifeCycleEventBroadcaster::stop));

        var latch = new CountDownLatch(500);
        IntStream.range(0, 500).forEach(__ -> transactionFlux.subscribe(null, error -> latch.countDown(), latch::countDown));
        latch.await();
    }
}