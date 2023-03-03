package io.github.pellse.reactive.assembler.test;

import io.github.pellse.assembler.*;
import io.github.pellse.reactive.assembler.caching.CacheEvent;
import io.github.pellse.reactive.assembler.caching.CacheFactory;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.LifeCycleEventBroadcaster.lifeCycleEventBroadcaster;
import static io.github.pellse.reactive.assembler.QueryUtils.toPublisher;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorContinue.onErrorContinue;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCache;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheEvents;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.*;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.ConcurrentCacheFactory.concurrentCache;
import static io.github.pellse.reactive.assembler.test.CDCAdd.cdcAdd;
import static io.github.pellse.reactive.assembler.test.CDCDelete.cdcDelete;
import static io.github.pellse.util.ObjectUtils.run;
import static java.time.Duration.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static reactor.core.publisher.Mono.error;
import static reactor.core.scheduler.Schedulers.*;

interface CDC<T> {
    T item();
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

        assertEquals(1, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithConcurrentCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, concurrentCache(cache())), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), concurrentCache()))),
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

        assertEquals(3, billingInvocationCount.get());
        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithFaultyQueryFunction() {

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Long>, Publisher<BillingInfo>> getBillingInfo = ids -> Flux.error(new IOException());

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
                        Transaction::new)
                .build(boundedElastic());

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
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(toPublisher(this::getAllOrdersNonReactive), new HashMap<>()))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, () -> new HashMap<>()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(HashMap::new)))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCache(billingInfoFlux).maxWindowSize(10).build(), cff1, cff2))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), autoCache(orderItemFlux).maxWindowSize(10).build()))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(autoCache(billingInfoFlux).maxWindowSize(4).build()))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCache(dataSource1).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCache(dataSource2).maxWindowSize(1).build()))),
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
                                autoCache(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .lifeCycleEventSource(lifeCycleEventBroadcaster)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(),
                                autoCache(orderItemFlux)
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
                                autoCache(billingInfoFlux)
                                        .errorHandler(onErrorContinue(error -> assertInstanceOf(NullPointerException.class, error)))
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
                autoCache(orderItemFlux, toCacheEvent(CDCAdd.class::isInstance, CDC::item))
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
    public void testReusableAssemblerBuilderWithAutoCachingEvents2() {

        Function<List<Long>, Publisher<OrderItem>> getAllOrders = customerIds -> {
            assertEquals(List.of(3L), customerIds);
            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<Updated<BillingInfo>> billingInfoFlux = Flux.just(
                        updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
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
                                autoCacheEvents(billingInfoFlux)
                                        .maxWindowSize(3)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders, cache(),
                                autoCache(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
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
    public void testLongRunningAutoCachingEvents() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        var customerList = List.of(customer1, customer2, customer3);

        var billingInfoList = List.of(cdcAdd(billingInfo1), cdcAdd(billingInfo2), cdcAdd(updatedBillingInfo2), cdcAdd(billingInfo3));

        var orderItemList = List.of(cdcAdd(orderItem11), cdcAdd(orderItem12), cdcAdd(orderItem13),
                cdcAdd(orderItem21), cdcAdd(orderItem22), cdcAdd(updatedOrderItem22),
                cdcAdd(orderItem31), cdcAdd(orderItem32), cdcAdd(orderItem33),
                cdcDelete(orderItem31), cdcDelete(orderItem32), cdcAdd(updatedOrderItem11));

        var billingInfoCount = new AtomicInteger();
        var orderItemCount = new AtomicInteger();

        var customerFlux = longRunningFlux(customerList, 1);

        var billingInfoFlux = longRunningFlux(billingInfoList, billingInfoCount, 1, 10_100)
                .map(toCacheEvent(CDCAdd.class::isInstance, CDC::item));

        var orderItemFlux = longRunningFlux(orderItemList, orderItemCount, 1, 10_100)
                .map(toCacheEvent(CDCAdd.class::isInstance, CDC::item));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(autoCacheEvents(billingInfoFlux).maxWindowSize(3).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(autoCacheEvents(orderItemFlux).maxWindowSize(3).build()))),
                        Transaction::new)
                .build(boundedElastic());

        var transactionFlux = customerFlux
                .window(3)
                .delayElements(ofNanos(1))
                .flatMapSequential(assembler::assemble)
                .take(10_000)
                .subscribeOn(boundedElastic());

        IntStream.range(0, 500).forEach(__ -> transactionFlux.subscribe());
        transactionFlux.blockLast(ofSeconds(30));
    }

    private <T> Flux<T> longRunningFlux(List<T> list, int nsDelay) {
        return longRunningFlux(list, null, nsDelay, -1);
    }

    private <T> Flux<T> longRunningFlux(List<T> list, AtomicInteger counter, int nsDelay, int maxItems) {
        int size = list.size();
        return Flux.<T, Integer>generate(() -> 0, (index, sink) -> {
                    if (counter != null && counter.incrementAndGet() == maxItems) {
                        sink.complete();
                    }
                    sink.next(list.get(index));
                    return (index + 1) % size;
                })
                .delayElements(ofNanos(nsDelay))
                .subscribeOn(boundedElastic());
    }
}

