package io.github.pellse.assembler.caching.spring;

import io.github.pellse.assembler.caching.CacheFactory;
import io.github.pellse.assembler.util.BillingInfo;
import io.github.pellse.assembler.util.Customer;
import io.github.pellse.assembler.util.OrderItem;
import io.github.pellse.assembler.util.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.RuleMapperSource.pipe;
import static io.github.pellse.assembler.caching.StreamTableFactory.streamTable;
import static io.github.pellse.assembler.caching.StreamTableFactoryBuilder.streamTableBuilder;
import static io.github.pellse.assembler.caching.CacheFactory.cached;
import static io.github.pellse.assembler.caching.CacheFactory.cachedMany;
import static io.github.pellse.assembler.caching.spring.SpringCacheAssemblerTest.CDCAdd.cdcAdd;
import static io.github.pellse.assembler.caching.spring.SpringCacheAssemblerTest.CDCDelete.cdcDelete;
import static io.github.pellse.assembler.caching.spring.SpringCacheFactory.springCache;
import static io.github.pellse.assembler.test.AssemblerTestUtils.*;
import static io.github.pellse.assembler.test.AssemblerTestUtils.transaction1;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtils.transform;
import static io.github.pellse.util.reactive.ReactiveUtils.defaultScheduler;
import static io.github.pellse.util.reactive.ReactiveUtils.scheduler;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Thread.currentThread;
import static java.time.Duration.ofMillis;
import static java.util.List.of;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.scheduler.Schedulers.*;
import static reactor.core.scheduler.Schedulers.newBoundedElastic;

@SpringBootTest
public class SpringCacheAssemblerTest {

    private static final String BILLING_INFO_CACHE = "billingInfoCache";
    private static final String ORDER_ITEMS_CACHE = "orderItemsCache";

    @Configuration
    @EnableCaching
    static class CaffeineCacheConfiguration {

        @Bean
        public CacheManager cacheManager() {
            return also(new CaffeineCacheManager(),
                    cacheManager -> cacheManager.setAsyncCacheMode(true),
                    cacheManager -> cacheManager.setCacheNames(of(BILLING_INFO_CACHE, ORDER_ITEMS_CACHE)));
        }
    }

    sealed interface CDC<T> {
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

    private Flux<Customer> getCustomers() {
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3, customer1, customer2, customer3);
    }

    private final CacheManager cacheManager;

    @Autowired
    SpringCacheAssemblerTest(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);

        cacheManager.getCacheNames()
                .forEach(name -> cacheManager.getCache(name).invalidate());
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

        Function<String, Consumer<Object>> simulateIO = tag -> __  -> {
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
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE),
                                streamTableBuilder(billingInfoFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .concurrent()
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(getOrderItems, springCache(cacheManager, ORDER_ITEMS_CACHE),
                                streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .concurrent()
                                        .build()))),
                        Transaction::new)
                .build();

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
    public void testReusableAssemblerBuilderWithSpringCache() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE)))),
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
    public void testReusableAssemblerBuilderWithSpringCache2() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE)))),
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
    public void testReusableAssemblerBuilderWithDoubleCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE))))),
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
    public void testReusableAssemblerBuilderWithTripleCaching() {

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(pipe(
                                        cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE)),
                                        CacheFactory::cached,
                                        CacheFactory::cached),
                                BillingInfo::new)),
                        rule(OrderItem::customerId,
                                oneToMany(OrderItem::id, pipe(
                                        cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE)),
                                        CacheFactory::cachedMany,
                                        CacheFactory::cachedMany))),
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
    public void testReusableAssemblerBuilderWithFaultyQueryFunction() {

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.error(new IOException());

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE)), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE)))),
                        Transaction::new)
                .build(defaultScheduler());

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
    public void testReusableAssemblerBuilderWithAutoCaching() {

        Flux<BillingInfo> dataSource1 = Flux.just(billingInfo1, billingInfo2, billingInfo3);
        Flux<OrderItem> dataSource2 = Flux.just(
                orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);

        Transaction transaction2 = new Transaction(customer2, billingInfo2, of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, of(orderItem31, orderItem32, orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(springCache(cacheManager, BILLING_INFO_CACHE), streamTable(dataSource1)))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(springCache(cacheManager, ORDER_ITEMS_CACHE), streamTable(dataSource2)))),
                        Transaction::new)
                .build();

        StepVerifier.create(getCustomers()
                        .window(1)
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
    public void testReusableAssemblerBuilderWithAutoCaching3() {

        Flux<BillingInfo> dataSource1 = Flux.just(billingInfo1, billingInfo2, billingInfo3);
        Flux<OrderItem> dataSource2 = Flux.just(
                orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);

        Transaction transaction2 = new Transaction(customer2, billingInfo2, of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, of(orderItem31, orderItem32, orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE), streamTableBuilder(dataSource1).build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id,
                                cachedMany(this::getAllOrders, springCache(cacheManager, ORDER_ITEMS_CACHE),
                                        streamTableBuilder(dataSource2)
                                                .maxWindowSize(3)
                                                .scheduler(boundedElastic())
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
        assertEquals(0, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithAutoCachingEvents() throws Exception {

        interface CDC {
            OrderItem item();
        }

        record CDCAdd(OrderItem item) implements CDC {
        }

        record CDCDelete(OrderItem item) implements CDC {
        }

        BillingInfo updatedBillingInfo2 = new BillingInfo(2, 2L, "4540111111111111");

        Flux<BillingInfo> billingInfoEventFlux = Flux.just(billingInfo1, billingInfo2, billingInfo3, updatedBillingInfo2)
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        new CDCAdd(orderItem11), new CDCAdd(orderItem12), new CDCAdd(orderItem13),
                        new CDCAdd(orderItem21), new CDCAdd(orderItem22), new CDCAdd(orderItem31),
                        new CDCAdd(orderItem32), new CDCAdd(orderItem33), new CDCDelete(orderItem31),
                        new CDCDelete(orderItem32))
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, of(orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, springCache(cacheManager, BILLING_INFO_CACHE),
                                streamTableBuilder(billingInfoEventFlux)
                                        .maxWindowSize(3)
                                        .build()))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(springCache(cacheManager, ORDER_ITEMS_CACHE),
                                streamTableBuilder(orderItemFlux, CDCAdd.class::isInstance, CDC::item)
                                        .maxWindowSize(3)
                                        .build()))),
                        Transaction::new)
                .build();

        Thread.sleep(100); // To give enough time to streamTable() calls above to subscribe and consume their flux

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
