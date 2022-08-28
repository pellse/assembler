package io.github.pellse.reactive.assembler.test;

import io.github.pellse.assembler.*;
import io.github.pellse.reactive.assembler.caching.CacheEvent;
import io.github.pellse.reactive.assembler.caching.CacheEvent.Updated;
import io.github.pellse.reactive.assembler.caching.CacheEvent.Removed;
import io.github.pellse.reactive.assembler.caching.CacheFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.QueryUtils.toPublisher;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static io.github.pellse.reactive.assembler.RuleMapperSource.emptyQuery;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorContinue.onErrorContinue;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.toCacheEvent;
import static io.github.pellse.reactive.assembler.caching.Cache.cache;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.updated;
import static io.github.pellse.reactive.assembler.caching.CacheEvent.removed;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static reactor.core.scheduler.Schedulers.immediate;
import static reactor.core.scheduler.Schedulers.parallel;

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
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, new HashMap<>()))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCache(toCacheEvent(billingInfoFlux), 10), cff1, cff2))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(), autoCache(toCacheEvent(orderItemFlux), 10)))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(emptyQuery(), autoCache(toCacheEvent(billingInfoFlux), 4)))),
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
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, autoCache(toCacheEvent(dataSource1))))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCache(toCacheEvent(dataSource2), 1)))),
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
    public void testReusableAssemblerBuilderWithAutoCachingError() {

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, null, "4540111111111111"); // null customerId, will trigger NullPointerException

        Flux<BillingInfo> billingInfoFlux = Flux.just(billingInfo1, billingInfo2Unknown, updatedBillingInfo2, billingInfo3);

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(emptyQuery(), autoCache(
                                toCacheEvent(billingInfoFlux),
                                onErrorContinue(error -> assertInstanceOf(NullPointerException.class, error)))))),
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

        record CDCAdd(OrderItem item) {
        }

        record CDCDelete(OrderItem item) {
        }

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540222222222222");
        OrderItem updatedOrderItem11 = new OrderItem("1", 1L, "Sweater", 25.99);
        OrderItem updatedOrderItem22 = new OrderItem("5", 2L, "Boots", 109.99);

        Flux<CacheEvent<BillingInfo>> billingInfoEventFlux = Flux.just(
                        updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        new CDCAdd(orderItem11), new CDCAdd(orderItem12), new CDCAdd(orderItem13),
                        new CDCAdd(orderItem21), new CDCAdd(orderItem22), new CDCAdd(updatedOrderItem22),
                        new CDCAdd(orderItem31), new CDCAdd(orderItem32), new CDCAdd(orderItem33),
                        new CDCDelete(orderItem31), new CDCDelete(orderItem32), new CDCAdd(updatedOrderItem11))
                .map(cdcEvent -> {
                    if (cdcEvent instanceof CDCAdd e) return updated(e.item);
                    else return removed(((CDCDelete) cdcEvent).item);
                })
                .subscribeOn(parallel());

        Transaction transaction1 = new Transaction(customer1, billingInfo1, List.of(updatedOrderItem11, orderItem12, orderItem13));
        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, updatedOrderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem33));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(emptyQuery(), autoCache(billingInfoEventFlux, 3)))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(emptyQuery(), cache(), autoCache(orderItemFlux, 3)))),
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
    public void testReusableAssemblerBuilderWithAutoCachingEvents2() {

        record CDCAdd(OrderItem item) {
        }

        record CDCDelete(OrderItem item) {
        }

        Function<List<Long>, Publisher<OrderItem>> getAllOrders = customerIds -> {
            assertEquals(List.of(3L), customerIds);
            return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                    .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                    .doOnComplete(ordersInvocationCount::incrementAndGet);
        };

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<CacheEvent<BillingInfo>> billingInfoFlux = Flux.just(
                        updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        new CDCAdd(orderItem11), new CDCAdd(orderItem12), new CDCAdd(orderItem13),
                        new CDCAdd(orderItem21), new CDCAdd(orderItem22),
                        new CDCAdd(orderItem31), new CDCAdd(orderItem32), new CDCAdd(orderItem33),
                        new CDCDelete(orderItem31), new CDCDelete(orderItem32), new CDCDelete(orderItem33))
                .map(cdcEvent -> {
                    if (cdcEvent instanceof CDCAdd e) return new Updated<>(e.item);
                    else return new Removed<>(((CDCDelete) cdcEvent).item);
                })
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));

        var assembler = assemblerOf(Transaction.class)
                .withCorrelationIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(emptyQuery(), autoCache(billingInfoFlux, 3)))),
                        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(getAllOrders, cache(), autoCache(orderItemFlux, 3)))),
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
}

