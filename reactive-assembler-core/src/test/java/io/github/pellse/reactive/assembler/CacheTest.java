package io.github.pellse.reactive.assembler;

import io.github.pellse.assembler.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.CacheFactory.cache;
import static io.github.pellse.reactive.assembler.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.RuleMapper.*;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CacheTest {

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

    private Publisher<BillingInfo> getBillingInfosWithIdSet(Set<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrdersWithIdSet(Set<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()))
                .doOnComplete(ordersInvocationCount::incrementAndGet);
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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfos), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(cached(this::getAllOrders, new HashMap<>()))),
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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfos, () -> new HashMap<>()), BillingInfo::new)),
                        rule(OrderItem::customerId, oneToMany(cached(this::getAllOrders, cache(HashMap::new)))),
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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfosWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToManyAsSet(cached(this::getAllOrdersWithIdSet))),
                        TransactionSet::new)
                .build(Schedulers.immediate());

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
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        rule(BillingInfo::customerId, HashSet::new, oneToOne(cached(this::getBillingInfosWithIdSet), BillingInfo::new)),
                        rule(OrderItem::customerId, HashSet::new, oneToMany(cached(this::getAllOrdersWithIdSet))),
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
}

