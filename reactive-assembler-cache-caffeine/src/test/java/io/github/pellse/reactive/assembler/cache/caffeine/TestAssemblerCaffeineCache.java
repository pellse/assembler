package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.pellse.assembler.BillingInfo;
import io.github.pellse.assembler.Customer;
import io.github.pellse.assembler.OrderItem;
import io.github.pellse.assembler.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.Mapper.oneToMany;
import static io.github.pellse.reactive.assembler.Mapper.oneToOne;
import static io.github.pellse.reactive.assembler.MapperCache.cached;
import static io.github.pellse.reactive.assembler.cache.caffeine.CaffeineMapperCacheHelper.newCache;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAssemblerCaffeineCache {

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

    private Flux<Customer> getCustomers() {
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3);
    }

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);
    }

    @Test
    public void testReusableAssemblerBuilderWithCaffeineCache() {

        final Cache<Iterable<Long>, Publisher<Map<Long, BillingInfo>>> billingInfoCache = newBuilder().maximumSize(10).build();

        var assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        cached(oneToOne(this::getBillingInfos, BillingInfo::customerId, BillingInfo::new), billingInfoCache::get),
                        cached(oneToMany(this::getAllOrders, OrderItem::customerId), newCache(newBuilder().maximumSize(10))),
                        Transaction::new)
                .build();

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
}
