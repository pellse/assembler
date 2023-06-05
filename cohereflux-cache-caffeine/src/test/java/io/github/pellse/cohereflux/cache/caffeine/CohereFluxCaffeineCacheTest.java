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

package io.github.pellse.cohereflux.cache.caffeine;

import io.github.pellse.cohereflux.CohereFluxBuilder;
import io.github.pellse.cohereflux.Rule;
import io.github.pellse.cohereflux.RuleMapperSource;
import io.github.pellse.cohereflux.caching.AutoCacheFactoryBuilder;
import io.github.pellse.cohereflux.caching.CacheEvent.Updated;
import io.github.pellse.cohereflux.caching.CacheFactory;
import io.github.pellse.cohereflux.util.BillingInfo;
import io.github.pellse.cohereflux.util.Customer;
import io.github.pellse.cohereflux.util.OrderItem;
import io.github.pellse.cohereflux.util.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.cohereflux.RuleMapper.oneToMany;
import static io.github.pellse.cohereflux.RuleMapper.oneToOne;
import static io.github.pellse.cohereflux.caching.AutoCacheFactory.autoCache;
import static io.github.pellse.cohereflux.caching.CacheEvent.removed;
import static io.github.pellse.cohereflux.caching.CacheEvent.updated;
import static io.github.pellse.cohereflux.caching.CacheFactory.cached;
import static io.github.pellse.cohereflux.test.CohereFluxTestUtils.*;
import static io.github.pellse.util.collection.CollectionUtils.transform;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static reactor.core.scheduler.Schedulers.parallel;

public class CohereFluxCaffeineCacheTest {

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

    @BeforeEach
    void setup() {
        billingInvocationCount.set(0);
        ordersInvocationCount.set(0);
    }

    @Test
    public void testReusableCohereFluxBuilderWithCaffeineCache() {

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache()), BillingInfo::new)),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache(newBuilder().maximumSize(10))))),
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
    public void testReusableCohereFluxBuilderWithCaffeineCache2() {

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache(b -> b.maximumSize(10))), BillingInfo::new)),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache(newBuilder().maximumSize(10))))),
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
    public void testReusableCohereFluxBuilderWithDoubleCaching() {

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache()), BillingInfo::new)),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache())))),
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
    public void testReusableCohereFluxBuilderWithTripleCaching() {

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(RuleMapperSource.pipe(
                                        cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache()),
                                        CacheFactory::cached,
                                        CacheFactory::cached),
                                BillingInfo::new)),
                        Rule.rule(OrderItem::customerId,
                                oneToMany(OrderItem::id, RuleMapperSource.pipe(
                                        cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache()),
                                        CacheFactory::cached,
                                        CacheFactory::cached))),
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
    public void testReusableCohereFluxBuilderWithFaultyQueryFunction() {

        Transaction defaultTransaction = new Transaction(null, null, null);
        Function<List<Customer>, Publisher<BillingInfo>> getBillingInfo = entities -> Flux.error(new IOException());

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(getBillingInfo, CaffeineCacheFactory.caffeineCache()), BillingInfo::new)),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache()))),
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

        assertEquals(1, ordersInvocationCount.get());
    }

    @Test
    public void testReusableCohereFluxBuilderWithAutoCaching() {

        Flux<BillingInfo> dataSource1 = Flux.just(billingInfo1, billingInfo2, billingInfo3);
        Flux<OrderItem> dataSource2 = Flux.just(
                orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);

        Transaction transaction2 = new Transaction(customer2, billingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem31, orderItem32, orderItem33));

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache(), autoCache(dataSource1)))),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, CaffeineCacheFactory.caffeineCache(), autoCache(dataSource2)))),
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
    public void testReusableCohereFluxBuilderWithAutoCachingEvents() throws Exception {

        interface CDC {
            OrderItem item();
        }

        record CDCAdd(OrderItem item) implements CDC {
        }

        record CDCDelete(OrderItem item) implements CDC {
        }

        BillingInfo updatedBillingInfo2 = new BillingInfo(2L, 2L, "4540111111111111");

        Flux<Updated<BillingInfo>> billingInfoEventFlux = Flux.just(
                        updated(billingInfo1), updated(billingInfo2), updated(billingInfo3), updated(updatedBillingInfo2))
                .subscribeOn(parallel());

        var orderItemFlux = Flux.just(
                        new CDCAdd(orderItem11), new CDCAdd(orderItem12), new CDCAdd(orderItem13),
                        new CDCAdd(orderItem21), new CDCAdd(orderItem22), new CDCAdd(orderItem31),
                        new CDCAdd(orderItem32), new CDCAdd(orderItem33), new CDCDelete(orderItem31),
                        new CDCDelete(orderItem32))
                .map(e -> e instanceof CDCAdd ? updated(e.item()) : removed(e.item()))
                .subscribeOn(parallel());

        Transaction transaction2 = new Transaction(customer2, updatedBillingInfo2, List.of(orderItem21, orderItem22));
        Transaction transaction3 = new Transaction(customer3, billingInfo3, List.of(orderItem33));

        var cohereFlux = CohereFluxBuilder.cohereFluxOf(Transaction.class)
                .withCorrelationIdResolver(Customer::customerId)
                .withRules(
                        Rule.rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, CaffeineCacheFactory.caffeineCache(), AutoCacheFactoryBuilder.autoCacheEvents(billingInfoEventFlux).maxWindowSize(3).build()))),
                        Rule.rule(OrderItem::customerId, oneToMany(OrderItem::id, CacheFactory.cached(CaffeineCacheFactory.caffeineCache(), AutoCacheFactoryBuilder.autoCacheEvents(orderItemFlux).maxWindowSize(3).build()))),
                        Transaction::new)
                .build();

        Thread.sleep(100); // To give enough time to autoCache() calls above to subscribe and consume their flux

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
