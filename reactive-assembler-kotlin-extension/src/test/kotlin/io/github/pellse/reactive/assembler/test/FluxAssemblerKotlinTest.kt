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

package io.github.pellse.reactive.assembler.test

import io.github.pellse.reactive.assembler.Rule.rule
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache
import io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheBuilder
import io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheEvents
import io.github.pellse.reactive.assembler.caching.CacheEvent.*
import io.github.pellse.reactive.assembler.caching.CacheFactory.cache
import io.github.pellse.reactive.assembler.kotlin.*
import io.github.pellse.reactive.assembler.test.ReactiveAssemblerTestUtils.*
import io.github.pellse.reactive.assembler.util.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers.parallel
import reactor.test.StepVerifier
import java.time.Duration.ofMillis
import java.util.concurrent.atomic.AtomicInteger

class FluxAssemblerKotlinTest {

    private val billingInvocationCount = AtomicInteger()
    private val ordersInvocationCount = AtomicInteger()

    private fun getBillingInfo(customerIds: List<Long>): Publisher<BillingInfo> {
        return Flux.just(billingInfo1, billingInfo3)
            .filter { customerIds.contains(it.customerId) }
            .doOnComplete(billingInvocationCount::incrementAndGet)
    }

    private fun getAllOrders(customerIds: List<Long>): Publisher<OrderItem> {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
            .filter { customerIds.contains(it.customerId) }
            .doOnComplete(ordersInvocationCount::incrementAndGet)
    }

    private fun getBillingInfoWithIdSet(customerIds: Set<Long>): Publisher<BillingInfo> {
        return Flux.just(billingInfo1, billingInfo3)
            .filter { customerIds.contains(it.customerId()) }
            .doOnComplete { billingInvocationCount.incrementAndGet() }
    }

    private fun getAllOrdersWithIdSet(customerIds: Set<Long>): Publisher<OrderItem> {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
            .filter { customerIds.contains(it.customerId()) }
            .doOnComplete { ordersInvocationCount.incrementAndGet() }
    }

    private fun getCustomers(): Flux<Customer> {
        return Flux.just(
            customer1,
            customer2,
            customer3,
            customer1,
            customer2,
            customer3,
            customer1,
            customer2,
            customer3
        )
    }

    private fun getBillingInfoNonReactive(customerIds: List<Long>): List<BillingInfo> {
        val list = listOf(billingInfo1, billingInfo3)
            .filter { billingInfo: BillingInfo -> customerIds.contains(billingInfo.customerId()) }

        billingInvocationCount.incrementAndGet()
        return list
    }

    private fun getAllOrdersNonReactive(customerIds: List<Long>): List<OrderItem> {
        val list = listOf(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
            .filter { orderItem: OrderItem -> customerIds.contains(orderItem.customerId()) }

        ordersInvocationCount.incrementAndGet()
        return list
    }


    @BeforeEach
    fun setup() {
        billingInvocationCount.set(0)
        ordersInvocationCount.set(0)
    }

    @Test
    fun testReusableAssemblerBuilderWithFluxWithBuffering() {

        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, ::getBillingInfo.oneToOne(::BillingInfo)),
                rule(OrderItem::customerId, ::getAllOrders.oneToMany(OrderItem::id)),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderTransactionSet() {

        val assembler = assembler<TransactionSet>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, ::hashSetOf, ::getBillingInfoWithIdSet.oneToOne(::BillingInfo)),
                rule(OrderItem::customerId, ::hashSetOf, ::getAllOrdersWithIdSet.oneToMany(OrderItem::id, ::hashSetOf)),
                ::TransactionSet
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transactionSet1,
                transactionSet2,
                transactionSet3,
                transactionSet1,
                transactionSet2,
                transactionSet3,
                transactionSet1,
                transactionSet2,
                transactionSet3
            )
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithNonReactiveDatasources() {

        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfoNonReactive.toPublisher(), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrdersNonReactive.toPublisher())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithNonReactiveCachedDatasources() {

        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(
                    BillingInfo::customerId,
                    oneToOne(::getBillingInfoNonReactive.toPublisher().cached(), ::BillingInfo)
                ),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrdersNonReactive.toPublisher().cached())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithCacheWindow3() {
        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(::sortedMapOf), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrders.cached())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .delayElements(ofMillis(100))
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(1, billingInvocationCount.get())
        assertEquals(1, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithCacheWindow2() {
        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(cache(::sortedMapOf)), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrders.cached(caffeineCache()))),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(2)
                .delayElements(ofMillis(100))
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(2, billingInvocationCount.get())
        assertEquals(2, ordersInvocationCount.get())
    }

    sealed interface CDC<T> {
        val item: T
    }

    data class CDCAdd<T>(override val item: T) : CDC<T>
    data class CDCDelete<T>(override val item: T) : CDC<T>

    @Test
    fun testReusableAssemblerBuilderWithAutoCachingEvents2() {

        val getAllOrders = { customerIds: List<Long> ->
            assertEquals(listOf(3L), customerIds)
            Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter { orderItem: OrderItem -> customerIds.contains(orderItem.customerId()) }
                .doOnComplete { ordersInvocationCount.incrementAndGet() }
        }

        val updatedBillingInfo2 = BillingInfo(2L, 2L, "4540111111111111")

        val billingInfoFlux =
            Flux.just(updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
                .subscribeOn(parallel())

        val orderItemFlux = Flux.just(
            CDCAdd(orderItem11), CDCAdd(orderItem12), CDCAdd(orderItem13),
            CDCAdd(orderItem21), CDCAdd(orderItem22),
            CDCAdd(orderItem31), CDCAdd(orderItem32), CDCAdd(orderItem33),
            CDCDelete(orderItem31), CDCDelete(orderItem32), CDCDelete(orderItem33)
        )
            .map {
                when (it) {
                    is CDCAdd -> Updated(it.item)
                    is CDCDelete -> Removed(it.item)
                }
            }
            .subscribeOn(parallel())

        val transaction2 = Transaction(customer2, updatedBillingInfo2, listOf(orderItem21, orderItem22))

        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(
                    BillingInfo::customerId,
                    oneToOne(::getBillingInfo.cached(autoCacheEvents(billingInfoFlux).maxWindowSize(3).build()))
                ),
                rule(
                    OrderItem::customerId,
                    oneToMany(
                        OrderItem::id,
                        getAllOrders.cached(cache(), autoCacheEvents(orderItemFlux).maxWindowSize(3).build())
                    )
                ),
                ::Transaction
            )
            .build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .delayElements(ofMillis(100))
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction3
            )
            .expectComplete()
            .verify()

        assertEquals(0, billingInvocationCount.get())
        assertEquals(1, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithAutoCachingEvents3() {

        val billingInfoFlux: Flux<CDC<BillingInfo>> =
            Flux.just(CDCAdd(billingInfo1), CDCAdd(billingInfo2), CDCAdd(billingInfo3), CDCDelete(billingInfo3))

        val orderItemFlux: Flux<CDC<OrderItem>> = Flux.just(
            CDCAdd(orderItem31), CDCAdd(orderItem32), CDCAdd(orderItem33),
            CDCDelete(orderItem31), CDCDelete(orderItem32), CDCDelete(orderItem33)
        )

        val assembler = assembler<Transaction>()
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId,
                    oneToOne(
                        ::getBillingInfo.cached(autoCache(billingInfoFlux, CDCAdd::class::isInstance) { it.item }))),
                rule(OrderItem::customerId,
                    oneToMany(OrderItem::id,
                        ::getAllOrders.cached(
                            autoCacheBuilder(orderItemFlux, CDCAdd::class::isInstance, CDC<OrderItem>::item)
                                .maxWindowSize(3)
                                .build()))),
                ::Transaction)
            .build()
    }
}
