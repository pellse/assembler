package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.assembler.*
import io.github.pellse.assembler.AssemblerTestUtils.*
import io.github.pellse.reactive.assembler.caching.Cache.cache
import io.github.pellse.reactive.assembler.Mapper.rule
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
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
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3, customer1, customer2, customer3)
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
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, ::getBillingInfo.oneToOne(::BillingInfo)),
                rule(OrderItem::customerId, ::getAllOrders.oneToMany()),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderTransactionSet() {

        val assembler = assembler<TransactionSet>()
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, ::hashSetOf, ::getBillingInfoWithIdSet.oneToOne(::BillingInfo)),
                rule(OrderItem::customerId, ::hashSetOf, ::getAllOrdersWithIdSet.oneToMany(::hashSetOf)),
                ::TransactionSet
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3)
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithNonReactiveDatasources() {

        val assembler = assembler<Transaction>()
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfoNonReactive.toPublisher(), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(::getAllOrdersNonReactive.toPublisher())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithNonReactiveCachedDatasources() {

        val assembler = assembler<Transaction>()
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfoNonReactive.toPublisher().cached(), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(::getAllOrdersNonReactive.toPublisher().cached())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(3, billingInvocationCount.get())
        assertEquals(3, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithCacheWindow3() {
        val assembler = assembler<Transaction>()
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(::getAllOrders.cached())),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .delayElements(ofMillis(100))
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(1, billingInvocationCount.get())
        assertEquals(1, ordersInvocationCount.get())
    }

    @Test
    fun testReusableAssemblerBuilderWithCacheWindow2() {
        val assembler = assembler<Transaction>()
            .withIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(cache()), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(::getAllOrders.cached(caffeineCache()))),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(2)
                .delayElements(ofMillis(100))
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(2, billingInvocationCount.get())
        assertEquals(2, ordersInvocationCount.get())
    }
}
