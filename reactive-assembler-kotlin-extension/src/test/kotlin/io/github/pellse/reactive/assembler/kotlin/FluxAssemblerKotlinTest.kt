package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.assembler.*
import io.github.pellse.assembler.AssemblerTestUtils.*
import io.github.pellse.reactive.assembler.Mapper.rule
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache
import io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheEvents
import io.github.pellse.reactive.assembler.caching.Cache.cache
import io.github.pellse.reactive.assembler.caching.CacheEvent.*
import io.github.pellse.reactive.assembler.kotlin.FluxAssemblerKotlinTest.CDC.CDCAdd
import io.github.pellse.reactive.assembler.kotlin.FluxAssemblerKotlinTest.CDC.CDCDelete
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
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
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
            .expectNext(transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3, transactionSet1, transactionSet2, transactionSet3)
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
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
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
                rule(BillingInfo::customerId, oneToOne(::getBillingInfoNonReactive.toPublisher().cached(), ::BillingInfo)),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrdersNonReactive.toPublisher().cached())),
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
            .withCorrelationIdExtractor(Customer::customerId)
            .withAssemblerRules(
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(), ::BillingInfo)),
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
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
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
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(cache()), ::BillingInfo)),
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
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(2, billingInvocationCount.get())
        assertEquals(2, ordersInvocationCount.get())
    }

    sealed interface CDC {
        data class CDCAdd(val item: OrderItem) : CDC
        data class CDCDelete(val item: OrderItem) : CDC
    }

    @Test
    fun testReusableAssemblerBuilderWithAutoCachingEvents2() {

        val getAllOrders = { customerIds: List<Long> ->
            assertEquals(listOf(3L), customerIds)
            Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter { orderItem: OrderItem -> customerIds.contains(orderItem.customerId()) }
                .doOnComplete { ordersInvocationCount.incrementAndGet() }
        }

        val updatedBillingInfo2 = BillingInfo(2L, 2L, "4540111111111111")

        val billingInfoFlux = Flux.just(updated(billingInfo1), updated(billingInfo2), updated(updatedBillingInfo2), updated(billingInfo3))
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
                rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(autoCacheEvents(billingInfoFlux).maxWindowSize(3).build()))),
                rule(OrderItem::customerId, oneToMany(OrderItem::id, getAllOrders.cached(cache(), autoCacheEvents(orderItemFlux).maxWindowSize(3).build()))),
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
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(0, billingInvocationCount.get())
        assertEquals(1, ordersInvocationCount.get())
    }
}
