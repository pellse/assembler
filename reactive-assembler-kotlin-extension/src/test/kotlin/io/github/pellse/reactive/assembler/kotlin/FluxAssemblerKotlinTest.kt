package io.github.pellse.reactive.assembler.kotlin

import io.github.pellse.assembler.AssemblerTestUtils.*
import io.github.pellse.assembler.BillingInfo
import io.github.pellse.assembler.Customer
import io.github.pellse.assembler.OrderItem
import io.github.pellse.assembler.Transaction
import io.github.pellse.reactive.assembler.Mapper.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.util.concurrent.atomic.AtomicInteger

class FluxAssemblerKotlinTest {

    private val billingInvocationCount = AtomicInteger()
    private val ordersInvocationCount = AtomicInteger()

    private fun getBillingInfos(customerIds: List<Long>): Publisher<BillingInfo> {
        return Flux.just(billingInfo1, billingInfo3)
            .filter { customerIds.contains(it.customerId) }
            .doOnComplete(billingInvocationCount::incrementAndGet)
    }

    private fun getAllOrders(customerIds: List<Long>): Publisher<OrderItem> {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
            .filter { customerIds.contains(it.customerId) }
            .doOnComplete(ordersInvocationCount::incrementAndGet)
    }

    private fun getCustomers(): Flux<Customer> {
        return Flux.just(customer1, customer2, customer3, customer1, customer2, customer3)
    }

    @BeforeEach
    fun setup() {
        billingInvocationCount.set(0)
        ordersInvocationCount.set(0)
    }

    @Test
    fun testReusableAssemblerBuilderWithFluxWithBuffering() {

        val assembler = assembler<Transaction>()
            .withIdExtractor(Customer::getCustomerId)
            .withAssemblerRules(
                oneToOne(this::getBillingInfos, BillingInfo::getCustomerId, ::BillingInfo),
                oneToMany(this::getAllOrders, OrderItem::getCustomerId),
                ::Transaction
            ).build()

        StepVerifier.create(
            getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble)
        )
            .expectSubscription()
            .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
            .expectComplete()
            .verify()

        assertEquals(2, billingInvocationCount.get())
        assertEquals(2, ordersInvocationCount.get())
    }
}
