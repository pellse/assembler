package io.github.pellse.reactive.assembler;

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
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.AssemblerRuleBuilder.rule;
import static io.github.pellse.reactive.assembler.FluxAdapter.fluxAdapter;
import static io.github.pellse.reactive.assembler.KeyValueStorePublisher.asKeyValueStore;
import static io.github.pellse.reactive.assembler.MapperBuilder.oneToManyAsList;
import static io.github.pellse.reactive.assembler.MapperBuilder.oneToOne;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FluxAssemblerWithMapperBuilderTest {

    private final AtomicInteger billingInvocationCount = new AtomicInteger();
    private final AtomicInteger ordersInvocationCount = new AtomicInteger();

    private Publisher<BillingInfo> getBillingInfos(List<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.getCustomerId()))
                .doOnComplete(billingInvocationCount::incrementAndGet);
    }

    private Publisher<OrderItem> getAllOrders(List<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.getCustomerId()))
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
    public void testReusableAssemblerBuilderWithFluxWithRuleBuilders() {

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        rule(oneToOne(this::getBillingInfos, BillingInfo::new)).withIdExtractor(BillingInfo::getCustomerId),
                        rule(oneToManyAsList(this::getAllOrders)).withIdExtractor(OrderItem::getCustomerId),
                        Transaction::new)
                .build(fluxAdapter());

        StepVerifier.create(getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();

        assertEquals(2, billingInvocationCount.get());
        assertEquals(2, ordersInvocationCount.get());
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithRuleBuilders2() {

        var a = asKeyValueStore(getBillingInfos(of(1L, 2L, 3L)), BillingInfo::getCustomerId);

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        rule(oneToOne(a, BillingInfo::new)).withIdExtractor(BillingInfo::getCustomerId),
                        rule(oneToManyAsList(this::getAllOrders)).withIdExtractor(OrderItem::getCustomerId),
                        Transaction::new)
                .build(fluxAdapter());

        StepVerifier.create(getCustomers()
                .window(3)
                .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2, transaction3)
                .expectComplete()
                .verify();
    }
}
