/*
 * Copyright 2024 Sebastien Pelletier
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

package io.github.pellse.assembler.test;

import io.github.pellse.assembler.BatchRule;
import io.github.pellse.assembler.util.BillingInfo;
import io.github.pellse.assembler.util.Customer;
import io.github.pellse.assembler.util.OrderItem;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static io.github.pellse.assembler.BatchRule.withIdResolver;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.caching.CacheFactory.cached;
import static io.github.pellse.assembler.caching.CacheFactory.cachedMany;
import static io.github.pellse.assembler.test.AssemblerTestUtils.*;

public class BatchRuleTest {

    private final BatchRule<Customer, BillingInfo> billingInfoBatchRule = withIdResolver(Customer::customerId)
            .createRule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo)));

    private final BatchRule<Customer, List<OrderItem>> orderItemBatchRule = withIdResolver(Customer::customerId)
            .createRule(OrderItem::customerId, oneToMany(OrderItem::id, cachedMany(this::getAllOrders)));

    List<Customer> customers = List.of(customer1, customer2, customer3);

    private Flux<BillingInfo> getBillingInfo(List<Customer> customers) {
        return Flux.just(billingInfo1, billingInfo2, billingInfo3);
    }

    private Flux<OrderItem> getAllOrders(List<Customer> customers) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22, orderItem31, orderItem32, orderItem33);
    }

    private Mono<Map<Customer, BillingInfo>> billingInfo(List<Customer> customers) {
        return billingInfoBatchRule.toMono(customers);
    }

    private Mono<Map<Customer, List<OrderItem>>> orderItems(List<Customer> customers) {
        return orderItemBatchRule.toMono(customers);
    }

    private Flux<BillingInfo> billingInfoFlux(List<Customer> customers) {
        return billingInfoBatchRule.toFlux(customers);
    }

    private Flux<List<OrderItem>> orderItemsFlux(List<Customer> customers) {
        return orderItemBatchRule.toFlux(customers);
    }

    @Test
    public void testBatchRuleToMono() {

        StepVerifier.create(billingInfo(customers))
                .expectSubscription()
                .expectNext(Map.of(
                        customer1, billingInfo1,
                        customer2, billingInfo2,
                        customer3, billingInfo3))
                .expectComplete()
                .verify();

        StepVerifier.create(orderItems(customers))
                .expectSubscription()
                .expectNext(Map.of(
                        customer1, List.of(orderItem11, orderItem12, orderItem13),
                        customer2, List.of(orderItem21, orderItem22),
                        customer3, List.of(orderItem31, orderItem32, orderItem33)))
                .expectComplete()
                .verify();
    }

    @Test
    public void testBatchRuleToFlux() {

        StepVerifier.create(billingInfoFlux(customers))
                .expectSubscription()
                .expectNext(billingInfo1, billingInfo2, billingInfo3)
                .expectComplete()
                .verify();

        StepVerifier.create(orderItemsFlux(customers))
                .expectSubscription()
                .expectNext(
                        List.of(orderItem11, orderItem12, orderItem13),
                        List.of(orderItem21, orderItem22),
                        List.of(orderItem31, orderItem32, orderItem33))
                .expectComplete()
                .verify();
    }
}
