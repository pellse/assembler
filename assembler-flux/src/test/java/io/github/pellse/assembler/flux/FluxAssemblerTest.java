/*
 * Copyright 2018 Sebastien Pelletier
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

package io.github.pellse.assembler.flux;

import io.github.pellse.assembler.*;
import io.github.pellse.util.function.checked.UncheckedException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.flux.FluxAdapter.fluxAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.util.Arrays.asList;

public class FluxAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3, customer1, customer2);
    }

    @Test
    public void testAssemblerBuilderWithFlux() {

        StepVerifier.create(
                assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::customerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                                Transaction::new)
                        .using(fluxAdapter())
                        .assembleFromSupplier(this::getCustomers))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2)
                .expectComplete()
                .verify();
    }

    @Test
    public void testAssemblerBuilderWithFluxWithError() {

        StepVerifier.create(
                assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::customerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::customerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::customerId),
                                Transaction::new)
                        .using(fluxAdapter())
                        .assembleFromSupplier(this::getCustomers))
                .expectSubscription()
                .expectError(UncheckedException.class)
                .verify();
    }

    @Test
    public void testAssemblerBuilderWithFluxWithBuffering() {

        StepVerifier.create(Flux.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(customers -> assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::customerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                                Transaction::new)
                        .using(fluxAdapter())
                        .assemble(customers)))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReusableAssemblerBuilderWithFluxWithBuffering() {

        Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(fluxAdapter());

        StepVerifier.create(Flux.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(assembler::assemble))
                .expectSubscription()
                .expectNext(transaction1, transaction2, transaction3, transaction1, transaction2)
                .expectComplete()
                .verify();
    }
}
