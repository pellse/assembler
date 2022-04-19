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

package io.github.pellse.assembler.future;

import io.github.pellse.assembler.*;
import io.github.pellse.util.function.checked.UncheckedException;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.future.CompletableFutureAdapter.completableFutureAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Sebastien Pelletier
 */
public class CompletableFutureAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3);
    }

    @Test
    public void testAssembleBuilder() throws InterruptedException, ExecutionException {

        CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(completableFutureAdapter())
                .assembleFromSupplier(this::getCustomers);

        assertThat(transactions.get(), equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleBuilderWithException() {
        assertThrows(UncheckedException.class, () -> {
            CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
                    .withIdExtractor(Customer::customerId)
                    .withAssemblerRules(
                            oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::customerId, BillingInfo::new),
                            oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::customerId),
                            Transaction::new)
                    .using(completableFutureAdapter())
                    .assembleFromSupplier(this::getCustomers);

            try {
                transactions.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    public void testAssembleBuilderWithCustomExecutor() throws InterruptedException, ExecutionException {

        CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(completableFutureAdapter(newFixedThreadPool(2)))
                .assembleFromSupplier(this::getCustomers);

        assertThat(transactions.get(), equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleBuilderAsSet() throws InterruptedException, ExecutionException {

        CompletableFuture<? extends Set<Transaction>> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(completableFutureAdapter(HashSet::new, null))
                .assembleFromSupplier(this::getCustomers);

        assertThat(transactions.get(), equalTo(Set.of(transaction1, transaction2, transaction3)));
    }
}
