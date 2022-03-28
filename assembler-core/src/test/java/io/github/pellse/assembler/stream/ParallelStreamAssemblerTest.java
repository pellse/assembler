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

package io.github.pellse.assembler.stream;

import io.github.pellse.assembler.*;
import io.github.pellse.util.function.checked.UncheckedException;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.stream.StreamAdapter.streamAdapter;
import static io.github.pellse.util.query.MapperUtils.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Sebastien Pelletier
 */
public class ParallelStreamAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3);
    }

    @Test
    public void testAssembleBuilder() {

        List<Transaction> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(streamAdapter(true))
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleBuilderWithException() {

        assertThrows(UncheckedException.class, () -> assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::customerId),
                        Transaction::new)
                .using(streamAdapter(true))
                .assembleFromSupplier(this::getCustomers));
    }

    @Test
    public void testAssembleBuilderWithLinkedListIds() {

        List<TransactionSet> transactions = assemblerOf(TransactionSet.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfosWithSetIds, BillingInfo::customerId, BillingInfo::new, HashSet::new),
                        oneToManyAsSet(AssemblerTestUtils::getAllOrdersWithLinkedListIds, OrderItem::customerId, LinkedList::new),
                        TransactionSet::new)
                .using(streamAdapter(true))
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transactionSet1, transactionSet2, transactionSet3)));
    }
}
