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
import io.github.pellse.util.query.Mapper;
import org.junit.Test;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.stream.StreamAdapter.streamAdapter;
import static io.github.pellse.util.query.MapperUtils.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Sebastien Pelletier
 */
public class StreamAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3);
    }

    @Test
    public void testAssembleBuilder() {

        List<Transaction> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId),
                        Transaction::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleBuilderWithCustomMapFactory() {

        List<Transaction> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new, ids -> new TreeMap<>()),
                        oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId, ids -> new LinkedHashMap<>(ids.size() * 2)),
                        Transaction::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleBuilderWithNullEntityIds() {

        List<Transaction> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(ids -> null, BillingInfo::getCustomerId),
                        oneToManyAsList(ids -> null, OrderItem::getCustomerId),
                        Transaction::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(
                new Transaction(customer1, null, List.of()),
                new Transaction(customer2, null, List.of()),
                new Transaction(customer3, null, List.of()))));
    }

    @Test(expected = UncheckedException.class)
    public void testAssembleBuilderWithUncheckedException() {

        assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::getCustomerId),
                        Transaction::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers);
    }

    @Test(expected = UserDefinedRuntimeException.class)
    public void testAssembleBuilderWithCustomException() {

        assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::getCustomerId),
                        Transaction::new)
                .withErrorConverter(UserDefinedRuntimeException::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers);
    }

    @Test
    public void testAssembleBuilderWithNonListIds() {

        List<TransactionSet> transactions = assemblerOf(TransactionSet.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfoForCustomersWithSetIds, BillingInfo::getCustomerId, BillingInfo::new, HashSet::new),
                        oneToManyAsSet(AssemblerTestUtils::getAllOrdersForCustomersWithLinkedListIds, OrderItem::getCustomerId, LinkedList::new),
                        TransactionSet::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transactionSet1, transactionSet2, transactionSet3)));
    }

    @Test
    public void testAssembleBuilderWithNullBillingInfo() {

        List<Transaction> transactions = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId),
                        oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId),
                        Transaction::new)
                .using(streamAdapter())
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }

    @Test
    public void testAssembleBuilderWithCachedMappers() {

        Mapper<Long, BillingInfo, SQLException> billingInfoMapper = cached(oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId));
        Mapper<Long, List<OrderItem>, SQLException> allOrdersMapper = oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId);

        Assembler<Customer, Stream<Transaction>> transactionAssembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(billingInfoMapper, allOrdersMapper, Transaction::new)
                .using(streamAdapter());

        List<Transaction> transactionList1 = transactionAssembler
                .assembleFromSupplier(this::getCustomers)
                .collect(toList());// Will invoke the remote MongoDB getBillingInfoForCustomers() call

        List<Transaction> transactionList2 = transactionAssembler
                .assembleFromSupplier(this::getCustomers)
                .collect(toList()); // Will reuse the results returned from the first invocation of getBillingInfoForCustomers() above

        assertThat(transactionList2, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }
}
