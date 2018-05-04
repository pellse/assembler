/*
 * Copyright 2017 Sebastien Pelletier
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

package io.github.pellse.assembler.synchronous;

import io.github.pellse.assembler.AssemblerTestUtils;
import io.github.pellse.assembler.BillingInfo;
import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static io.github.pellse.util.query.MapperUtils.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Sebastien Pelletier
 */
public class SynchronousAssemblerTest {

    @Test
    public void testAssemble() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = SynchronousAssembler.of(customerProvider, Customer::getCustomerId)
                .assemble(
                        oneToOne(AssemblerTestUtils::queryBillingInfoForAllCustomersIn, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::queryAllOrdersForAllCustomersIn, OrderItem::getCustomerId),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleWithNullBillingInfo() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = SynchronousAssembler.of(customerProvider, Customer::getCustomerId)
                .assemble(
                        oneToOne(AssemblerTestUtils::queryBillingInfoForAllCustomersIn, BillingInfo::getCustomerId),
                        oneToMany(AssemblerTestUtils::queryAllOrdersForAllCustomersIn, OrderItem::getCustomerId, ArrayList::new),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }

    @Test(expected = UncheckedException.class)
    public void testAssembleWithUncheckedException() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = SynchronousAssembler.of(customerProvider, Customer::getCustomerId)
                .assemble(
                        oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::getCustomerId),
                        oneToManyAsList(AssemblerTestUtils::queryAllOrdersForAllCustomersIn, OrderItem::getCustomerId),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }
}
