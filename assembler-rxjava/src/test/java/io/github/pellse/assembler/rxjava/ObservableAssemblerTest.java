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

package io.github.pellse.assembler.rxjava;

import io.github.pellse.assembler.*;
import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.rxjava.ObservableAdapter.observableAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static io.reactivex.rxjava3.schedulers.Schedulers.single;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ObservableAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3, customer1, customer2);
    }

    @Test
    public void testAssemblerBuilderWithObservable() {

        Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(observableAdapter())
                .assembleFromSupplier(this::getCustomers);

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test
    public void testAssemblerBuilderWithErrorWithObservable() {

        assertThrows(UserDefinedRuntimeException.class, () -> {
            Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
                    .withIdExtractor(Customer::customerId)
                    .withAssemblerRules(
                            oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::customerId, BillingInfo::new),
                            oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::customerId),
                            Transaction::new)
                    .withErrorConverter(UserDefinedRuntimeException::new)
                    .using(observableAdapter(single()))
                    .assembleFromSupplier(this::getCustomers);

            transactionObservable.toList().blockingGet();
        });
    }

    @Test
    public void testAssemblerBuilderWithObservableWithBuffering() {

        Observable<Transaction> transactionObservable = Observable.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(customers -> assemblerOf(Transaction.class)
                        .withIdExtractor(Customer::customerId)
                        .withAssemblerRules(
                                oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                                Transaction::new)
                        .using(observableAdapter(single()))
                        .assemble(customers));

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test
    public void testReusableAssemblerBuilderWithObservableWithBuffering() {

        Assembler<Customer, Observable<Transaction>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::customerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfo, BillingInfo::customerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::customerId),
                        Transaction::new)
                .using(observableAdapter());

        Observable<Transaction> transactionObservable = Observable.fromIterable(getCustomers())
                .buffer(3)
                .concatMap(assembler::assemble);

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }
}
