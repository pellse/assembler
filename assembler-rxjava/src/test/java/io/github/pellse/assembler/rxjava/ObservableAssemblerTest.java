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

import io.github.pellse.assembler.AssemblerTestUtils;
import io.github.pellse.assembler.AssemblerTestUtils.*;
import io.reactivex.Observable;
import org.junit.Test;

import java.util.List;

import static io.github.pellse.assembler.Assembler.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.rxjava.ObservableAdapter.observableAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static io.reactivex.schedulers.Schedulers.single;
import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ObservableAssemblerTest {

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3, customer1, customer2);
    }

    @Test
    public void testAssemblerBuilderWithObservable() {

        Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
                .fromSupplier(this::getCustomers, Customer::getCustomerId)
                .assembleWith(
                        oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId),
                        Transaction::new)
                .using(observableAdapter(single()));

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test(expected = UserDefinedRuntimeException.class)
    public void testAssemblerBuilderWithErrorWithObservable() {

        Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
                .fromSupplier(this::getCustomers, Customer::getCustomerId)
                .assembleWith(
                        oneToOne(AssemblerTestUtils::throwSQLException, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::throwSQLException, OrderItem::getCustomerId),
                        Transaction::new)
                .withErrorConverter(UserDefinedRuntimeException::new)
                .using(observableAdapter(single()));

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }

    @Test
    public void testAssemblerBuilderWithObservableWithBuffering() {

        Observable<Transaction> transactionObservable = Observable.fromIterable(getCustomers())
                .buffer(3)
                .flatMap(customers -> assemblerOf(Transaction.class)
                        .from(customers, Customer::getCustomerId)
                        .assembleWith(
                                oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
                                oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId),
                                Transaction::new)
                        .using(observableAdapter(single())));

        assertThat(transactionObservable.toList().blockingGet(),
                equalTo(List.of(transaction1, transaction2, transaction3, transaction1, transaction2)));
    }
}
