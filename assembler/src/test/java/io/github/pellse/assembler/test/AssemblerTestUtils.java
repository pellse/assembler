/*
 * Copyright 2023 Sebastien Pelletier
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

import io.github.pellse.assembler.util.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface AssemblerTestUtils {

    BillingInfo billingInfo1 = new BillingInfo(1L, 1L, "4540977822220971");
    BillingInfo billingInfo2 = new BillingInfo(2L, 2L, "4530987722349872");
    BillingInfo billingInfo2Unknown = new BillingInfo(null, 2L);
    BillingInfo billingInfo3 = new BillingInfo(4L, 3L, "4540987722211234");

    OrderItem orderItem11 = new OrderItem("1", 1L, "Sweater", 19.99);
    OrderItem orderItem12 = new OrderItem("2", 1L, "Pants", 39.99);
    OrderItem orderItem13 = new OrderItem("3", 1L, "Socks", 9.99);

    OrderItem orderItem21 = new OrderItem("4", 2L, "Shoes", 79.99);
    OrderItem orderItem22 = new OrderItem("5", 2L, "Boots", 99.99);

    OrderItem orderItem31 = new OrderItem("7", 3L, "Gloves", 5.99);
    OrderItem orderItem32 = new OrderItem("8", 3L, "Dress", 45.99);
    OrderItem orderItem33 = new OrderItem("9", 3L, "Sneakers", 119.99);

    Customer customer1 = new Customer(1L, "Clair Gabriel");
    Customer customer2 = new Customer(2L, "Erick Daria");
    Customer customer3 = new Customer(3L, "Brenden Jacob");

    Transaction transaction2WithNullBillingInfo = new Transaction(customer2, null,
            List.of(orderItem21, orderItem22));

    Transaction transaction1 = new Transaction(customer1, billingInfo1,
            List.of(orderItem11, orderItem12, orderItem13));
    Transaction transaction2 = new Transaction(customer2, billingInfo2Unknown,
            List.of(orderItem21, orderItem22));
    Transaction transaction3 = new Transaction(customer3, billingInfo3, emptyList());

    TransactionSet transactionSet1 = new TransactionSet(customer1, billingInfo1,
            Set.of(orderItem11, orderItem12, orderItem13));
    TransactionSet transactionSet2 = new TransactionSet(customer2, billingInfo2Unknown,
            Set.of(orderItem21, orderItem22));
    TransactionSet transactionSet3 = new TransactionSet(customer3, billingInfo3, emptySet());

    static Publisher<BillingInfo> getBillingInfo(List<Long> customerIds) {
        return Flux.just(billingInfo1, billingInfo3)
                .filter(billingInfo -> customerIds.contains(billingInfo.customerId()));
    }

    static Publisher<BillingInfo> getBillingInfoWithSetIds(Set<Long> customerIds) {
        return Flux.just(billingInfo1, null, billingInfo3)
                .filter(billingInfo -> billingInfo == null || customerIds.contains(billingInfo.customerId()));
    }

    static Publisher<OrderItem> getAllOrders(List<Long> customerIds) {
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()));
    }

    static Publisher<OrderItem> getAllOrdersWithLinkedListIds(LinkedList<Long> customerIds) {
        //throw new SQLException("Exception in queryDatabaseForAllOrders");
        return Flux.just(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.customerId()));
    }

    static <R> Publisher<R> throwSQLException(List<Customer> customerIds) {
        // throw new IOException("Unable to query database");
        return Flux.error(new SQLException("Unable to query database"));
    }

    static Publisher<BillingInfo> errorBillingInfos(List<Customer> customers) {
        // throw new IOException("Unable to query database");
        return Flux.error(new SQLException("Unable to query database"));
    }

    static Publisher<OrderItem> errorOrderItems(List<Customer> customerIds) {
        // throw new IOException("Unable to query database");
        return Flux.error(new SQLException("Unable to query database"));
    }
}
