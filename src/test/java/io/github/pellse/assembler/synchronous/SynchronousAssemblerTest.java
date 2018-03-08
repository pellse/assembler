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

import io.github.pellse.util.function.checked.CheckedSupplier;
import io.github.pellse.util.function.checked.UncheckedException;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.github.pellse.assembler.synchronous.SynchronousAssembler.entityAssembler;
import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static io.github.pellse.util.query.Mapper.oneToManyMapping;
import static io.github.pellse.util.query.Mapper.oneToManyMappingAsList;
import static io.github.pellse.util.query.Mapper.oneToOneMapping;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Sebastien Pelletier
 */
public class SynchronousAssemblerTest {

    private BillingInfo billingInfo1 = new BillingInfo(1L, "4540977822220971");
    private BillingInfo billingInfo2 = new BillingInfo(2L, "4530987722349872");
    private BillingInfo billingInfo2Unknown = new BillingInfo(2L);
    private BillingInfo billingInfo3 = new BillingInfo(3L, "4540987722211234");

    private OrderItem orderItem11 = new OrderItem(1L, "Sweater", 19.99);
    private OrderItem orderItem12 = new OrderItem(1L, "Pants", 39.99);
    private OrderItem orderItem13 = new OrderItem(1L, "Socks", 9.99);

    private OrderItem orderItem21 = new OrderItem(2L, "Shoes", 79.99);
    private OrderItem orderItem22 = new OrderItem(2L, "Boots", 99.99);

    private Customer customer1 = new Customer(1L, "Clair Gabriel");
    private Customer customer2 = new Customer(2L, "Erick Daria");
    private Customer customer3 = new Customer(3L, "Brenden Jacob");

    private Transaction transaction1 = new Transaction(customer1, billingInfo1, List.of(orderItem11, orderItem12,
            orderItem13));
    private Transaction transaction2 = new Transaction(customer2, billingInfo2Unknown, List.of(orderItem21,
            orderItem22));
    private Transaction transaction2WithNullBillingInfo = new Transaction(customer2, null, List.of(orderItem21,
            orderItem22));
    private Transaction transaction3 = new Transaction(customer3, billingInfo3, emptyList());

    @Test
    public void testAssembleWithBuilder() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = AssemblerBuilder.<Customer, Long, List<Customer>, List<Long>>builder()
                .entitiesProvider(customerProvider)
                .entityIdExtractor(Customer::getCustomerId)
                .idCollectionFactory(LinkedList::new)
                .errorHandler(e -> sneakyThrow(new UncheckedException(e)))
                .build()
                .assemble(
                        oneToOneMapping(this::queryDatabaseForBillingInfos, BillingInfo::getCustomerId,
                                BillingInfo::new),
                        oneToManyMappingAsList(this::queryDatabaseForAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(asList(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssemble() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = entityAssembler(customerProvider, Customer::getCustomerId)
                .assemble(
                        oneToOneMapping(this::queryDatabaseForBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyMappingAsList(this::queryDatabaseForAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2, transaction3)));
    }

    @Test
    public void testAssembleWithNullBillingInfo() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = entityAssembler(customerProvider, Customer::getCustomerId, ArrayList::new,
                this::throwUncheckedException)
                .assemble(
                        oneToOneMapping(this::queryDatabaseForBillingInfos, BillingInfo::getCustomerId),
                        oneToManyMapping(this::queryDatabaseForAllOrders, OrderItem::getCustomerId, ArrayList::new),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }

    @Test(expected = UncheckedException.class)
    public void testAssembleWithUncheckedException() {

        CheckedSupplier<List<Customer>, Throwable> customerProvider = () -> List.of(customer1, customer2, customer3);

        List<Transaction> transactions = entityAssembler(customerProvider, Customer::getCustomerId, ArrayList::new,
                this::throwUncheckedException)
                .assemble(
                        oneToOneMapping(this::queryDatabaseForBillingInfosAndThrow, BillingInfo::getCustomerId),
                        oneToManyMappingAsList(this::queryDatabaseForAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .collect(toList());

        assertThat(transactions, equalTo(List.of(transaction1, transaction2WithNullBillingInfo, transaction3)));
    }

    private <R> R throwUncheckedException(Throwable t) {
        return sneakyThrow(new UncheckedException(t));
    }

    private List<BillingInfo> queryDatabaseForBillingInfosAndThrow(List<Long> customerIds) throws SQLException {
        throw new SQLException("Unable to query database");
    }

    private List<BillingInfo> queryDatabaseForBillingInfos(List<Long> customerIds) throws SQLException {
        return Stream.of(billingInfo1, null, billingInfo3)
                .filter(adr -> adr == null || customerIds.contains(adr.getCustomerId()))
                .collect(toList());
    }

    private List<OrderItem> queryDatabaseForAllOrders(List<Long> customerIds) throws SQLException {
        //throw new SQLException("Throwable in queryDatabaseForAllOrders");
        return Stream.of(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.getCustomerId()))
                .collect(toList());
    }

    final class Transaction {

        private final Customer customer;
        private final BillingInfo billingInfo;
        private final List<OrderItem> orderItems;

        public Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {
            this.customer = customer;
            this.billingInfo = billingInfo;
            this.orderItems = orderItems;
        }

        public Customer getCustomer() {
            return customer;
        }

        public BillingInfo getBillingInfo() {
            return billingInfo;
        }

        public List<OrderItem> getOrderItems() {
            return orderItems;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Transaction that = (Transaction) o;
            return Objects.equals(customer, that.customer) &&
                    Objects.equals(billingInfo, that.billingInfo) &&
                    Objects.equals(orderItems, that.orderItems);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customer, billingInfo, orderItems);
        }

        @Override
        public String toString() {
            return "Transaction [customer=" + customer + ", billingInfo=" + billingInfo + ", orderItems=" +
                    orderItems + "]";
        }
    }

    final class Customer {

        private final Long customerId;
        private final String name;

        public Customer(Long customerId, String name) {
            this.customerId = customerId;
            this.name = name;
        }

        public Long getCustomerId() {
            return customerId;
        }

        public String getName() {
            return name;
        }

        public List<OrderItem> getOrders() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Customer that = (Customer) o;
            return Objects.equals(customerId, that.customerId) &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, name);
        }

        @Override
        public String toString() {
            return "Customer [customerId=" + customerId + ", name=" + name + "]";
        }
    }

    final class BillingInfo {

        private static final String UNKNOWN = "unknown";

        private final Long customerId;
        private final String creditCardNumber;

        public BillingInfo(Long customerId) {
            this(customerId, UNKNOWN);
        }

        public BillingInfo(Long customerId, String creditCardNumber) {
            this.customerId = customerId;
            this.creditCardNumber = creditCardNumber;
        }

        public Long getCustomerId() {
            return customerId;
        }

        public String getCreditCardNumber() {
            return creditCardNumber;
        }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BillingInfo that = (BillingInfo) o;
            return Objects.equals(customerId, that.customerId) &&
                    Objects.equals(creditCardNumber, that.creditCardNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, creditCardNumber);
        }

        @Override
        public String toString() {
            return "BillingInfo [customerId=" + customerId + ", creditCardNumber=" + creditCardNumber + "]";
        }
    }

    final class OrderItem {

        private final Long customerId;
        private final String orderDescription;
        private final Double price;

        public OrderItem(Long customerId, String orderDescription, Double price) {
            this.customerId = customerId;
            this.orderDescription = orderDescription;
            this.price = price;
        }

        public Long getCustomerId() {
            return customerId;
        }

        public String getOrderDescription() {
            return orderDescription;
        }

        public Double getPrice() {
            return price;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderItem that = (OrderItem) o;
            return Objects.equals(customerId, that.customerId) &&
                    Objects.equals(orderDescription, that.orderDescription) &&
                    Objects.equals(price, that.price);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, orderDescription, price);
        }

        @Override
        public String toString() {
            return "OrderItem [customerId=" + customerId + ", orderDescription=" + orderDescription + ", " +
                    "price=" + price + "]";
        }
    }
}
