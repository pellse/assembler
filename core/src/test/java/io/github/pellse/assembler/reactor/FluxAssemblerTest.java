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

package io.github.pellse.assembler.reactor;

import io.github.pellse.assembler.core.CoreAssembler;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static reactor.core.publisher.Flux.interval;
import static reactor.core.publisher.Flux.zip;

public class FluxAssemblerTest {

    private BillingInfo billingInfo1 = new BillingInfoProjection(1L, "4540977822220971");
    private BillingInfo address2 = new BillingInfoProjection(2L, "4530987722349872");
    private BillingInfo billingInfo2Unknown = new BillingInfoProjection(2L, "unknown");
    private BillingInfo billingInfo3 = new BillingInfoProjection(3L, "4540987722211234");

    private OrderItem orderItem11 = new OrderItemProjection(1L, "Sweater", 19.99);
    private OrderItem orderItem12 = new OrderItemProjection(1L, "Pants", 39.99);
    private OrderItem orderItem13 = new OrderItemProjection(1L, "Socks", 9.99);

    private OrderItem orderItem21 = new OrderItemProjection(2L, "Shoes", 79.99);
    private OrderItem orderItem22 = new OrderItemProjection(2L, "Boots", 99.99);

    private Customer customer1 = new CustomerProjection(1L, "Clair Gabriel");
    private Customer customer2 = new CustomerProjection(2L, "Erick Daria");
    private Customer customer3 = new CustomerProjection(3L, "Brenden Jacob");

    private Transaction transaction1 = new SimpleTransaction(customer1, billingInfo1, List.of(orderItem11,
            orderItem12, orderItem13));
    private Transaction transaction2 = new SimpleTransaction(customer2, billingInfo2Unknown, List.of(orderItem21,
            orderItem22));
    private Transaction transaction3 = new SimpleTransaction(customer3, billingInfo3, emptyList());

    @Test
    public void testAssembleWithFlux() throws Exception {

        List<Customer> customerList = asList(customer1, customer2, customer3);

        Flux<Customer> customerFlux = Flux.generate(() -> 0, (index, sink) -> {
            sink.next(customerList.get(index));
            return ++index % 3;
        });
        Flux<Customer> intervalCustomerFlux = zip(customerFlux, interval(ofMillis(200)), (cust, l) -> cust);

        intervalCustomerFlux
                .log()
                //.windowTimeout(3, ofMillis(200))
                .bufferTimeout(3, ofMillis(200))
                .flatMap(customers -> FluxAssembler.of(customers, Customer::getCustomerId)
                        .assemble(
                                oneToOne(this::queryDatabaseForBillingInfos, BillingInfo::getCustomerId),
                                oneToManyAsList(this::queryDatabaseForAllOrders, OrderItem::getCustomerId),
                                SimpleTransaction::new))
                .doOnError(e -> System.out.println("error: " + e))
                .subscribe(e -> System.out.println(Thread.currentThread().getName() + ", " + e));

        Thread.sleep(500000);
    }

    private List<BillingInfo> queryDatabaseForBillingInfos(List<Long> customerIds) throws SQLException {
        return Stream.of(billingInfo1, null, billingInfo3)
                .filter(adr -> adr == null || customerIds.contains(adr.getCustomerId()))
                .collect(toList());
    }

    public List<OrderItem> queryDatabaseForAllOrders(List<Long> customerIds) throws SQLException {
        //throw new SQLException("Exception in queryDatabaseForAllOrders");
        return Stream.of(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.getCustomerId()))
                .collect(toList());
    }

    interface Transaction {
        Customer getCustomer();

        BillingInfo getBillingInfo();

        List<OrderItem> getOrderItems();
    }

    interface Customer {
        Long getCustomerId();

        String getName();

        List<OrderItem> getOrders();
    }

    interface BillingInfo {
        Long getCustomerId();

        String getCreditCardNumber();
    }

    interface OrderItem {
        Long getCustomerId();

        String getOrderDescription();

        Double getPrice();
    }

    class SimpleTransaction implements Transaction {

        private final Customer customer;
        private final BillingInfo billingInfo;
        private final List<OrderItem> orderItems;

        public SimpleTransaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {
            this.customer = customer;
            this.billingInfo = billingInfo;
            this.orderItems = orderItems;
        }

        @Override
        public Customer getCustomer() {
            return customer;
        }

        @Override
        public BillingInfo getBillingInfo() {
            return billingInfo;
        }

        @Override
        public List<OrderItem> getOrderItems() {
            return orderItems;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleTransaction that = (SimpleTransaction) o;
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
            return "SimpleTransaction [\ncustomer=" + customer + "\n    billingInfo=" + billingInfo +
                    "\n    orderItems=" + orderItems + "]";
        }
    }

    class CustomerProjection implements Customer {

        private final Long customerId;
        private final String name;

        public CustomerProjection(Long customerId, String name) {
            this.customerId = customerId;
            this.name = name;
        }

        @Override
        public Long getCustomerId() {
            return customerId;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public List<OrderItem> getOrders() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomerProjection that = (CustomerProjection) o;
            return Objects.equals(customerId, that.customerId) &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, name);
        }

        @Override
        public String toString() {
            return "CustomerProjection [customerId=" + customerId + ", name=" + name + "]";
        }
    }

    class BillingInfoProjection implements BillingInfo {

        private final Long customerId;
        private final String creditCardNumber;

        public BillingInfoProjection(Long customerId, String creditCardNumber) {
            this.customerId = customerId;
            this.creditCardNumber = creditCardNumber;
        }

        @Override
        public Long getCustomerId() {
            return customerId;
        }

        @Override
        public String getCreditCardNumber() {
            return creditCardNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BillingInfoProjection that = (BillingInfoProjection) o;
            return Objects.equals(customerId, that.customerId) &&
                    Objects.equals(creditCardNumber, that.creditCardNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customerId, creditCardNumber);
        }

        @Override
        public String toString() {
            return "BillingInfoProjection [customerId=" + customerId + ", creditCardNumber=" + creditCardNumber + "]";
        }
    }

    class OrderItemProjection implements OrderItem {

        private final Long customerId;
        private final String orderDescription;
        private final Double price;

        public OrderItemProjection(Long customerId, String orderDescription, Double price) {
            this.customerId = customerId;
            this.orderDescription = orderDescription;
            this.price = price;
        }

        @Override
        public Long getCustomerId() {
            return customerId;
        }

        @Override
        public String getOrderDescription() {
            return orderDescription;
        }

        @Override
        public Double getPrice() {
            return price;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderItemProjection that = (OrderItemProjection) o;
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
            return orderDescription + ", " + price;
            //return "OrderItemProjection [customerId=" + customerId + ", orderDescription=" + orderDescription + ", " +
            //        "price=" + price + "]";
        }
    }
}
