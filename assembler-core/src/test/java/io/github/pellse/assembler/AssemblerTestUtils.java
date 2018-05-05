package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class AssemblerTestUtils {

    public static final BillingInfo billingInfo1 = new BillingInfo(1L, "4540977822220971");
    public static final BillingInfo address2 = new BillingInfo(2L, "4530987722349872");
    public static final BillingInfo billingInfo2Unknown = new BillingInfo(2L, "unknown");
    public static final BillingInfo billingInfo3 = new BillingInfo(3L, "4540987722211234");

    public static final OrderItem orderItem11 = new OrderItem(1L, "Sweater", 19.99);
    public static final OrderItem orderItem12 = new OrderItem(1L, "Pants", 39.99);
    public static final OrderItem orderItem13 = new OrderItem(1L, "Socks", 9.99);

    public static final OrderItem orderItem21 = new OrderItem(2L, "Shoes", 79.99);
    public static final OrderItem orderItem22 = new OrderItem(2L, "Boots", 99.99);

    public static final Customer customer1 = new Customer(1L, "Clair Gabriel");
    public static final Customer customer2 = new Customer(2L, "Erick Daria");
    public static final Customer customer3 = new Customer(3L, "Brenden Jacob");

    public static final Transaction transaction2WithNullBillingInfo = new Transaction(customer2, null,
            List.of(orderItem21, orderItem22));

    public static final Transaction transaction1 = new Transaction(customer1, billingInfo1,
            List.of(orderItem11, orderItem12, orderItem13));
    public static final Transaction transaction2 = new Transaction(customer2, billingInfo2Unknown,
            List.of(orderItem21, orderItem22));
    public static final Transaction transaction3 = new Transaction(customer3, billingInfo3, emptyList());

    public static List<BillingInfo> getBillingInfoForCustomers(List<Long> customerIds) throws SQLException {
        return Stream.of(billingInfo1, null, billingInfo3)
                .filter(adr -> adr == null || customerIds.contains(adr.getCustomerId()))
                .collect(toList());
    }

    public static List<OrderItem> getAllOrdersForCustomers(List<Long> customerIds) throws SQLException {
        //throw new SQLException("Exception in queryDatabaseForAllOrders");
        return Stream.of(orderItem11, orderItem12, orderItem13, orderItem21, orderItem22)
                .filter(orderItem -> customerIds.contains(orderItem.getCustomerId()))
                .collect(toList());
    }

    public static <R> List<R> throwSQLException(List<Long> customerIds) throws SQLException {
        throw new SQLException("Unable to query database");
    }

    @Data
    @AllArgsConstructor
    public static class Transaction {
        private final Customer customer;
        private final BillingInfo billingInfo;
        private final List<OrderItem> orderItems;
    }

    @Data
    @AllArgsConstructor
    public static class Customer {
        private final Long customerId;
        private final String name;
    }

    @Data
    @AllArgsConstructor
    public static class BillingInfo {
        private static final String UNKNOWN = "unknown";

        private final Long customerId;
        private final String creditCardNumber;

        public BillingInfo(Long customerId) {
            this(customerId, UNKNOWN);
        }
    }

    @Data
    @AllArgsConstructor
    public static class OrderItem {
        private final Long customerId;
        private final String orderDescription;
        private final Double price;
    }
}
