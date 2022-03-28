package io.github.pellse.assembler;

import java.util.List;

public record Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {
}
