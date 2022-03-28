package io.github.pellse.assembler;

import java.util.Set;

public record TransactionSet(Customer customer, BillingInfo billingInfo, Set<OrderItem> orderItems) { }
