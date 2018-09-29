package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor
public class TransactionSet {
    private final Customer customer;
    private final BillingInfo billingInfo;
    private final Set<OrderItem> orderItems;
}
