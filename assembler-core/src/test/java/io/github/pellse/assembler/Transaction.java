package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Transaction {
    private final Customer customer;
    private final BillingInfo billingInfo;
    private final List<OrderItem> orderItems;
}