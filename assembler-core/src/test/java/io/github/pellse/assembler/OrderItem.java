package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderItem {
    private final Long customerId;
    private final String orderDescription;
    private final Double price;
}
