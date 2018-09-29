package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Customer {
    private final Long customerId;
    private final String name;
}