package io.github.pellse.assembler;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BillingInfo {
    private static final String UNKNOWN = "unknown";

    private final Long customerId;
    private final String creditCardNumber;

    public BillingInfo(Long customerId) {
        this(customerId, UNKNOWN);
    }
}