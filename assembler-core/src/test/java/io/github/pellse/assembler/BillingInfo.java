package io.github.pellse.assembler;

public record BillingInfo(Long customerId, String creditCardNumber) {
    private static final String UNKNOWN = "unknown";

    public BillingInfo(Long customerId) {
        this(customerId, UNKNOWN);
    }
}
