package io.github.pellse.assembler;

public record BillingInfo(Long id, Long customerId, String creditCardNumber) {

    public BillingInfo(Long customerId) {
        this(null, customerId);
    }

    public BillingInfo(Long id, Long customerId) {
        this(id, customerId, null);
    }
}
