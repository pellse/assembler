package io.github.pellse.assembler;

public record OrderItem(String id, Long customerId, String orderDescription, Double price) {
}
