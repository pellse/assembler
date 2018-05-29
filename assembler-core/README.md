# assembler-core

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-core)

## Usage Example

For synchronous implementation:
```java
List<Transaction> transactions = assemblerOf(Transaction.class)
    .fromSupplier(this::getCustomers, Customer::getCustomerId)
    .assembleWith(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // supplier of default values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(synchronousAdapter())
    .collect(toList());
```
For `CompletableFuture` implementation:
```java
CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
    .fromSupplier(this::getCustomers, Customer::getCustomerId)
    .assembleWith(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId)
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(completableFutureAdapter());
```
