# assembler-core

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-core)

## Usage Example

For synchronous Java 8 Stream implementation:
```java
List<Transaction> transactions = assemblerOf(Transaction.class)
    .fromSourceSupplier(this::getCustomers, Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // supplier of default values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .assembleUsing(streamAdapter())
    .collect(toList());
```
To switch to a parallel Java 8 Stream implementation and asynchronous aggregation, just set the `parallel` flag to `true` on the supplied `StreamAdapter` :
```java
List<Transaction> transactions = assemblerOf(Transaction.class)
    .fromSourceSupplier(this::getCustomers, Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // supplier of default values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .assembleUsing(streamAdapter(true))
    .collect(toList());
```
For `CompletableFuture` implementation:
```java
CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
    .fromSourceSupplier(this::getCustomers, Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId)
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .assembleUsing(completableFutureAdapter());
```
