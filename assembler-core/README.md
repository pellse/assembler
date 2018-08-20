# assembler-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-core%22)

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
