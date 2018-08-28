# assembler-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-core%22)

## Usage Example

For synchronous Java 8 Stream implementation:
```java
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.assembler.stream.StreamAdapter.streamAdapter;

List<Transaction> transactions = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // Default BillingInfo for null values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(streamAdapter())
    .assembleFromSupplier(this::getCustomers)
    .collect(toList());
```
To switch to a parallel Java 8 Stream implementation and asynchronous aggregation, just set the `parallel` flag to `true` on the supplied `StreamAdapter` :
```java
List<Transaction> transactions = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // Default BillingInfo for null values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(streamAdapter(true))
    .assembleFromSupplier(this::getCustomers)
    .collect(toList());
```
For `CompletableFuture` implementation:
```java
import static io.github.pellse.assembler.future.CompletableFutureAdapter.completableFutureAdapter;

CompletableFuture<List<Transaction>> transactions = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(completableFutureAdapter())
    .assembleFromSupplier(this::getCustomers);
```
