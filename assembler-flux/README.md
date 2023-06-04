# assembler-flux

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-flux.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-flux%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-flux.svg)](http://javadoc.io/doc/io.github.pellse/assembler-flux)

## Usage Example

```java
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.assembler.flux.FluxAdapter.fluxAdapter;

Flux<Transaction> transactionFlux = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(fluxAdapter(elastic()))
    .assembleFromSupplier(this::getCustomers))
```
or by reusing the same `Assembler` instance as a transformation step within a `Flux`: 
```java
Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
         oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
         oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
         Transaction::new)
    .using(fluxAdapter()); // Parallel scheduler used by default

Flux<Transaction> transactionFlux = Flux.fromIterable(getCustomers()) // or just getCustomerFlux()
    .bufferTimeout(10, ofSeconds(5)) // batch every 5 seconds or max of 10 customers
    .flatMap(assembler::assemble)
```
