# assembler-reactive-stream-operators

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-reactive-stream-operators.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-reactive-stream-operators%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-reactive-stream-operators.svg)](http://javadoc.io/doc/io.github.pellse/assembler-reactive-stream-operators)

## Usage Example

```java
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;

import static io.github.pellse.assembler.microprofile.PublisherBuilderAdapter.publisherBuilderAdapter;

Assembler<Customer, PublisherBuilder<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
        Transaction::new)
    .using(publisherBuilderAdapter());

Flux<Transaction> transactionFlux = Flux.fromIterable(getCustomers())
    .buffer(3)
    .concatMap(customers -> assembler.assemble(customers)
        .filter(transaction -> transaction.getOrderItems().size() > 1)
        .buildRs());
```
In the above example we create an `Assembler` that returns an Eclipse MicroProfile Reactive Stream Operator [PublisherBuilder](https://download.eclipse.org/microprofile/microprofile-reactive-streams-operators-1.0/apidocs/org/eclipse/microprofile/reactive/streams/operators/PublisherBuilder.html), we can then see the integration with other Reactive Stream implementations, Project Reactor in this example, everything inside the Project Reactor's `Flux ` `concatMap()` method is Eclipse MicroProfile Reactive Stream Operator code, the `buildRs()` method returns a standardized [org.reactivestreams.Publisher](https://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/Publisher.html) which can then act as an integration point with 3rd party libraries or be used standalone in [Eclipse MicroProfile](https://microprofile.io/) compliant code.
