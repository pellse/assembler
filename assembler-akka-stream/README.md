# assembler-akka-stream

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-akka-stream.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-akka-stream%22)

## Usage Example

By using an `AkkaSourceAdapter` we can support the [Akka Stream](https://akka.io/) framework by creating instances of Akka `Source`:
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Source<Transaction, NotUsed> transactionSource = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(akkaSourceAdapter()))
    .assembleFromSupplier(this::getCustomers); // Sequential

transactionSource.runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```
or
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Assembler<Customer, Source<Transaction, NotUsed>> assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(akkaSourceAdapter(true)); // Parallel

Source<Transaction, NotUsed> transactionSource = Source.from(getCustomers())
    .groupedWithin(3, ofSeconds(5))
    .flatMapConcat(assembler::assemble)

transactionSource.runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```
It is also possible to create an Akka `Flow` from the Assembler DSL:
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Assembler<Customer, Source<Transaction, NotUsed>> assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(akkaSourceAdapter(Source::async)); // Custom underlying sources configuration

Source<Customer, NotUsed> customerSource = Source.from(getCustomers());

Flow<Customer, Transaction, NotUsed> transactionFlow = Flow.<Customer>create()
    .grouped(3)
    .flatMapConcat(assembler::assemble);
        
customerSource.via(transactionFlow)
    .runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```
