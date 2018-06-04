# assembler-akka-stream

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-akka-stream/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.pellse/assembler-akka-stream)

## Usage Example

By using an `AkkaSourceAdapter` we can support the [Akka Stream](https://akka.io/) framework by creating instances of Akka `Source`:
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Source<Transaction, NotUsed> transactionSource = assemblerOf(Transaction.class)
    .fromSupplier(this::getCustomers, Customer::getCustomerId)
    .assembleWith(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(akkaSourceAdapter())); // Sequential

transactionSource.runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```
or
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Source<Transaction, NotUsed> transactionSource = Source.from(getCustomers())
    .groupedWithin(3, ofSeconds(5))
    .flatMapConcat(customerList ->
        assemblerOf(Transaction.class)
            .from(customerList, Customer::getCustomerId)
            .assembleWith(
                oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
                oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
                Transaction::new)
            .using(akkaSourceAdapter(true))); // Parallel

transactionSource.runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```

It is also possible to create an Akka `Flow` from the Assembler DSL:
```java
ActorSystem system = ActorSystem.create();
Materializer mat = ActorMaterializer.create(system);

Source<Customer, NotUsed> customerSource = Source.from(getCustomers());

Flow<Customer, Transaction, NotUsed> transactionFlow = Flow.<Customer>create()
    .grouped(3)
    .flatMapConcat(customerList ->
        assemblerOf(Transaction.class)
            .from(customerList, Customer::getCustomerId)
            .assembleWith(
                oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
                oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
                Transaction::new)
            .using(akkaSourceAdapter(Source::async))); // Custom underlying sources configuration
        
customerSource.via(transactionFlow)
    .runWith(Sink.foreach(System.out::println), mat)
    .toCompletableFuture().get();
```
