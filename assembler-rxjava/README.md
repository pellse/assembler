# assembler-rxjava

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-rxjava.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-rxjava%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-rxjava.svg)](http://javadoc.io/doc/io.github.pellse/assembler-rxjava)

## Usage Example

When using RxJava `Observable`:
```java
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.query.io.github.pellse.util.MapperUtils.oneToOne;
import static io.github.pellse.query.io.github.pellse.util.MapperUtils.oneToManyAsList;
import static io.github.pellse.assembler.rxjava.ObservableAdapter.observableAdapter;

Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
         oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
         oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
         Transaction::new)
     .using(observableAdapter(newThread()))
     .assembleFromSupplier(this::getCustomers);
```
or
```java
Assembler<Customer, Observable<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(observableAdapter()); // computation scheduler used by default

Observable<Transaction> transactionObservable = Observable.fromIterable(getCustomers())
    .buffer(10)
    .flatMap(assembler::assemble);
```

When using RxJava `Flowable`:
```java
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.query.io.github.pellse.util.MapperUtils.oneToOne;
import static io.github.pellse.query.io.github.pellse.util.MapperUtils.oneToManyAsList;
import static io.github.pellse.assembler.rxjava.FlowableAdapter.flowableAdapter;

Flowable<Transaction> transactionFlowable = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(flowableAdapter(newThread()))
    .assembleFromSupplier(this::getCustomers);
```
or
```java
Assembler<Customer, Flowable<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdResolver(Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(AssemblerTestUtils::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new),
        oneToManyAsList(AssemblerTestUtils::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(flowableAdapter()); // computation scheduler used by default

Flowable<Transaction> transactionFlowable = Flowable.fromIterable(getCustomers())
    .buffer(3)
    .flatMap(assembler::assemble);
```
