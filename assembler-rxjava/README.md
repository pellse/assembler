# assembler-rxjava

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-rxjava.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-rxjava%22)

## Usage Example

When using RxJava `Observable`:
```java
Observable<Transaction> transactionObservable = assemblerOf(Transaction.class)
    .fromSourceSupplier(this::getCustomers, Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .assembleUsing(observableAdapter(newThread()))
```
or
```java
Observable<Transaction> transactionObservable = Observable.fromIterable(getCustomers()) // or just getCustomerObservable()
    .buffer(10) // or bufferTimeout(10, ofSeconds(5)) to e.g. batch every 5 seconds or max of 10 customers
    .flatMap(customers -> assemblerOf(Transaction.class)
        .fromSource(customers, Customer::getCustomerId)
        .withAssemblerRules(
            oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
            oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
            Transaction::new)
        .assembleUsing(observableAdapter())) // computation scheduler used by default
```

When using RxJava `Flowable`:
```java
Flowable<Transaction> transactionFlowable = assemblerOf(Transaction.class)
    .fromSourceSupplier(this::getCustomers, Customer::getCustomerId)
    .withAssemblerRules(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .assembleUsing(flowableAdapter(newThread()))
```
or
```java
Flowable<Transaction> transactionFlowable = Flowable.fromIterable(getCustomers()) // or just getCustomerObservable()
    .buffer(10) // or bufferTimeout(10, ofSeconds(5)) to e.g. batch every 5 seconds or max of 10 customers
    .flatMap(customers -> assemblerOf(Transaction.class)
        .fromSource(customers, Customer::getCustomerId)
        .withAssemblerRules(
            oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
            oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
            Transaction::new)
        .assembleUsing(flowableAdapter())) // computation scheduler used by default
```
