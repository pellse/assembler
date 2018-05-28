# Assembler
Small library allowing to efficiently assemble entities from querying/merging external datasources or aggregating microservices.

More specifically it was designed as a very lightweight solution to resolve the N + 1 queries problem when aggregating data, not only from database calls (e.g. Spring Data JPA, Hibernate) but from arbitrary datasources (relational databases, NoSQL, REST, local method calls, etc.).

One key feature is that the caller doesn't need to worry about the order of the data returned by the different datasources, so no need for example to modify any SQL query to add an ORDER BY clause.

Stay tuned for more complete documentation very soon in terms of more detailed explanations regarding how the library works and comparisons with other solutions, a dedicated series of blog posts is also coming on https://javatechnicalwealth.com/blog/

## Supported Technologies
Currently the following implementations are supported (with links to their respective Maven repositories):
1. [Synchronous](https://github.com/pellse/assembler/tree/master/assembler-synchronous)
2. [Flux](https://github.com/pellse/assembler/tree/master/assembler-flux)

A `CompletableFuture`, Akka Stream and RxJava implementation will be available soon.

## Use Cases

One interesting use case would be for example to build a materialized view in a microservice architecture supporting Event Sourcing and Command Query Responsibility Segregation (CQRS). In this context, if you have an incoming stream of events where each event needs to be enriched with some sort of external data before being stored (e.g. stream of GPS coordinates enriched with location service and/or weather service), it would be convenient to be able to easily batch those events instead of hitting those external services for every single event.

Another use case could be when working with JPA projections (to fetch minimal amount of data) instead of full blown JPA entities (directly with Hibernate of through Spring Data repositories). In some cases the EntityManager might not be of any help to efficiently join multiple entities together or at least it might not be trivial to cache/optimize queries to avoid the N + 1 query problem.

## Usage Examples
Assuming the following data model and api to return those entities:
```java
@Data
@AllArgsConstructor
public class Customer {
    private final Long customerId;
    private final String name;
}

@Data
@AllArgsConstructor
public class BillingInfo {
    private final Long customerId;
    private final String creditCardNumber;
}

@Data
@AllArgsConstructor
public class OrderItem {
    private final Long customerId;
    private final String orderDescription;
    private final Double price;
}

@Data
@AllArgsConstructor
public class Transaction {
    private final Customer customer;
    private final BillingInfo billingInfo;
    private final List<OrderItem> orderItems;
}

List<Customer> getCustomers(); // This could be a REST call to an already existing microservice
List<BillingInfo> getBillingInfoForCustomers(List<Long> customerIds); // This could be a call to MongoDB
List<OrderItem> getAllOrdersForCustomers(List<Long> customerIds); // This could be a call to an Oracle database
```

So if `getCustomers()` returns 50 customers, instead of having to make one additional call per *customerId* to retrieve each customer's associated `BillingInfo` list (which would result in 50 additional network calls, thus the N + 1 queries issue) we can only make 1 additional call to retrieve all at once all `BillingInfo`s for all `Customer`s returned by `getCustomer()`, same for `OrderItem`s. This implies though that combining `Customer`s, `BillingInfo`s and `OrderItem`s into `Transaction`s using *customerId* as a correlation id between all those entities has to be done outside those datasources, which is what this library was implemented for:

To build the `Transaction` entity we can simply combine the invocation of the methods declared above using (from [SynchronousAssemblerTest](https://github.com/pellse/assembler/blob/master/assembler-synchronous/src/test/java/io/github/pellse/assembler/synchronous/SynchronousAssemblerTest.java)):
```java
List<Transaction> transactions = assemblerOf(Transaction.class)
    .fromSupplier(this::getCustomers, Customer::getCustomerId)
    .assembleWith(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId, BillingInfo::new), // supplier of default values
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(synchronousAssemblerAdapter())
    .collect(toList());
```

Reactive support is also provided through the [Spring Reactor Project](https://projectreactor.io/) to asynchronously retrieve all data to be aggregated, for example (from [FluxAssemblerTest]( https://github.com/pellse/assembler/blob/master/assembler-flux/src/test/java/io/github/pellse/assembler/flux/FluxAssemblerTest.java)):
```java
Flux<Transaction> transactionFlux = assemblerOf(Transaction.class)
    .fromSupplier(this::getCustomers, Customer::getCustomerId)
    .assembleWith(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .using(fluxAssemblerAdapter(elastic()))
```
or
```java
Flux<Transaction> transactionFlux = Flux.fromIterable(getCustomers()) // or just getCustomerFlux()
    .buffer(10) // or bufferTimeout(10, ofSeconds(5)) to e.g. batch every 5 seconds or max of 10 customers
    .flatMap(customers -> assemblerOf(Transaction.class)
        .from(customers, Customer::getCustomerId)
        .assembleWith(
            oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
            oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
            Transaction::new)
        .using(fluxAssemblerAdapter())) // parallel scheduler used by default
```
## What's Next?
See the [list of issues](https://github.com/pellse/assembler/issues) for planned improvements in a near future.
