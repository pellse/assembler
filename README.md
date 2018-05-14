# assembler
Small library allowing to efficiently assemble entities from querying/merging external datasources or aggregating microservices.

More specifically it was designed as a lightweight solution to resolve the N + 1 queries problem when aggregating data, not only from database calls (e.g. Spring Data JPA, Hibernate) but from arbitrary datasources (relational databases, NoSQL, REST, local method calls, etc.).

One key feature is that the caller doesn't need to worry about the order of the data returned by the different datasources, so no need for example to modify any SQL query to add an ORDER BY clause.

## Usage examples
Assuming the following data model and api to return those entities:
```java
@Data
@AllArgsConstructor
public static class Customer {
    private final Long customerId;
    private final String name;
}

@Data
@AllArgsConstructor
public static class BillingInfo {
    private final Long customerId;
    private final String creditCardNumber;
}

@Data
@AllArgsConstructor
public static class OrderItem {
    private final Long customerId;
    private final String orderDescription;
    private final Double price;
}

@Data
@AllArgsConstructor
public static class Transaction {
    private final Customer customer;
    private final BillingInfo billingInfo;
    private final List<OrderItem> orderItems;
}

List<Customer> getCustomers(); // This could be a REST call to an already created microservice
List<BillingInfo> getBillingInfoForCustomers(List<Long> customerIds); // This could be a call to MongoDB
List<OrderItem> getAllOrdersForCustomers(List<Long> customerIds); // This could be a call to an Oracle database
```

So if we have 50 customers, instead of having to make a call per *customerId* to retrieve each customer's associated `BillingInfo` list (50 calls) we can only make 1 call to retrieve all `BillingInfo`s for all `Customer`s returned by `getCustomer()`, same for `OrderItem`s. This implies though that combining of `Customer`s, `BillingInfo`s and `OrderItem`s into `Transaction`s using *customerId* as a correlation id between all those entities has to be done outside those datasources, which is what this library was implemented for:

To build the `Transaction` entity we can simply combine the invocation of the methods declared above using:
```java
List<Transaction> transactions = SynchronousAssembler.of(this::getCustomers, Customer::getCustomerId)
    .assemble(
        oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
        oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
        Transaction::new)
    .collect(toList());
```

Reactive support is also provided through the [Spring Reactor Project](https://projectreactor.io/) :
```java
Flux<Transaction> transactionFlux = Flux.fromIterable(getCustomers())
    .buffer(3)
    .flatMap(customers -> FluxAssembler.of(customers, Customer::getCustomerId)
        .assemble(
             oneToOne(this::getBillingInfoForCustomers, BillingInfo::getCustomerId),
             oneToManyAsList(this::getAllOrdersForCustomers, OrderItem::getCustomerId),
             Transaction::new));
```
## What's next?
See the [list of issues](https://github.com/pellse/assembler/issues) for planned improvements in a near future.

Currently the following implementations are supported:
1. [Synchronous](https://github.com/pellse/assembler/tree/master/assembler-synchronous)
2. [Flux](https://github.com/pellse/assembler/tree/master/assembler-flux)

A `CompletableFuture` and Akka Stream implementation will be available soon.

This page is currently weak in terms of more detailed explanations regarding how the library works and comparisons with other solutions, but stay tuned for better documentation very soon...
