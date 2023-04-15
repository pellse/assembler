# Assembler
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-core) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core)

[Reactive](https://www.reactivemanifesto.org), functional, type-safe and stateless Java API for efficient implementation of the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html) (similar to the Service Aggregator pattern) for querying and merging data from multiple data sources/services, with a specific focus on solving the N + 1 query problem.

The Assembler Library internally leverages [Project Reactor](https://projectreactor.io) to implement end to end reactive streams pipelines (e.g. from a REST endpoint to a RSocket based microservice to the database) and keep all reactive streams properties as defined by the [Reactive Manifesto](https://www.reactivemanifesto.org) (Responsive, Resilient, Elastic, Message Driven with back-pressure, non-blocking, etc.)

## Use Cases

The Assembler library can be used in situations where an application needs to access data or functionality that is spread across multiple services. Some common use cases for this pattern include:

1. CQRS/Event Sourcing: The Assembler library can be used on the read side of a CQRS and Event Sourcing architecture to efficiently build materialized views that aggregate data from multiple sources.
2. Data integration: An application may need to access data from multiple sources, such as databases, third-party services, or internal systems. The Assembler library can be used to create a single API that integrates data from all of these sources, providing a single point of access for the application.
3. Microservices architecture: In a microservices architecture, different functionality is provided by different services. The Assembler library can be used to create a single API that combines the functionality of multiple microservices, making it easier for the application to access the services it needs.
4. Combining functionality from different APIs: The Assembler library can be used to create a new API that combines functionality from different existing APIs, making it easier for clients to access the functionality they need. 
5. Creating a single point of entry: The Assembler library can be used to create a single point of entry for different systems, making it easier for clients to access the functionality they need.

## Usage Example
Below is an example of generating transaction information from a list of customers of an online store. Assuming the following fictional data model and api to access different services:
```java
public record Customer(Long customerId, String name) {}
public record BillingInfo(Long id, Long customerId, String creditCardNumber) {}
public record OrderItem(String id, Long customerId, String orderDescription, Double price) {}
public record Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {}

Flux<Customer> getCustomers(); // Call to a REST or RSocket microservice
Flux<BillingInfo> getBillingInfo(List<Long> customerIds); // Connects to relational database (R2DBC)
Flux<OrderItem> getAllOrders(List<Long> customerIds); // Connects to MongoDB (Reactive Streams Driver)
```
If for example `getCustomers()` returns 50 customers, instead of having to make one additional call per *customerId* to retrieve each customer's associated `BillingInfo` (which would result in 50 additional network calls, thus the N + 1 queries issue) we can only make 1 additional call to retrieve all at once all `BillingInfo` for all `Customer` returned by `getCustomers()`, idem for `OrderItem`. Since we are working with 3 different and independent data sources, joining data from `Customer`, `BillingInfo` and `OrderItem` into `Transaction` (using *customerId* as a correlation id between all those entities) has to be done at the application level, which is what this library was implemented for.

When using [reactive-assembler-core](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-core), here is how we would aggregate multiple reactive data sources and implement the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html):

```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
    
Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo)),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, this::getAllOrders)),
        Transaction::new)
    .build();

Flux<Transaction> transactionFlux = assembler.assemble(getCustomers());
```
In the code snippet above, we first retrieve all customers, then we concurrently retrieve all billing info (in a single query) and all orders (in a single query) associated with all previously retrieved customers (as defined by the assembler rules). We finally aggregate each customer/billing info/list of order items (related by the same customer id) into a `Transaction` object. We end up with a reactive stream (`Flux`) of `Transaction` objects.

## Infinite Stream of Data
In the scenario where we deal with an infinite or very large stream of data e.g. 100 000+ customers, since the Assembler Library needs to completely drain the upstream from `getCustomers()` to gather all the correlation ids (*customerId*), the example above will result in resource exhaustion. The solution is to split the stream into multiple smaller streams and batch the processing of those individual streams. Most reactive libraries already support that concept, below is an example using [Project Reactor](https://projectreactor.io):
```java
Flux<Transaction> transactionFlux = getCustomers()
    .windowTimeout(100, ofSeconds(5))
    .flatMapSequential(assembler::assemble);
```

## Asynchronous Caching
In addition to providing helper functions to define mapping semantics (e.g. `oneToOne()`, `oneToMany()`), the Assembler also provides a caching/memoization mechanism of the downstream subqueries via the `CacheFactory.cached()` wrapper function.

```java
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
    
var assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders))),
        Transaction::new)
    .build();
    
var transactionFlux = getCustomers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```

### Pluggable Asynchronous Caching Strategy
Overloaded versions of `CacheFactory.cached()` allow to plug different `Cache` implementations. We can pass an additional parameter of type `CacheFactory` to the `cached()` method to customize the caching mechanism. If no `CacheFactory` parameter is passed to `cached()`, the default implementation will internally return a `Cache` based on `HashMap`.

Below is an example of a few different ways we can explicitly customize the caching mechanism:
```java
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.Cache.cache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
    
var assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, new TreeMap<>()))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(TreeMap::new)))),
        Transaction::new)
    .build();
```

### Third Party Asynchronous Cache Provider Integration

Below is a compilation of supplementary modules that are available for integration with third-party asynchronous caching libraries. Additional modules will be incorporated in the future:

| Assembler add-on module | Third party cache library |
| --- | --- |
| [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-cache-caffeine.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-cache-caffeine) [reactive-assembler-cache-caffeine](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-cache-caffeine) | [Caffeine](https://github.com/ben-manes/caffeine) |

The following is a sample implementation of `CacheFactory` that demonstrates the use of the [Caffeine](https://github.com/ben-manes/caffeine) library via the `caffeineCache()` helper method, which is provided as part of the caffeine add-on module:
```java
import com.github.benmanes.caffeine.cache.Caffeine;
import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache;
import static java.time.Duration.ofMinutes;

Caffeine<Object, Object> cacheBuilder = newBuilder()
    .recordStats()
    .expireAfterWrite(ofMinutes(10))
    .maximumSize(1000);

var assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, caffeineCache(cacheBuilder)))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, caffeineCache()))),
        Transaction::new)
    .build();
```

### Auto Caching

```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory;

Flux<BillingInfo> billingInfoFlux = ... // BillingInfo data coming from e.g. Kafka;
Flux<OrderItem> orderItemFlux = ... // OrderItem data coming from e.g. Kafka;

var assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, caffeineCache(), autoCache(billingInfoFlux)))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, autoCache(orderItemFlux)))),
        Transaction::new)
    .build();
    
var transactionFlux = getCustomers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```

It is also possible to customize the behavior of Auto Caching via `AutoCacheFactoryBuilder.autoCache()`:
```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorMap.onErrorMap;
import static java.time.Duration.*;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;
import static reactor.core.scheduler.Schedulers.newParallel;

var logger = getLogger("auto-cache-logger");

Flux<BillingInfo> billingInfoFlux = ... // BillingInfo data coming from e.g. Kafka;
Flux<OrderItem> orderItemFlux = ... // OrderItem data coming from e.g. Kafka;

var assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo,
            autoCache(billingInfoFlux)
                .maxWindowSizeAndTime(100, ofSeconds(5))
                .errorHandler(error -> logger.log(WARNING, "Error in autoCache", error))
                .scheduler(newParallel("billing-info"))
                .build()))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders,
            autoCache(orderItemFlux)
                .maxWindowSize(50)
                .errorHandler(onErrorMap(MyException::new))
                .scheduler(newParallel("order-item"))
                .build()))),
        Transaction::new)
    .build();
    
var transactionFlux = getCustomers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```
By default, the cache is updated for every element from the incoming stream of data, but it can be configured to batch the cache updates, useful when we are updating a remote cache to optimize network calls

### Event Based Auto Caching
```java
import io.github.pellse.reactive.assembler.Assembler;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache;

// Example of your custom domain events not known by the Assembler Library
sealed interface MyEvent<T> {
    T item();
}
record ItemUpdated<T>(T item) implements MyEvent<T> {}
record ItemDeleted<T>(T item) implements MyEvent<T> {}

record MyOtherEvent<T>(T value, boolean isAddOrUpdateEvent) {}

// E.g. Flux coming from a CDC/Kafka source
Flux<MyOtherEvent<BillingInfo>> billingInfoFlux = Flux.just(
    new MyOtherEvent<>(billingInfo1, true), new MyOtherEvent<>(billingInfo2, true),
    new MyOtherEvent<>(billingInfo2, false), new MyOtherEvent<>(billingInfo3, false));

// E.g. Flux coming from a CDC/Kafka source
Flux<MyEvent<OrderItem>> orderItemFlux = Flux.just(
    new ItemUpdated<>(orderItem11), new ItemUpdated<>(orderItem12), new ItemUpdated<>(orderItem13),
    new ItemDeleted<>(orderItem31), new ItemDeleted<>(orderItem32), new ItemDeleted<>(orderItem33));

CacheTransformer<Long, BillingInfo, BillingInfo> billingInfoAutoCache =
    autoCache(billingInfoFlux, MyOtherEvent::isAddOrUpdateEvent, MyOtherEvent::value);

CacheTransformer<Long, OrderItem, List<OrderItem>> orderItemAutoCache =
    autoCache(orderItemFlux, ItemUpdated.class::isInstance, MyEvent::item);

Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, billingInfoAutoCache))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, orderItemAutoCache))),
        Transaction::new)
    .build();

var transactionFlux = getCustomers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```

## Integration with non-reactive sources
A utility function `toPublisher()` is also provided to wrap non-reactive sources, useful when e.g. calling 3rd party synchronous APIs:
```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.QueryUtils.toPublisher;

List<BillingInfo> getBillingInfo(List<Long> customerIds); // non-reactive source
List<OrderItem> getAllOrders(List<Long> customerIds); // non-reactive source

Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withCorrelationIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(toPublisher(this::getBillingInfo))),
        rule(OrderItem::customerId, oneToMany(OrderItem::id, toPublisher(this::getAllOrders))),
        Transaction::new)
    .build();
```

## Kotlin Support
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-kotlin-extension.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-kotlin-extension) [reactive-assembler-kotlin-extension](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-kotlin-extension)
```kotlin
import io.github.pellse.reactive.assembler.kotlin.assembler
import io.github.pellse.reactive.assembler.kotlin.cached
import io.github.pellse.reactive.assembler.Cache.cache
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.Rule.rule
import io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache

// Example 1:
val assembler = assembler<Transaction>()
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached())),
        rule(OrderItem::customerId, oneToMany(::getAllOrders.cached(::hashMapOf))),
        ::Transaction
    ).build()
            
// Example 2:
val assembler = assembler<Transaction>()
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(cache()))),
        rule(OrderItem::customerId, oneToMany(::getAllOrders.cached(caffeineCache()))),
        ::Transaction
    ).build()
```

## What's Next?
See the [list of issues](https://github.com/pellse/assembler/issues) for planned improvements in a near future.
