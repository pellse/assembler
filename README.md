# Assembler
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-core.svg?label=reactive-assembler-core)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-core) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core)

The Assembler Library provides a [reactive](https://www.reactivemanifesto.org), functional, type-safe, and stateless Java API designed to efficiently implement the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html) (similar to the Service Aggregator pattern) for querying and merging data from multiple data sources/services, with a focus on solving the N + 1 query problem.

Internally, the library leverages [Project Reactor](https://projectreactor.io) to implement end-to-end reactive stream pipelines and maintain all the reactive stream properties as defined by the [Reactive Manifesto](https://www.reactivemanifesto.org), including responsiveness, resilience, elasticity, message-driven with back-pressure, non-blocking, and more.

## Table of Contents

- **[Use Cases](#use-cases)**
- **[Basic Usage](#basic-usage)**
  - [Default values for missing data](#default-values-for-missing-data)
- **[Infinite Stream of Data](#infinite-stream-of-data)**
- **[Reactive Caching](#reactive-caching)**
  - [Pluggable Reactive Caching Strategies](#pluggable-reactive-caching-strategies)
    - *[Third Party Reactive Cache Provider Integration](#third-party-reactive-cache-provider-integration)*
  - [Auto Caching](#auto-caching)
    - *[Event Based Auto Caching](#event-based-auto-caching)*
- **[Integration with non-reactive sources](#integration-with-non-reactive-sources)**
- **[Kotlin Support](#kotlin-support)**
- **[What's Next?](#whats-next)**

## Use Cases

The Assembler library can be used in situations where an application needs to access data or functionality that is spread across multiple services. Some common use cases for this pattern include:

1. CQRS/Event Sourcing: The Assembler library can be used on the read side of a CQRS and Event Sourcing architecture to efficiently build materialized views that aggregate data from multiple sources.
2. Data integration: An application may need to access data from multiple sources, such as databases, third-party services, or internal systems. The Assembler library can be used to create a single API that integrates data from all of these sources, providing a single point of access for the application.
3. Microservices architecture: In a microservices architecture, different functionality is provided by different services. The Assembler library can be used to create a single API that combines the functionality of multiple microservices, making it easier for the application to access the services it needs.
4. Combining functionality from different APIs: The Assembler library can be used to create a new API that combines functionality from different existing APIs, making it easier for clients to access the functionality they need. 
5. Creating a single point of entry: The Assembler library can be used to create a single point of entry for different systems, making it easier for clients to access the functionality they need.

[:arrow_up:](#table-of-contents)

## Basic Usage
Here is an example of how to use the Assembler Library to generate transaction information from a list of customers of an online store. This example assumes the following fictional data model and API to access different services:
```java
public record Customer(Long customerId, String name) {}

public record BillingInfo(Long id, Long customerId, String creditCardNumber) {
    
  public BillingInfo(Long customerId) {
    this(null, customerId, "0000 0000 0000 0000");
  }
}

public record OrderItem(String id, Long customerId, String orderDescription, Double price) {}

public record Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {}
```

```java
Flux<Customer> getCustomers(); // e.g. call to a microservice or a Flux connected to a Kafka source
Flux<BillingInfo> getBillingInfo(List<Long> customerIds); // e.g. connects to relational database (R2DBC)
Flux<OrderItem> getAllOrders(List<Long> customerIds); // e.g. connects to MongoDB
```
In cases where the `getCustomers()` method returns a substantial number of customers, retrieving the associated `BillingInfo` for each customer would require an additional call per `customerId`. This would result in a considerable increase in network calls, causing the N + 1 queries issue. To mitigate this, we can retrieve all the `BillingInfo` for all the customers returned by `getCustomers()` with a single additional call. The same approach can be used for retrieving OrderItem information.

As we are working with three distinct and independent data sources, the process of joining data from `Customer`, `BillingInfo`, and `OrderItem` into a `Transaction` must be performed at the application level. This is the primary objective of this library.

When utilizing the [Assembler Library](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-core), the aggregation of multiple reactive data sources and the implementation of the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html) can be accomplished as follows:

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
The code snippet above demonstrates the process of first retrieving all customers, followed by the concurrent retrieval of all billing information and orders (in a single query) associated with the previously retrieved customers, as defined by the assembler rules. The final step involves aggregating each customer, their respective billing information, and list of order items (related by the same customer id) into a `Transaction` object. This results in a reactive stream (`Flux`) of `Transaction` objects.

[:arrow_up:](#table-of-contents)

### Default values for missing data
To provide a default value for each missing values from the result of the API call, a factory function can also be supplied as a 2nd parameter to the `oneToOne()` function. For example, when `getCustomers()` returns 3 `Customer` *[C1, C2, C3]*, and `getBillingInfo([ID1, ID2, ID3])` returns only 2 associated `BillingInfo` *[B1, B2]*, the missing value *B3* can be generated as a default value. By doing so, a `null` `BillingInfo` is never passed to the `Transaction` constructor:
```java
rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, customerId -> new BillingInfo(customerId)))
``` 
or more concisely:
```java
rule(BillingInfo::customerId, oneToOne(this::getBillingInfo, BillingInfo::new))
```
Unlike the `oneToOne()` function, `oneToMany()` will always default to generating an empty collection. Therefore, providing a default factory function is not needed. In the example above, an empty `List<OrderItem>` is passed to the `Transaction` constructor if `getAllOrders([1, 2, 3])` returns `null`.

[:arrow_up:](#table-of-contents)

## Infinite Stream of Data
In situations where an infinite or very large stream of data is being handled, such as dealing with 100,000+ customers, the Assembler Library needs to completely drain the upstream from `getCustomers()` to gather all correlation IDs (customerId). This can lead to resource exhaustion if not handled correctly. To mitigate this issue, the stream can be split into multiple smaller streams and processed in batches. Most reactive libraries already support this concept. Below is an example of this approach, utilizing [Project Reactor](https://projectreactor.io):
```java
Flux<Transaction> transactionFlux = getCustomers()
  .windowTimeout(100, ofSeconds(5))
  .flatMapSequential(assembler::assemble);
```
[:arrow_up:](#table-of-contents)

## Reactive Caching
Apart from offering convenient helper functions to define mapping semantics such as `oneToOne()` and `oneToMany()`, the Assembler library also includes a caching/memoization mechanism for the downstream subqueries via the `cached()` wrapper function:
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
[:arrow_up:](#table-of-contents)

### Pluggable Reactive Caching Strategies
The `cached()` function includes overloaded versions that enable users to utilize different `Cache` implementations. By providing an additional parameter of type `CacheFactory` to the `cached()` method, users can customize the caching mechanism as per their requirements. In case no `CacheFactory` parameter is passed to `cached()`, the default implementation will internally use a `Cache` based on `HashMap`. All `Cache` implementations are internally decorated with non-blocking concurrency controls, making them safe for concurrent access and modifications.

Here is an example of a different approach that users can use to explicitly customize the caching mechanism e.g. storing cache entries in a `TreeMap`:
```java
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cache;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
    
var assembler = assemblerOf(Transaction.class)
  .withCorrelationIdExtractor(Customer::customerId)
  .withAssemblerRules(
    rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo, cache(TreeMap::new)))),
    rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders, cache(TreeMap::new)))),
    Transaction::new)
  .build();
```
[:arrow_up:](#table-of-contents)

### Third Party Reactive Cache Provider Integration

Below is a compilation of supplementary modules that are available for integration with third-party caching libraries. Additional modules will be incorporated in the future:

| Assembler add-on module | Third party cache library |
| --- | --- |
| [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-cache-caffeine.svg?label=reactive-assembler-cache-caffeine)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-cache-caffeine) | [Caffeine](https://github.com/ben-manes/caffeine) |

Here is a sample implementation of `CacheFactory` that showcases the use of the [Caffeine](https://github.com/ben-manes/caffeine) library, which can be accomplished via the `caffeineCache()` helper method. This helper method is provided as part of the caffeine add-on module:
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
[:arrow_up:](#table-of-contents)

### Auto Caching
In addition to the cache mechanism provided by the `cached()` function, the Assembler Library also provides a mechanism to automatically and asynchronously update the cache in real-time as new data becomes available via the `autoCache()` function. This ensures that the cache is always up-to-date and avoids in most cases the need for `cached()` to fall back to fetch missing data.

The auto caching mechanism in the Assembler Library can be seen as being conceptually similar to a `KTable` in Kafka. Both mechanisms provide a way to keep a key-value store updated in real-time with the latest value per key from its associated data stream. However, the Assembler Library is not limited to just Kafka data sources and can work with any data source that can be consumed in a reactive stream.

This is how `autoCache()` connects to a data stream and automatically and asynchronously update the cache in real-time:
```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory;

Flux<BillingInfo> billingInfoFlux = ... // From e.g. Debezium/Kafka, RabbitMQ, etc.;
Flux<OrderItem> orderItemFlux = ... // From e.g. Debezium/Kafka, RabbitMQ, etc.;

var assembler = assemblerOf(Transaction.class)
  .withCorrelationIdExtractor(Customer::customerId)
  .withAssemblerRules(
    rule(BillingInfo::customerId,
      oneToOne(cached(this::getBillingInfo, caffeineCache(), autoCache(billingInfoFlux)))),
    rule(OrderItem::customerId,
      oneToMany(OrderItem::id, cached(this::getAllOrders, autoCache(orderItemFlux)))),
    Transaction::new)
  .build();
    
var transactionFlux = getCustomers()
  .window(3)
  .flatMapSequential(assembler::assemble);
```

It is also possible to customize the Auto Caching configuration via `autoCacheBuilder()`:
```java
import reactor.core.publisher.Flux;
import io.github.pellse.reactive.assembler.Assembler;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheBuilder;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.OnErrorMap.onErrorMap;
import static reactor.core.scheduler.Schedulers.newParallel;
import static java.time.Duration.*;
import static java.lang.System.Logger.Level.WARNING;
import static java.lang.System.getLogger;

var logger = getLogger("auto-cache-logger");

Flux<BillingInfo> billingInfoFlux = ... // From e.g. Debezium/Kafka, RabbitMQ, etc.;
Flux<OrderItem> orderItemFlux = ... // From e.g. Debezium/Kafka, RabbitMQ, etc.;

var assembler = assemblerOf(Transaction.class)
  .withCorrelationIdExtractor(Customer::customerId)
  .withAssemblerRules(
    rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo,
      autoCacheBuilder(billingInfoFlux)
        .maxWindowSizeAndTime(100, ofSeconds(5))
        .errorHandler(error -> logger.log(WARNING, "Error in autoCache", error))
        .scheduler(newParallel("billing-info"))
        .concurrency(50)
        .build()))),
    rule(OrderItem::customerId, oneToMany(OrderItem::id, cached(this::getAllOrders,
      autoCacheBuilder(orderItemFlux)
        .maxWindowSize(50)
        .errorHandler(onErrorMap(MyException::new))
        .scheduler(newParallel("order-item"))
        .concurrency(100, ofMillis(10))
        .build()))),
    Transaction::new)
  .build();
    
var transactionFlux = getCustomers()
  .window(3)
  .flatMapSequential(assembler::assemble);
```
By default, the cache is updated for every element from the incoming stream of data, but it can be configured to batch the cache updates, useful when we are updating a remote cache to optimize network calls

[:arrow_up:](#table-of-contents)

### Event Based Auto Caching
Assuming the following custom domain events not known by the Assembler Library:
```java
sealed interface MyEvent<T> {
  T item();
}

record ItemUpdated<T>(T item) implements MyEvent<T> {}
record ItemDeleted<T>(T item) implements MyEvent<T> {}

record MyOtherEvent<T>(T value, boolean isAddOrUpdateEvent) {}

// E.g. Flux coming from a Change Data Capture/Kafka source
Flux<MyOtherEvent<BillingInfo>> billingInfoFlux = Flux.just(
  new MyOtherEvent<>(billingInfo1, true), new MyOtherEvent<>(billingInfo2, true),
  new MyOtherEvent<>(billingInfo2, false), new MyOtherEvent<>(billingInfo3, false));

// E.g. Flux coming from a Change Data Capture/Kafka source
Flux<MyEvent<OrderItem>> orderItemFlux = Flux.just(
  new ItemUpdated<>(orderItem11), new ItemUpdated<>(orderItem12), new ItemUpdated<>(orderItem13),
  new ItemDeleted<>(orderItem31), new ItemDeleted<>(orderItem32), new ItemDeleted<>(orderItem33));
```
Here is how `autoCache()` can be used to adapt those custom domain events to add, update or delete entries from the cache in real-time:
```java
import io.github.pellse.reactive.assembler.Assembler;
import io.github.pellse.reactive.assembler.caching.CacheFactory.CacheTransformer;
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Rule.rule;
import static io.github.pellse.reactive.assembler.caching.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache;
    
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
[:arrow_up:](#table-of-contents)

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
[:arrow_up:](#table-of-contents)

## Kotlin Support
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-kotlin-extension.svg?label=reactive-assembler-kotlin-extension)](https://central.sonatype.com/artifact/io.github.pellse/reactive-assembler-kotlin-extension)
```kotlin
sealed interface MyEvent<T> {
  val item: T
}

data class ItemUpdated<T>(override val item: T) : MyEvent<T>
data class ItemDeleted<T>(override val item: T) : MyEvent<T>

// E.g. Flux coming from a Change Data Capture/Kafka source
val billingInfoFlux: Flux<MyEvent<BillingInfo>> = Flux.just(
  ItemUpdated(billingInfo1), ItemUpdated(billingInfo2),
  ItemUpdated(billingInfo3), ItemDeleted(billingInfo3))

// E.g. Flux coming from a Change Data Capture/Kafka source
val orderItemFlux: Flux<MyEvent<OrderItem>> = Flux.just(
  ItemUpdated(orderItem31), ItemUpdated(orderItem32), ItemUpdated(orderItem33),
  ItemDeleted(orderItem31), ItemDeleted(orderItem32), ItemDeleted(orderItem33))
```
```kotlin
import io.github.pellse.reactive.assembler.kotlin.assembler
import io.github.pellse.reactive.assembler.kotlin.cached
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.Rule.rule
import io.github.pellse.reactive.assembler.caching.AutoCacheFactory.autoCache
import io.github.pellse.reactive.assembler.caching.AutoCacheFactoryBuilder.autoCacheBuilder

val assembler = assembler<Transaction>()
  .withCorrelationIdExtractor(Customer::customerId)
  .withAssemblerRules(
    rule(BillingInfo::customerId, oneToOne(::getBillingInfo.cached(
      autoCache(billingInfoFlux, ItemUpdated::class::isInstance) { it.item }))),
    rule(OrderItem::customerId, oneToMany(OrderItem::id, ::getAllOrders.cached(
      autoCacheBuilder(orderItemFlux, ItemUpdated::class::isInstance, MyEvent<OrderItem>::item)
        .maxWindowSize(3)
        .build()))), 
    ::Transaction)
  .build()
```
[:arrow_up:](#table-of-contents)

## What's Next?
See the [list of issues](https://github.com/pellse/assembler/issues) for planned improvements in a near future.

[:arrow_up:](#table-of-contents)
