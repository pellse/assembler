# Assembler
Efficient implementation of the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html) for querying/merging data from multiple datasources/services, with a specific focus on solving the N + 1 query problem.

## Native Reactive Support

As of version 0.3.1, a new implementation [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core) was added to natively support [Reactive Streams](http://www.reactive-streams.org) specification:

[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22reactive-assembler-core%22) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core)

This new implementation is based on [Project Reactor](https://projectreactor.io), which means the Assembler library through the [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core) module can participate in a end to end reactive streams chain (e.g. from a REST endpoint to the database) and keep all reactive streams properties as defined by the [Reactive Manifesto](https://www.reactivemanifesto.org) (Responsive, Resillient, Elastic, Message Driven with back-pressure, non-blocking, etc.)

This is the only module still actively maintained, all the other ones (see below) are still available but deprecated in favor of this one.

## Use Cases

One interesting use case would be for example to build a materialized view in a microservice architecture supporting Event Sourcing and Command Query Responsibility Segregation (CQRS). In this context, if we have an incoming stream of events where each event needs to be enriched with some sort of external data before being stored, it would be convenient to be able to easily batch those events instead of hitting those external services for every single event.

## Usage Example for Native Reactive Support
Assuming the following data model of a very simplified online store, and api to access different services:
```java
public record Customer(Long customerId, String name) {}
public record BillingInfo(Long customerId, String creditCardNumber) {}
public record OrderItem(Long customerId, String orderDescription, Double price) {}
public record Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {}

Flux<Customer> customers(); // REST call to a separate microservice (no query filters for brevity)
Publisher<BillingInfo> billingInfo(List<Long> customerIds); // Connects to MongoDB
Publisher<OrderItem> allOrders(List<Long> customerIds); // Connects to a relational database
```

If `customers()` returns 50 customers, instead of having to make one additional call per *customerId* to retrieve each customer's associated `BillingInfo` (which would result in 50 additional network calls, thus the N + 1 queries issue) we can only make 1 additional call to retrieve all at once all `BillingInfo` for all `Customer` returned by `customers()`, same for `OrderItem`. Since we are working with 3 different and independent datasources, joining data from `Customer`, `BillingInfo` and `OrderItem` into `Transaction` (using *customerId* as a correlation id between all those entities) has to be done at the application level, which is what this library was implemented for.

When using [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core), the code to aggregate different reactive datasources will typically look like this:

```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.Mapper.*;
import reactor.core.publisher.Flux;

Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        oneToOne(this::billingInfo, BillingInfo::customerId),
        oneToMany(this::allOrders, OrderItem::customerId),
        Transaction::new)
    .build();

Flux<Transaction> transactionFlux = assembler.assemble(customers());
```
In the scenario where we deal with an infinite stream of data, since the Assembler needs to completely drain the upstream from `customers()` to gather all the correlation ids (*customerId*), the example above will result in resource exhaustion. The solution is to split the stream into multiple smaller streams and batch the processing of those individual smaller streams. Most reactive libraries already support that concept, below is an example using Project Reactor:
```java
Flux<Transaction> transactionFlux = customers()
    .windowTimeout(100, ofSeconds(5))
    .flatMapSequential(assembler::assemble);
```
## Caching
In addition to providing helper functions to define mapping semantics (e.g. `oneToOne()`, `OneToMany()`), `Mapper` also provides a simple caching/memoization mechanism through the `cached()` wrapper method.

```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.Mapper.*;
import static io.github.pellse.reactive.assembler.MapperCache.cached;
import reactor.core.publisher.Flux;

var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        cached(oneToOne(this::getBillingInfos, BillingInfo::customerId)),
        cached(oneToMany(this::getAllOrders, OrderItem::customerId)),
        Transaction::new)
    .build();
    
var transactionFlux = customers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```
This can be useful for aggregating dynamic data with static data or data we know doesn't change often (or on a predefined schedule e.g. data that is refreshed by a batch job once a day).

The `cached()` method internally uses the list of correlation ids from the upstream (list of customer ids in the above example) as the cache key. The result of the consumed downstream is cached, not each separate item from the downstream. Concretely, if we take the line `cached(oneToOne(this::getBillingInfos, BillingInfo::customerId))`, from the example above `window(3)` would generate windows of 3 `Customer` e.g. ( *(C1, C2, C3)*, *(C1, C4, C7)*, *(C4, C5, C6)*, *(C2, C8, C9)* ), at that moment in time the cache would like this:

| Key [correlation id list] | Cached BillingInfo Downstream |
| --- | --- |
| *[1, 2, 3]* | *(B1, B2, B3)* |
| *[1, 4, 7]* | *(B1, B4, B7)* |
| *[4, 5, 6]* | *(B4, B5, B6)* |
| *[2, 8, 9]* | *(B2, B8, B9)* |

Here B1, B2 and B4 are each cached twice as each are part of 2 different streams. This is because we effectively cache the query itself vs. separate individual values, so when caching downstreams we need to be careful to have predictable results otherwise the number of different combinations (of correlation id lists) might not justify to use caching.

**_In future versions, reactive caching of individual values should be supported_**.

### Pluggable Caching Strategy

Overloaded versions of the `cached()` method are also defined to allow plugging your own cache implementation. We can pass an additional parameter of type `MapperCache` to customize the caching mechanism. Here is how the `MapperCache` interface is defined:
```java
public interface Mapper<ID, R> extends Function<Iterable<ID>, Publisher<Map<ID, R>>>
public interface MapperCache<ID, R> extends BiFunction<Iterable<ID>, Mapper<ID, R>, Publisher<Map<ID, R>>> {}
```
If no `MapperCache` is passed to `cached()`, the default implementation of `MapperCache` used internally is based on `ConcurrentHashMap`, below is an example of how we can explicitely customize the caching mecanism:
```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.Mapper.*;
import static io.github.pellse.reactive.assembler.MapperCache.cached;

var billingInfoCache = new HashMap<Iterable<Long>, Publisher<Map<Long, BillingInfo>>>();

var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        cached(oneToOne(this::getBillingInfos, BillingInfo::customerId), billingInfoCache::computeIfAbsent),
        cached(oneToMany(this::getAllOrders, OrderItem::customerId), cache(HashMap::new)),
        Transaction::new)
    .build();
```
As we can see above, the declaration of a cache implementing `MapperCache` can be verbose. Utility methods are provided to take care of the generic types verbosity and make it more convenient to define new implementations of `MapperCache` via `cache()`.

### Third Party Cache Integration

Here is a list of add-on modules that can be used to integrate third party caching libraries (more will be added in the future):

| Assembler add-on module | Third party cache library |
| --- | --- |
| [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-cache-caffeine.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22reactive-assembler-cache-caffeine`%22) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core) [reactive-assembler-cache-caffeine](https://github.com/pellse/assembler/tree/master/reactive-assembler-cache-caffeine) | [Caffeine](https://github.com/ben-manes/caffeine) |


Below is an example of defining a `MapperCache` implementation for the [Caffeine](https://github.com/ben-manes/caffeine) library. For `BillingInfo` stream the integration is done manually, for `OrderItem` stream the `cache()` helper method is used: 
```java
import static io.github.pellse.reactive.assembler.cache.caffeine.CaffeineMapperCacheHelper.cache;
import static io.github.pellse.reactive.assembler.Mapper.oneToMany;
import static io.github.pellse.reactive.assembler.Mapper.oneToOne;
import static io.github.pellse.reactive.assembler.MapperCache.cached;

import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

final Cache<Iterable<Long>, Publisher<Map<Long, BillingInfo>>> billingInfoCache = newBuilder().maximumSize(10).build();

var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        cached(oneToOne(this::getBillingInfos, BillingInfo::customerId), billingInfoCache::get),
        cached(oneToMany(this::getAllOrders, OrderItem::customerId), cache(newBuilder().maximumSize(10))),
        Transaction::new)
    .build();
```

## Kotlin Support
```kotlin
import io.github.pellse.reactive.assembler.Mapper.oneToMany
import io.github.pellse.reactive.assembler.Mapper.oneToOne
import io.github.pellse.reactive.assembler.MapperCache.cache
import io.github.pellse.reactive.assembler.kotlin.assembler
import io.github.pellse.reactive.assembler.kotlin.cached

val orderItemCache = hashMapOf<Iterable<Long>, Publisher<Map<Long, List<OrderItem>>>>()

val assembler = assembler<Transaction>()
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        oneToOne(::getBillingInfos, BillingInfo::customerId).cached(cache(::hashMapOf)),
        oneToMany(::getAllOrders, OrderItem::customerId).cached(orderItemCache::computeIfAbsent),
        ::Transaction
    ).build()
```

## Other Supported Technologies
Currently the following legacy implementations are still available, but it is strongly recommended to switch to [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core) as any future development will be focused on the Native Reactive implementation described above:

1. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-core%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/assembler-core) [Java 8 Stream (synchronous and parallel)](https://github.com/pellse/assembler/tree/master/assembler-core)
2. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-core%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/assembler-core) [CompletableFuture](https://github.com/pellse/assembler/tree/master/assembler-core)
3. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-flux.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-flux%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-flux.svg)](http://javadoc.io/doc/io.github.pellse/assembler-flux) [Flux](https://github.com/pellse/assembler/tree/master/assembler-flux)
4. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-rxjava.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-rxjava%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-rxjava.svg)](http://javadoc.io/doc/io.github.pellse/assembler-rxjava) [RxJava](https://github.com/pellse/assembler/tree/master/assembler-rxjava)
5. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-akka-stream.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-akka-stream%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-akka-stream.svg)](http://javadoc.io/doc/io.github.pellse/assembler-akka-stream) [Akka Stream](https://github.com/pellse/assembler/tree/master/assembler-akka-stream)
6. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-reactive-stream-operators.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-reactive-stream-operators%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-reactive-stream-operators.svg)](http://javadoc.io/doc/io.github.pellse/assembler-reactive-stream-operators) [Eclipse MicroProfile Reactive Stream Operators](https://github.com/pellse/assembler/tree/master/assembler-reactive-stream-operators)

You only need to include in your project's build file (maven, gradle) the lib that corresponds to the type of reactive (or non reactive) support needed (Java 8 stream, CompletableFuture, Flux, RxJava, Akka Stream, Eclipse MicroProfile Reactive Stream Operators).

All modules above have dependencies on the following modules:
1. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-core%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/assembler-core) [assembler-core](https://github.com/pellse/assembler/tree/master/assembler-core)
2. [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/assembler-util.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22assembler-util%22)
[![Javadocs](http://javadoc.io/badge/io.github.pellse/assembler-util.svg)](http://javadoc.io/doc/io.github.pellse/assembler-util) [assembler-util](https://github.com/pellse/assembler/tree/master/assembler-util)

## What's Next?
See the [list of issues](https://github.com/pellse/assembler/issues) for planned improvements in a near future.
