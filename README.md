# Assembler
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22reactive-assembler-core%22) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core)

Functional, type-safe and stateless Java API for efficient implementation of the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html) for querying/merging data from multiple datasources/services, with a specific focus on solving the N + 1 query problem.

## Native Reactive Streams Support

As of version 0.3.1, a new implementation [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core) was added to natively support [Reactive Streams](http://www.reactive-streams.org) specification. This new implementation internally leverages [Project Reactor](https://projectreactor.io), which now allows the Assembler library (through the [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core) module) to participate in end to end reactive streams chains (e.g. from a REST endpoint to a RSocket based microservice to the database) and keep all reactive streams properties as defined by the [Reactive Manifesto](https://www.reactivemanifesto.org) (Responsive, Resillient, Elastic, Message Driven with back-pressure, non-blocking, etc.)

This is the only module still actively maintained, all the other ones (see below) are still available but deprecated in favor of this one.

## Use Cases

One interesting use case would be for example to build a materialized view in a microservice architecture supporting Event Sourcing and Command Query Responsibility Segregation (CQRS). In this context, if we have an incoming stream of events where each event needs to be enriched with some sort of external data before being stored, it would be convenient to be able to easily batch those events instead of hitting those external services for every single event.

## Usage Example for Native Reactive Support
Assuming the following data model of a very simplified online store, and api to access different services:
```java
public record Customer(Long customerId, String name) {}
public record BillingInfo(Long id, Long customerId, String creditCardNumber) {}
public record OrderItem(Long id, Long customerId, String orderDescription, Double price) {}
public record Transaction(Customer customer, BillingInfo billingInfo, List<OrderItem> orderItems) {}

Flux<Customer> customers(); // call to a RSocket microservice (no query filters for brevity)
Publisher<BillingInfo> billingInfo(List<Long> customerIds); // Connects to relational database (R2DBC)
Publisher<OrderItem> allOrders(List<Long> customerIds); // Connects to MongoDB (Reactive Streams Driver)
```

If `customers()` returns 50 customers, instead of having to make one additional call per *customerId* to retrieve each customer's associated `BillingInfo` (which would result in 50 additional network calls, thus the N + 1 queries issue) we can only make 1 additional call to retrieve all at once all `BillingInfo` for all `Customer` returned by `customers()`, same for `OrderItem`. Since we are working with 3 different and independent datasources, joining data from `Customer`, `BillingInfo` and `OrderItem` into `Transaction` (using *customerId* as a correlation id between all those entities) has to be done at the application level, which is what this library was implemented for.

When using [reactive-assembler-core](https://github.com/pellse/assembler/tree/master/reactive-assembler-core), here is how we would aggregate multiple reactive datasources and implement the [API Composition Pattern](https://microservices.io/patterns/data/api-composition.html):

```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import reactor.core.publisher.Flux;
    
Assembler<Customer, Flux<Transaction>> assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(this::getBillingInfo)),
        rule(OrderItem::customerId, oneToMany(this::getAllOrders)),
        Transaction::new)
    .build();

Flux<Transaction> transactionFlux = assembler.assemble(customers());
```
In the scenario where we deal with an infinite stream of data, since the Assembler needs to completely drain the upstream from `customers()` to gather all the correlation ids (*customerId*), the example above will result in resource exhaustion. The solution is to split the stream into multiple smaller streams and batch the processing of those individual smaller streams. Most reactive libraries already support that concept, below is an example using [Project Reactor](https://projectreactor.io):
```java
Flux<Transaction> transactionFlux = customers()
    .windowTimeout(100, ofSeconds(5))
    .flatMapSequential(assembler::assemble);
```
## Asynchronous Caching
In addition to providing helper functions to define mapping semantics (e.g. `oneToOne()`, `OneToMany()`), the Assembler also provides a caching/memoization mechanism of the downstreams through the `cached()` wrapper method.

```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.CacheFactory.cached;
import reactor.core.publisher.Flux;
    
var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo))),
        rule(OrderItem::customerId, oneToMany(cached(this::getAllOrders))),
        Transaction::new)
    .build();
    
var transactionFlux = customers()
    .window(3)
    .flatMapSequential(assembler::assemble);
```
This can be useful for aggregating dynamic data with static data or data we know doesn't change often (or on a predefined schedule e.g. data that is refreshed by a batch job once a day).

The `cached()` method internally uses the list of correlation ids from the upstream as cache keys (list of customer ids in the above example) with each individual value (or entity) of the downstream being cached. Concretely, if we take the line `rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfo)))`, from the example above `window(3)` would generate windows of 3 `Customer` e.g.:

*[C1, C2, C3]*, *[C1, C4, C7]*, *[C4, C5, C6]*, *[C2, C4, C6]*

At that specific moment in time the execution would like this:

1. *getBillingInfo([1, 2, 3]) = [B1, B2, B3]*
2. *merge(getBillingInfo([4, 7]), fromCache([1])) = [B4, B7, B1]*
3. *merge(getBillingInfo([5, 6]), fromCache([4])) = [B5, B6, B4]*
4. *fromCache([2, 4, 6]) = [B2, B4, B6]*

### Pluggable Asynchronous Caching Strategy

Overloaded versions of the `cached()` method are also defined to allow plugging your own cache implementation. We can pass an additional parameter of type `CacheFactory` to customize the caching mechanism:
```java
public interface CacheFactory<ID, R> {
    Cache<ID, R> create(Function<Iterable<? extends ID>, Mono<Map<ID, Collection<R>>>> fetchFunction);
}
    
public interface Cache<ID, R> {
    Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids);
}
```
If no `CacheFactory` parameter is passed to `cached()`, the default implementation will internally return a 'Cache' based on `ConcurrentHashMap`.

Below is an example of a few different ways we can explicitely customize the caching mecanism:
```java
import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.Cache.cache;
import static io.github.pellse.reactive.assembler.CacheFactory.cached;
import reactor.core.publisher.Flux;
  
var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfos, new HashMap<>()))),
        rule(OrderItem::customerId, oneToMany(cached(this::getAllOrders, cache(HashMap::new)))),
        Transaction::new)
    .build();
```
Overloaded versions of `cached()` and `cache()` are provided to wrap any implementation of `java.util.Map` since it doesn't natively implement
`Mono<Map<ID, Collection<R>>> getAll(Iterable<ID> ids)`.

### Third Party Cache Provider Integration

Here is a list of add-on modules that can be used to integrate third party caching libraries (more will be added in the future):

| Assembler add-on module | Third party cache library |
| --- | --- |
| [![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-cache-caffeine.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22reactive-assembler-cache-caffeine`%22) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-core.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-core) [reactive-assembler-cache-caffeine](https://github.com/pellse/assembler/tree/master/reactive-assembler-cache-caffeine) | [Caffeine](https://github.com/ben-manes/caffeine) |


Below is an example of using a `CacheFactory` implementation for the [Caffeine](https://github.com/ben-manes/caffeine) through the `caffeineCache()` helper method from the caffeine add-on module: 
```java
import com.github.benmanes.caffeine.cache.Cache;

import static io.github.pellse.reactive.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToMany;
import static io.github.pellse.reactive.assembler.RuleMapper.oneToOne;
import static io.github.pellse.reactive.assembler.Mapper.rule;
import static io.github.pellse.reactive.assembler.CacheFactory.cached;
import static io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache;

import static java.time.Duration.ofMinutes;
import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;

var cacheBuilder = newBuilder()
                .recordStats()
                .expireAfterWrite(ofMinutes(10))
                .maximumSize(1000);

var assembler = assemblerOf(Transaction.class)
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(cached(this::getBillingInfos, caffeineCache()))),
        rule(OrderItem::customerId, oneToMany(cached(this::getAllOrders, caffeineCache(cacheBuilder))))),
        Transaction::new)
    .build();
```

## Kotlin Support
[![Maven Central](https://img.shields.io/maven-central/v/io.github.pellse/reactive-assembler-kotlin-extension.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.pellse%22%20AND%20a:%22reactive-assembler-kotlin-extension%22) [![Javadocs](http://javadoc.io/badge/io.github.pellse/reactive-assembler-kotlin-extension.svg)](http://javadoc.io/doc/io.github.pellse/reactive-assembler-kotlin-extension)  [reactive-assembler-kotlin-extension](https://github.com/pellse/assembler/tree/master/reactive-assembler-kotlin-extension)
```kotlin
import io.github.pellse.reactive.assembler.kotlin.assembler
import io.github.pellse.reactive.assembler.kotlin.cached
import io.github.pellse.reactive.assembler.Cache.cache
import io.github.pellse.reactive.assembler.RuleMapper.oneToMany
import io.github.pellse.reactive.assembler.RuleMapper.oneToOne
import io.github.pellse.reactive.assembler.Mapper.rule
import io.github.pellse.reactive.assembler.cache.caffeine.CaffeineCacheFactory.caffeineCache

// Example 1:
val assembler = assembler<Transaction>()
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(::getBillingInfos.cached())),
        rule(OrderItem::customerId, oneToMany(::getAllOrders.cached(::hashMapOf))),
        ::Transaction
    ).build()
            
// Example 2:
val assembler = assembler<Transaction>()
    .withIdExtractor(Customer::customerId)
    .withAssemblerRules(
        rule(BillingInfo::customerId, oneToOne(::getBillingInfos.cached(cache()))),
        rule(OrderItem::customerId, oneToMany(::getAllOrders.cached(caffeineCache()))),
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
