package io.github.pellse.reactive.assembler.cache.caffeine;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.pellse.reactive.assembler.CachedRuleMapperSource;
import io.github.pellse.reactive.assembler.RuleMapperSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.function.Function;

import static com.github.benmanes.caffeine.cache.AsyncCacheLoader.bulk;
import static com.github.benmanes.caffeine.cache.Caffeine.newBuilder;
import static io.github.pellse.reactive.assembler.RuleMapperSource.call;
import static io.github.pellse.util.ObjectUtils.also;
import static io.github.pellse.util.collection.CollectionUtil.intersect;
import static io.github.pellse.util.collection.CollectionUtil.translate;
import static reactor.core.publisher.Mono.fromFuture;

public interface CaffeineCacheHelper {

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(Function<IDC, Publisher<R>> queryFunction) {
        return cached(call(queryFunction));
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource) {
        return cached(ruleMapperSource, newBuilder());
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            Function<IDC, Publisher<R>> queryFunction,
            Caffeine<Object, Object> caffeine) {
        return cached(call(queryFunction), caffeine);
    }

    static <ID, IDC extends Collection<ID>, R, RRC> CachedRuleMapperSource<ID, IDC, R, RRC> cached(
            RuleMapperSource<ID, IDC, R, RRC> ruleMapperSource,
            Caffeine<Object, Object> caffeine) {

        return ruleContext -> {
            final AsyncLoadingCache<ID, Collection<R>> delegateCache = caffeine.buildAsync(
                    bulk((ids, executor) -> Flux.from(
                                    ruleMapperSource.apply(ruleContext).apply(translate(ids, ruleContext.idCollectionFactory())))
                            .collectMultimap(ruleContext.idExtractor())
                            .map(queryResultsMap -> buildCacheFragment(ids, queryResultsMap))
                            .doOnNext(map -> System.out.println("missingIds = " + ids + ", map from query = " + map))
                            .toFuture()));

            System.out.println("AsyncLoadingCache = " + delegateCache);
            return ids -> fromFuture(delegateCache.getAll(ids));
        };
    }

    private static <ID, R> Map<ID, Collection<R>> buildCacheFragment(Set<? extends ID> entityIds, Map<ID, Collection<R>> queryResultsMap) {
        return also(new HashMap<>(queryResultsMap), cacheFragment -> intersect(entityIds, cacheFragment.keySet())
                .forEach(id -> cacheFragment.put(id, List.of())));
    }
}
