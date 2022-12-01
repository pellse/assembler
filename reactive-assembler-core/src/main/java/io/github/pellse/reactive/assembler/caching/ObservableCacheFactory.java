package io.github.pellse.reactive.assembler.caching;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.github.pellse.reactive.assembler.caching.AdapterCache.adapterCache;
import static io.github.pellse.util.ObjectUtils.then;
import static java.lang.System.getLogger;

public interface ObservableCacheFactory {

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> loggableCache(Level level) {
        return loggableCache(getLogger(ObservableCacheFactory.class.getName()), level);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> loggableCache(String loggerName, Level level) {
        return loggableCache(getLogger(loggerName), level);
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> loggableCache(Logger logger, Level level) {

        Function<String, Consumer<Map<ID, List<R>>>> log = prefix -> map -> {
            if (logger.isLoggable(level)) {
                logger.log(level, "{0}: {1}", prefix, map);
            }
        };

        return observableCache(log.apply("getAll"), log.apply("putAll"), log.apply("removeAll"));
    }

    static <ID, R, RRC> Function<CacheFactory<ID, R, RRC>, CacheFactory<ID, R, RRC>> observableCache(
            Consumer<Map<ID, List<R>>> getAllCallback,
            Consumer<Map<ID, List<R>>> putAllCallback,
            Consumer<Map<ID, List<R>>> removeAllCallback) {

        return cacheFactory -> observableCache(cacheFactory, getAllCallback, putAllCallback, removeAllCallback);
    }

    static <ID, R, RRC> CacheFactory<ID, R, RRC> observableCache(
            CacheFactory<ID, R, RRC> delegateCacheFactory,
            Consumer<Map<ID, List<R>>> getAllCallback,
            Consumer<Map<ID, List<R>>> putAllCallback,
            Consumer<Map<ID, List<R>>> removeAllCallback) {

        return (fetchFunction, context) -> then(delegateCacheFactory.create(fetchFunction, context), cache -> adapterCache(
                (ids, computeIfAbsent) -> cache.getAll(ids, computeIfAbsent).doOnNext(getAllCallback),
                map -> cache.putAll(map).doOnSuccess(__ -> putAllCallback.accept(map)),
                map -> cache.removeAll(map).doOnSuccess(__ -> removeAllCallback.accept(map))));

    }
}
