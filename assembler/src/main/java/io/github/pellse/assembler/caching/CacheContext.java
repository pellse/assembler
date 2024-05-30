package io.github.pellse.assembler.caching;

import io.github.pellse.assembler.RuleMapperContext;
import io.github.pellse.assembler.RuleMapperContext.OneToManyContext;
import io.github.pellse.assembler.RuleMapperContext.OneToOneContext;

import java.util.Collection;

import static io.github.pellse.util.ObjectUtils.also;

public sealed interface CacheContext<ID, EID, R, RRC> {

    RuleMapperContext<?, ?, ID, EID, R, RRC> ctx();

    record OneToOneCacheContext<ID, R>(
            boolean isEmptySource,
            OneToOneContext<?, ?, ID, R> ctx) implements CacheContext<ID, ID, R, R> {
    }

    record OneToManyCacheContext<ID, EID, R, RC extends Collection<R>>(
            boolean isEmptySource,
            Class<RC> collectionType,
            OneToManyContext<?, ?, ID, EID, R, RC> ctx) implements CacheContext<ID, EID, R, RC> {

        @SuppressWarnings("unchecked")
        public OneToManyCacheContext(boolean isEmptySource, OneToManyContext<?, ?, ID, EID, R, RC> ctx) {
            this(isEmptySource, (Class<RC>) ctx.collectionFactory().get().getClass(), ctx);
        }

        @SuppressWarnings("unchecked")
        public RC convert(Collection<R> collection) {
            return collectionType().isInstance(collection) ? (RC) collection : also(ctx().collectionFactory().get(), c -> c.addAll(collection));
        }
    }
}
