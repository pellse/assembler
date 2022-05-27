package io.github.pellse.reactive.assembler;

import io.github.pellse.reactive.assembler.caching.MergeStrategy;
import io.github.pellse.reactive.assembler.caching.RemoveStrategy;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

public interface RuleMapperContext<ID, IDC extends Collection<ID>, R, RRC> extends RuleContext<ID, IDC, R, RRC> {

    Function<ID, RRC> defaultResultProvider();

    Function<Integer, Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<Stream<RRC>, Stream<R>> streamFlattener();

    MergeStrategy<ID, RRC> mergeStrategy();

    RemoveStrategy<ID, RRC> removeStrategy();

    record DefaultRuleMapperContext<ID, IDC extends Collection<ID>, R, RRC>(
            Function<R, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            Function<ID, RRC> defaultResultProvider,
            Function<Integer, Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<Stream<RRC>, Stream<R>> streamFlattener,
            MergeStrategy<ID, RRC> mergeStrategy,
            RemoveStrategy<ID, RRC> removeStrategy) implements RuleMapperContext<ID, IDC, R, RRC> {
    }

    static <ID, IDC extends Collection<ID>, R, RRC> DefaultRuleMapperContext<ID, IDC, R, RRC> toRuleMapperContext(
            RuleContext<ID, IDC, R, RRC> ruleContext,
            Function<ID, RRC> defaultResultProvider,
            Function<Integer, Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<Stream<RRC>, Stream<R>> streamFlattener,
            MergeStrategy<ID, RRC> mergeStrategy,
            RemoveStrategy<ID, RRC> removeStrategy) {

        return new DefaultRuleMapperContext<>(
                ruleContext.idExtractor(),
                ruleContext.idCollectionFactory(),
                ruleContext.mapFactory(),
                defaultResultProvider,
                mapCollector,
                streamFlattener,
                mergeStrategy,
                removeStrategy);
    }
}
