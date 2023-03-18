package io.github.pellse.reactive.assembler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collector;

public interface RuleMapperContext<ID, EID, IDC extends Collection<ID>, R, RRC> extends RuleContext<ID, IDC, R, RRC> {

    static <ID, EID, IDC extends Collection<ID>, R, RRC> DefaultRuleMapperContext<ID, EID, IDC, R, RRC> toRuleMapperContext(
            IdAwareRuleContext<ID, EID, IDC, R, RRC> ruleContext,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) {

        return new DefaultRuleMapperContext<>(
                ruleContext.idExtractor(),
                ruleContext.correlationIdExtractor(),
                ruleContext.idCollectionFactory(),
                ruleContext.mapFactory(),
                defaultResultProvider,
                mapCollector,
                fromListConverter,
                toListConverter);
    }

    Function<R, EID> idExtractor();

    Function<ID, RRC> defaultResultProvider();

    IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector();

    Function<List<R>, RRC> fromListConverter();

    Function<RRC, List<R>> toListConverter();

    record DefaultRuleMapperContext<ID, EID, IDC extends Collection<ID>, R, RRC>(
            Function<R, EID> idExtractor,
            Function<R, ID> correlationIdExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            Function<ID, RRC> defaultResultProvider,
            IntFunction<Collector<R, ?, Map<ID, RRC>>> mapCollector,
            Function<List<R>, RRC> fromListConverter,
            Function<RRC, List<R>> toListConverter) implements RuleMapperContext<ID, EID, IDC, R, RRC> {
    }
}
