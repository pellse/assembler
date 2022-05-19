package io.github.pellse.reactive.assembler;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public interface RuleMapperContext<ID, IDC extends Collection<ID>, R, RRC> extends RuleContext<ID, IDC, R, RRC> {

    record DefaultRuleMapperContext<ID, IDC extends Collection<ID>, R, RRC>(
            Function<R, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory,
            Function<ID, RRC> defaultResultProvider,
            Function<IDC, Collector<R, ?, Map<ID, RRC>>> mapCollector) implements RuleMapperContext<ID, IDC, R, RRC> {
    }

    static <ID, IDC extends Collection<ID>, R, RRC> DefaultRuleMapperContext<ID, IDC, R, RRC> toRuleMapperContext(
            RuleContext<ID, IDC, R, RRC> ruleContext,
            Function<ID, RRC> defaultResultProvider,
            Function<IDC, Collector<R, ?, Map<ID, RRC>>> mapCollector) {

        return new DefaultRuleMapperContext<>(
                ruleContext.idExtractor(),
                ruleContext.idCollectionFactory(),
                ruleContext.mapFactory(),
                defaultResultProvider,
                mapCollector);
    }
}
