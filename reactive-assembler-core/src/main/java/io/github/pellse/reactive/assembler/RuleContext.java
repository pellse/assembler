package io.github.pellse.reactive.assembler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.pellse.reactive.assembler.MapFactory.defaultMapFactory;

/**
 *
 * @param idExtractor
 * @param idCollectionFactory
 * @param mapFactory
 * @param <ID> Correlation Id type
 * @param <IDC> Collection of correlation ids type (e.g. {@code List<ID>}, {@code Set<ID>})
 * @param <R> Type of the publisher elements returned from {@code queryFunction}
 * @param <RRC> Either R or collection of R (e.g. R vs. {@code List<R>})
 */
public record RuleContext<ID, IDC extends Collection<ID>, R, RRC>(
        Function<R, ID> idExtractor,
        Supplier<IDC> idCollectionFactory,
        MapFactory<ID, RRC> mapFactory) {

    public static <ID, R, RRC> RuleContext<ID, List<ID>, R, RRC> ruleContext(Function<R, ID> idExtractor) {
        return ruleContext(idExtractor, ArrayList::new);
    }

    public static <ID, IDC extends Collection<ID>, R, RRC> RuleContext<ID, IDC, R, RRC> ruleContext(
            Function<R, ID> idExtractor,
            Supplier<IDC> idCollectionFactory) {
        return ruleContext(idExtractor, idCollectionFactory, defaultMapFactory());
    }

    public static <ID, IDC extends Collection<ID>, R, RRC> RuleContext<ID, IDC, R, RRC> ruleContext(
            Function<R, ID> idExtractor,
            Supplier<IDC> idCollectionFactory,
            MapFactory<ID, RRC> mapFactory) {
        return new RuleContext<>(idExtractor, idCollectionFactory, mapFactory);
    }
}
