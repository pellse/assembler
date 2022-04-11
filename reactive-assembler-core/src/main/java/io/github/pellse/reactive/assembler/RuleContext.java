package io.github.pellse.reactive.assembler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

record RuleContext<ID, IDC extends Collection<ID>, T>(Function<T, ID> idExtractor, Supplier<IDC> idCollectionFactory) {

    public static <ID, T> RuleContext<ID, List<ID>, T> ruleContext(Function<T, ID> idExtractor) {
        return new RuleContext<>(idExtractor, ArrayList::new);
    }

    public static <ID, IDC extends Collection<ID>, T> RuleContext<ID, IDC, T> ruleContext(
            Function<T, ID> idExtractor,
            Supplier<IDC> idCollectionFactory) {
        return new RuleContext<>(idExtractor, idCollectionFactory);
    }
}
