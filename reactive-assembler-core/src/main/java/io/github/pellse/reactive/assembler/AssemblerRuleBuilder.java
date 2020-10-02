package io.github.pellse.reactive.assembler;

import java.util.function.Function;

public class AssemblerRuleBuilder<ID, R, RC, EX extends Throwable> {

    @FunctionalInterface
    public interface WithIdExtractorBuilder<ID, R, RC, EX extends Throwable> {
        Mapper<ID, RC, EX> withIdExtractor(Function<R, ID> idExtractor);
    }

    public static <ID, R, RC, EX extends Throwable> WithIdExtractorBuilder<ID, R, RC, EX> rule(MapperBuilder<ID, R, RC, EX> mapperBuilder) {
        return new AssemblerRuleBuilder<>(mapperBuilder)::withIdExtractor;
    }

    private final MapperBuilder<ID, R, RC, EX> mapperBuilder;

    public AssemblerRuleBuilder(MapperBuilder<ID, R, RC, EX> mapperBuilder) {
        this.mapperBuilder = mapperBuilder;
    }

    public Mapper<ID, RC, EX> withIdExtractor(Function<R, ID> idExtractor) {
        return this.mapperBuilder.apply(idExtractor);
    }
}
