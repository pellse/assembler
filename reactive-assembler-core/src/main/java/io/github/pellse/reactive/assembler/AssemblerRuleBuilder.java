package io.github.pellse.reactive.assembler;

import java.util.function.Function;

public class AssemblerRuleBuilder<ID, R, RC> {

    @FunctionalInterface
    public interface WithIdExtractorBuilder<ID, R, RC> {
        Mapper<ID, RC> withIdExtractor(Function<R, ID> idExtractor);
    }

    public static <ID, R, RC> WithIdExtractorBuilder<ID, R, RC> rule(MapperBuilder<ID, R, RC> mapperBuilder) {
        return new AssemblerRuleBuilder<>(mapperBuilder)::withIdExtractor;
    }

    private final MapperBuilder<ID, R, RC> mapperBuilder;

    public AssemblerRuleBuilder(MapperBuilder<ID, R, RC> mapperBuilder) {
        this.mapperBuilder = mapperBuilder;
    }

    public Mapper<ID, RC> withIdExtractor(Function<R, ID> idExtractor) {
        return this.mapperBuilder.apply(idExtractor);
    }
}
