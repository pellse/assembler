package io.github.pellse.assembler;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface AssemblerAdapter<ID, M, RC> {

    M convertMapperSupplier(Supplier<Map<ID, ?>> mapperSupplier);

    <R> RC convertMapperSources(List<M> sources, Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder);
}
