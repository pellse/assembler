package io.github.pellse.assembler.synchronous;

import io.github.pellse.assembler.AssemblerAdapter;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.stream.Collectors.toList;

class SynchronousAssemblerAdapter<ID> implements AssemblerAdapter<ID, Supplier<Map<ID, ?>>, Stream<?>> {

    @Override
    public Supplier<Map<ID, ?>> convertMapperSupplier(Supplier<Map<ID, ?>> mapperSupplier) {
        return mapperSupplier;
    }

    @Override
    public <R> Stream<R> convertMapperSources(List<Supplier<Map<ID, ?>>> sources,
                                              Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder,
                                              Function<Throwable, RuntimeException> errorConverter) {
        try {
            return domainObjectStreamBuilder.apply(sources.stream()
                    .map(Supplier::get)
                    .collect(toList()));
        } catch(Throwable t) {
            return sneakyThrow(errorConverter.apply(t));
        }
    }
}
