package io.github.pellse.assembler.flux;

import io.github.pellse.assembler.AssemblerAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.github.pellse.util.ExceptionUtils.sneakyThrow;
import static java.util.stream.Collectors.toList;

class FluxAssemblerAdapter<ID> implements AssemblerAdapter<ID, Mono<Map<ID, ?>>, Flux<?>> {

    @Override
    public Mono<Map<ID, ?>> convertMapperSupplier(Supplier<Map<ID, ?>> mapperSupplier) {
        return Mono.fromSupplier(mapperSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> Flux<R> convertMapperSources(List<Mono<Map<ID, ?>>> sources,
                                            Function<List<Map<ID, ?>>, Stream<R>> domainObjectStreamBuilder,
                                            Function<Throwable, RuntimeException> errorConverter) {
        return Flux.zip(sources, mapperResults -> domainObjectStreamBuilder.apply(
                Stream.of(mapperResults)
                        .map(mapResult -> (Map<ID, ?>) mapResult)
                        .collect(toList())))
                .flatMap(Flux::fromStream)
                .doOnError(e -> sneakyThrow(errorConverter.apply(e)));
    }
}
