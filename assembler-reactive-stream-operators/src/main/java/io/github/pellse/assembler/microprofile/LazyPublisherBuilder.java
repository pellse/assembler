package io.github.pellse.assembler.microprofile;

import org.eclipse.microprofile.reactive.streams.operators.CompletionRunner;
import org.eclipse.microprofile.reactive.streams.operators.ProcessorBuilder;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.eclipse.microprofile.reactive.streams.operators.spi.ReactiveStreamsEngine;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.*;
import java.util.stream.Collector;

public final class LazyPublisherBuilder<T> implements PublisherBuilder<T> {

    private final Supplier<PublisherBuilder<T>> publisherBuilderSupplier;

    private LazyPublisherBuilder(Supplier<PublisherBuilder<T>> publisherBuilderSupplier) {
        this.publisherBuilderSupplier = publisherBuilderSupplier;
    }

    public static <T> LazyPublisherBuilder<T> lazyPublisherBuilder(Supplier<PublisherBuilder<T>> publisherBuilderSupplier) {
        return new LazyPublisherBuilder<>(publisherBuilderSupplier);
    }

    @Override
    public <R> LazyPublisherBuilder<R> map(Function<? super T, ? extends R> mapper) {
        return buildLazyPublisherBuilder(p -> p.map(mapper));
    }

    @Override
    public <S> LazyPublisherBuilder<S> flatMap(Function<? super T, ? extends PublisherBuilder<? extends S>> mapper) {
        return buildLazyPublisherBuilder(p -> p.flatMap(mapper));
    }

    @Override
    public <S> LazyPublisherBuilder<S> flatMapRsPublisher(Function<? super T, ? extends Publisher<? extends S>> mapper) {
        return buildLazyPublisherBuilder(p -> p.flatMapRsPublisher(mapper));
    }

    @Override
    public <S> LazyPublisherBuilder<S> flatMapCompletionStage(Function<? super T, ? extends CompletionStage<? extends S>> mapper) {
        return buildLazyPublisherBuilder(p -> p.flatMapCompletionStage(mapper));
    }

    @Override
    public <S> LazyPublisherBuilder<S> flatMapIterable(Function<? super T, ? extends Iterable<? extends S>> mapper) {
        return buildLazyPublisherBuilder(p -> p.flatMapIterable(mapper));
    }

    @Override
    public LazyPublisherBuilder<T> filter(Predicate<? super T> predicate) {
        return buildLazyPublisherBuilder(p -> p.filter(predicate));
    }

    @Override
    public LazyPublisherBuilder<T> distinct() {
        return buildLazyPublisherBuilder(PublisherBuilder::distinct);
    }

    @Override
    public LazyPublisherBuilder<T> limit(long maxSize) {
        return buildLazyPublisherBuilder(p -> p.limit(maxSize));
    }

    @Override
    public LazyPublisherBuilder<T> skip(long n) {
        return buildLazyPublisherBuilder(p -> p.skip(n));
    }

    @Override
    public LazyPublisherBuilder<T> takeWhile(Predicate<? super T> predicate) {
        return buildLazyPublisherBuilder(p -> p.takeWhile(predicate));
    }

    @Override
    public LazyPublisherBuilder<T> dropWhile(Predicate<? super T> predicate) {
        return buildLazyPublisherBuilder(p -> p.dropWhile(predicate));
    }

    @Override
    public LazyPublisherBuilder<T> peek(Consumer<? super T> consumer) {
        return buildLazyPublisherBuilder(p -> p.peek(consumer));
    }

    @Override
    public LazyPublisherBuilder<T> onError(Consumer<Throwable> errorHandler) {
        return buildLazyPublisherBuilder(p -> p.onError(errorHandler));
    }

    @Override
    public LazyPublisherBuilder<T> onTerminate(Runnable action) {
        return buildLazyPublisherBuilder(p -> p.onTerminate(action));
    }

    @Override
    public LazyPublisherBuilder<T> onComplete(Runnable action) {
        return buildLazyPublisherBuilder(p -> p.onComplete(action));
    }

    @Override
    public CompletionRunner<Void> forEach(Consumer<? super T> action) {
        return buildCompletionRunner(p -> p.forEach(action));
    }

    @Override
    public CompletionRunner<Void> ignore() {
        return buildCompletionRunner(PublisherBuilder::ignore);
    }

    @Override
    public CompletionRunner<Void> cancel() {
        return buildCompletionRunner(PublisherBuilder::cancel);
    }

    @Override
    public CompletionRunner<T> reduce(T identity, BinaryOperator<T> accumulator) {
        return buildCompletionRunner(p -> p.reduce(identity, accumulator));
    }

    @Override
    public CompletionRunner<Optional<T>> reduce(BinaryOperator<T> accumulator) {
        return buildCompletionRunner(p -> p.reduce(accumulator));
    }

    @Override
    public CompletionRunner<Optional<T>> findFirst() {
        return buildCompletionRunner(PublisherBuilder::findFirst);
    }

    @Override
    public <R, A> CompletionRunner<R> collect(Collector<? super T, A, R> collector) {
        return buildCompletionRunner(p -> p.collect(collector));
    }

    @Override
    public <R> CompletionRunner<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator) {
        return buildCompletionRunner(p -> p.collect(supplier, accumulator));
    }

    @Override
    public CompletionRunner<List<T>> toList() {
        return buildCompletionRunner(PublisherBuilder::toList);
    }

    @Override
    public LazyPublisherBuilder<T> onErrorResume(Function<Throwable, ? extends T> errorHandler) {
        return buildLazyPublisherBuilder(p -> p.onErrorResume(errorHandler));
    }

    @Override
    public LazyPublisherBuilder<T> onErrorResumeWith(Function<Throwable, ? extends PublisherBuilder<? extends T>> errorHandler) {
        return buildLazyPublisherBuilder(p -> p.onErrorResumeWith(errorHandler));
    }

    @Override
    public LazyPublisherBuilder<T> onErrorResumeWithRsPublisher(Function<Throwable, ? extends Publisher<? extends T>> errorHandler) {
        return buildLazyPublisherBuilder(p -> p.onErrorResumeWithRsPublisher(errorHandler));
    }

    @Override
    public CompletionRunner<Void> to(Subscriber<? super T> subscriber) {
        return buildCompletionRunner(p -> p.to(subscriber));
    }

    @Override
    public <R> CompletionRunner<R> to(SubscriberBuilder<? super T, ? extends R> subscriberBuilder) {
        return buildCompletionRunner(p -> p.to(subscriberBuilder));
    }

    @Override
    public <R> LazyPublisherBuilder<R> via(ProcessorBuilder<? super T, ? extends R> processorBuilder) {
        return buildLazyPublisherBuilder(p -> p.via(processorBuilder));
    }

    @Override
    public <R> LazyPublisherBuilder<R> via(Processor<? super T, ? extends R> processor) {
        return buildLazyPublisherBuilder(p -> p.via(processor));
    }

    @Override
    public Publisher<T> buildRs() {
        return buildPublisher(PublisherBuilder::buildRs);
    }

    @Override
    public Publisher<T> buildRs(ReactiveStreamsEngine engine) {
        return buildPublisher(p -> p.buildRs(engine));
    }

    private <R> LazyPublisherBuilder<R> buildLazyPublisherBuilder(Function<PublisherBuilder<T>, PublisherBuilder<R>> publisherBuilderMapper) {
        return lazyPublisherBuilder(() -> invoke(publisherBuilderMapper));
    }

    private Publisher<T> buildPublisher(Function<PublisherBuilder<T>, Publisher<T>> publisherBuilderMapper) {
        return subscriber -> invoke(publisherBuilderMapper).subscribe(subscriber);
    }

    private <R> CompletionRunner<R> buildCompletionRunner(Function<PublisherBuilder<T>, CompletionRunner<R>> publisherBuilderMapper) {
        return new CompletionRunner<>() {
            @Override
            public CompletionStage<R> run() {
                return invoke(publisherBuilderMapper).run();
            }

            @Override
            public CompletionStage<R> run(ReactiveStreamsEngine engine) {
                return invoke(publisherBuilderMapper).run(engine);
            }
        };
    }

    private <R> R invoke(Function<PublisherBuilder<T>, R> publisherBuilderMapper) {
        return publisherBuilderMapper.apply(publisherBuilderSupplier.get());
    }
}
