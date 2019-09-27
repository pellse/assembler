package io.github.pellse.util.query;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface MapFactory<ID, R> extends Function<Collection<ID>, Map<ID, R>> {
}
