/*
 * Copyright 2024 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.concurrent;

import io.github.pellse.util.DelegateAware;
import io.github.pellse.util.ObjectUtils;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

import static java.util.Map.entry;
import static reactor.core.publisher.Mono.fromRunnable;

public interface Lock<L extends CoreLock<L>> extends DelegateAware<L> {

    long token();

    CoreLock<?> outerLock();

    Consumer<L> lockReleaser();

    Thread acquiredOnThread();

    default Mono<?> release() {
        return fromRunnable(() -> lockReleaser().accept(delegate()));
    }

    @Override
    @SuppressWarnings("unchecked")
    default L delegate() {
        return (L) this;
    }

    default String log() {
        return ObjectUtils.toString(this,
                entry("token", token()),
                entry("outerLock", outerLock().log()),
                entry("acquiredOnThread", acquiredOnThread().getName()));
    }
}