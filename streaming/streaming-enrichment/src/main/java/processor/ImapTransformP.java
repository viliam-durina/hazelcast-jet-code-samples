/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package processor;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;

import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class ImapTransformP<T, K, V, R> extends AbstractProcessor {

    // parallel operations limit is per vertex. Better would be per member and target cluster, but we
    // don't have enough information.
    private static final int MAX_OPS = 1000;

    private final String iMapName;
    private final DistributedFunction<? super T, ? extends K> extractKeyFn;
    private final DistributedBiFunction<? super T, ? super V, ? extends Traverser<? extends R>> mapFn;

    private int maxOps;
    private ArrayDeque<T> items;
    private ArrayDeque<ICompletableFuture<V>> futures;
    private IMapJet<K, V> map;
    private Traverser<? extends R> traverser = empty();

    private ImapTransformP(
            String iMapName,
            DistributedFunction<? super T, ? extends K> extractKeyFn,
            DistributedBiFunction<? super T, ? super V, ? extends Traverser<? extends R>> mapFn
    ) {
        this.iMapName = iMapName;
        this.extractKeyFn = extractKeyFn;
        this.mapFn = mapFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        map = context.jetInstance().getMap(iMapName);
        maxOps = MAX_OPS / context.localParallelism();
        items = new ArrayDeque<>(maxOps);
        futures = new ArrayDeque<>(maxOps);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (futures.size() == maxOps) {
            // if queue is full, apply backpressure
            return false;
        }
        K key = extractKeyFn.apply((T) item);
        if (key == null) {
            return true;
        }
        futures.add(map.getAsync(key));
        items.add((T) item);
        return true;
    }

    @Override
    public boolean tryProcess() {
        // finish the traverser first
        if (!emitFromTraverser(traverser)) {
            // we return true - we can accept more items even while emitting this item
            return true;
        }
        // we check the futures in submission order. While this might increase latency if some
        // later-submitted item will get the result earlier, on the other hand we don't have to
        // do many volatile reads to check the futures in each call.
        for (ICompletableFuture<V> f; (f = futures.peek()) != null; ) {
            if (!f.isDone()) {
                return true;
            }
            f = futures.remove();
            V value;
            try {
                value = f.get();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
            traverser = mapFn.apply(items.remove(), value);
        }
        emitFromTraverser(traverser);
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        // TODO we need to save queued items - we consumed them but did not yet
        // produce output for them
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        // TODO
    }

    public static <T, K, V, R> DistributedSupplier<Processor> flatMapUsingImap(
            String iMapName,
            DistributedFunction<? super T, ? extends K> extractKeyFn,
            DistributedBiFunction<? super T, ? super V, ? extends Traverser<? extends R>> mapFn
    ) {
        return () -> new ImapTransformP<>(iMapName, extractKeyFn, mapFn);
    }

    /**
     * Map using the stream items using a value stored in an IMap.
     *
     * @param iMapName the name if the IMap
     * @param extractKeyFn function to extract the key from stream item
     * @param mapFn function that takes the stream item and value read from
     *              the imap and produces a result (the result can be null to filter
     *              the item out)
     */
    public static <T, K, V, R> DistributedSupplier<Processor> mapUsingImap(
            String iMapName,
            DistributedFunction<? super T, ? extends K> extractKeyFn,
            DistributedBiFunction<? super T, ? super V, ? extends R> mapFn
    ) {
        return () -> {
            ResettableSingletonTraverser<R> traverser = new ResettableSingletonTraverser<>();
            DistributedBiFunction<T, V, Traverser<R>> mapFn2 = (t1, t2) -> {
                R mapped = mapFn.apply(t1, t2);
                traverser.accept(mapped);
                return traverser;
            };
            return new ImapTransformP<>(iMapName, extractKeyFn, mapFn2);
        };
    }
}
