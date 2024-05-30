/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import java.util.function.BiFunction;

import dev.gemfire.dtype.internal.DAtomicLongImpl;
import dev.gemfire.dtype.internal.DBlockingQueueImpl;
import dev.gemfire.dtype.internal.DCircularQueueImpl;
import dev.gemfire.dtype.internal.DListImpl;
import dev.gemfire.dtype.internal.DSemaphoreImpl;
import dev.gemfire.dtype.internal.DSetImpl;
import dev.gemfire.dtype.internal.FunctionOperationPerformer;
import dev.gemfire.dtype.internal.OperationPerformer;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class DTypeFactory {

  public static final String DTYPES_REGION = "DTYPES";

  private GemFireCacheImpl cache;
  private Region<String, Object> region;
  private OperationPerformer operationPerformer;

  public DTypeFactory(GemFireCache cache) {
    this(cache, FunctionOperationPerformer::new);
  }

  public DTypeFactory(GemFireCache cache,
      BiFunction<Region<String, Object>, String, OperationPerformer> performerFunctionFactory) {
    this.cache = (GemFireCacheImpl) cache;

    if (this.cache.isClient()) {
      region = this.cache.<String, Object>createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(DTYPES_REGION);
    } else {
      region = this.cache.getRegion(DTYPES_REGION);
    }

    String memberId = this.cache.getDistributedSystem().getDistributedMember().getUniqueId();
    this.operationPerformer = performerFunctionFactory.apply(region, memberId);
  }

  public void destroy(String name) {
    region.destroy(name);
  }

  public DAtomicLong createAtomicLong(String name) {
    DAtomicLongImpl value =
        (DAtomicLongImpl) region.computeIfAbsent(name, DAtomicLongImpl::new);
    value.initialize(region, operationPerformer);

    return value;
  }

  @SuppressWarnings("unchecked")
  public <E> DList<E> createDList(String name) {
    DListImpl<E> value =
        (DListImpl<E>) region.computeIfAbsent(name, DListImpl::new);
    value.initialize(region, operationPerformer);

    return value;
  }

  @SuppressWarnings("unchecked")
  public <E> DSet<E> createDSet(String name) {
    DSetImpl<E> value =
        (DSetImpl<E>) region.computeIfAbsent(name, DSetImpl::new);
    value.initialize(region, operationPerformer);

    return value;
  }

  public DSemaphore createDSemaphore(String name, int permits) {
    DSemaphoreImpl value = new DSemaphoreImpl(name);
    value.initialize(region, operationPerformer);
    value.setPermits(permits);

    return value;
  }

  @SuppressWarnings("unchecked")
  public <E> DBlockingQueue<E> createDQueue(String name) {
    DBlockingQueueImpl<E> value =
        (DBlockingQueueImpl<E>) region.computeIfAbsent(name, DBlockingQueueImpl::new);
    value.initialize(region, operationPerformer);

    return value;
  }

  @SuppressWarnings("unchecked")
  public <E> DBlockingQueue<E> createDQueue(String name, int capacity) {
    DBlockingQueueImpl<E> value =
        (DBlockingQueueImpl<E>) region.computeIfAbsent(name,
            r -> new DBlockingQueueImpl<>(name, capacity));
    value.initialize(region, operationPerformer);

    return value;
  }

  @SuppressWarnings("unchecked")
  public <E> DCircularQueue<E> createDCircularQueue(String name, int capacity) {
    DCircularQueueImpl<E> value =
        (DCircularQueueImpl<E>) region.computeIfAbsent(name,
            r -> new DCircularQueueImpl<>(name, capacity));
    value.initialize(region, operationPerformer);

    return value;
  }

}
