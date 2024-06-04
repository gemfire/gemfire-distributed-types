/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.util.concurrent.Callable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;

public class SemaphoreBackendFunction implements Function<Object> {

  public static final String ID = "dsemaphore-function";

  private final DSemaphoreTracker tracker;

  public SemaphoreBackendFunction(DSemaphoreTracker tracker) {
    this.tracker = tracker;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext<Object> context) {
    Object[] args = (Object[]) context.getArguments();
    String name = (String) args[0];
    String memberTag = (String) args[1];
    DTypeContextualFunction fn = (DTypeContextualFunction) args[2];
    boolean isUpdate = (boolean) args[3];

    Region<String, AbstractDType> region = ((RegionFunctionContext) context).getDataSet();
    AbstractDType entry = region.get(name);

    if (entry == null) {
      entry = new DSemaphoreBackend(name);
    } else {
      ((DSemaphoreBackend) entry).recoverTrackingIfNeeded(tracker);
    }

    AbstractDType finalEntry = entry;
    Callable<Object> wrappingFn = () -> {
      Object innerResult = fn.apply(finalEntry, new DSemaphoreFunctionContext(memberTag, tracker));
      if (isUpdate) {
        region.put(name, finalEntry);
      }
      return innerResult;
    };

    Object result;
    synchronized (entry) {
      try {
        result = ((PartitionedRegion) region).computeWithPrimaryLocked(name, wrappingFn);
      } catch (Exception e) {
        throw new MarkerException(e);
      }
    }

    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }
}
