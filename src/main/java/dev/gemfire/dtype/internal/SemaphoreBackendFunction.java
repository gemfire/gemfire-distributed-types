/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import static dev.gemfire.dtype.internal.OperationType.UPDATE;

import java.util.concurrent.Callable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.internal.cache.execute.BucketMovedException;

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
    OperationType operationType = (OperationType) args[3];

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
      if (operationType == UPDATE) {
        region.put(name, finalEntry);
      }
      return innerResult;
    };

    Object result;
    synchronized (finalEntry) {
      try {
        result = ((PartitionedRegion) region).computeWithPrimaryLocked(name, wrappingFn);
      } catch (PrimaryBucketLockException | BucketMovedException | RegionDestroyedException ex) {
        throw ex;
      } catch (Exception ex) {
        context.getResultSender().sendException(ex);
        return;
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
