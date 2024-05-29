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

public class CollectionsBackendFunction implements Function<Object> {

  public static final String ID = "dtype-collections-function";

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext<Object> context) {
    Object[] args = (Object[]) context.getArguments();
    String name = (String) args[0];
    String memberId = (String) args[1];
    DTypeCollectionsFunction fn = (DTypeCollectionsFunction) args[2];
    boolean isUpdate = (boolean) args[3];

    Region<String, AbstractDType> region = ((RegionFunctionContext) context).getDataSet();
    AbstractDType entry = region.get(name);

    Callable<Object> wrappingFn = () -> {
      Object innerResult;
      if (isUpdate) {
        entry.setDelta(fn);
        innerResult = fn.apply(entry);
        region.put(name, entry);
      } else {
        innerResult = fn.apply(entry);
      }
      return innerResult;
    };

    Object result = null;
    int retrySleepTime;
    do {
      retrySleepTime = 0;
      synchronized (entry) {
        try {
          result = ((PartitionedRegion) region).computeWithPrimaryLocked(name, wrappingFn);
        } catch (RetryableException rex) {
          retrySleepTime = rex.getRetrySleepTime();
        } catch (Exception ex) {
          throw new MarkerException(ex);
        }
      }
      if (retrySleepTime > 0) {
        try {
          Thread.sleep(retrySleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new MarkerException(e);
        }
      }
    } while (retrySleepTime > 0);

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
