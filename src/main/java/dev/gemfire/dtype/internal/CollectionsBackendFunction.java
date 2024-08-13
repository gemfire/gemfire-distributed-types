/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import static dev.gemfire.dtype.internal.OperationType.*;

import java.util.concurrent.Callable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.internal.cache.execute.BucketMovedException;

public class CollectionsBackendFunction implements Function<Object> {

  public static final String ID = "dtype-collections-function";

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext<Object> context) {
    Object[] args = (Object[]) context.getArguments();
    String name = (String) args[0];
    String memberTag = (String) args[1];
    DTypeCollectionsFunction fn = (DTypeCollectionsFunction) args[2];
    OperationType operationType = (OperationType) args[3];

    Region<String, AbstractDType> region = ((RegionFunctionContext) context).getDataSet();
    AbstractDType entry = region.get(name);

    Callable<Object> wrappingFn = () -> {
      Object innerResult;
      if (operationType == QUERY) {
        innerResult = fn.apply(entry);
      } else {
        if (operationType == UPDATE) {
          entry.setDelta(fn);
        }
        innerResult = fn.apply(entry);
        region.put(name, entry);
      }
      return innerResult;
    };

    Object result = null;
    int retrySleepTime;
    long startTime = System.currentTimeMillis();
    do {
      retrySleepTime = 0;
      synchronized (entry) {
        try {
          result = ((PartitionedRegion) region).computeWithPrimaryLocked(name, wrappingFn);
        } catch (RetryableException rex) {
          retrySleepTime = rex.getRetrySleepTime();
          long elapsedTime = System.currentTimeMillis() - startTime;
          if (elapsedTime > rex.getMaxTimeToRetryMs()) {
            result = rex.getFailingResult();
            break;
          }
        } catch (PrimaryBucketLockException | BucketMovedException | RegionDestroyedException ex) {
          throw ex;
        } catch (Exception ex) {
          context.getResultSender().sendException(ex);
          return;
        }
      }
      if (retrySleepTime > 0) {
        try {
          Thread.sleep(retrySleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          context.getResultSender().sendException(new UncheckedInterruptedException(e));
          break;
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
