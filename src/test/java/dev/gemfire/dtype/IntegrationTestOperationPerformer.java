/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import dev.gemfire.dtype.internal.AbstractDType;
import dev.gemfire.dtype.internal.DTypeCollectionsFunction;
import dev.gemfire.dtype.internal.DTypeFunction;
import dev.gemfire.dtype.internal.MarkerException;
import dev.gemfire.dtype.internal.OperationPerformer;
import dev.gemfire.dtype.internal.RetryableException;

public class IntegrationTestOperationPerformer implements OperationPerformer {

  @Override
  public <T> T performOperation(DType entry, DTypeFunction fn, boolean isUpdate,
      String gemfireFunctionId) {

    DTypeCollectionsFunction realFn = (DTypeCollectionsFunction) fn;
    Object result = null;
    int retrySleepTime;
    long startTime = System.currentTimeMillis();
    do {
      retrySleepTime = 0;
      try {
        result = realFn.apply(entry);
      } catch (RetryableException rex) {
        retrySleepTime = rex.getRetrySleepTime();
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime > rex.getMaxTimeToRetryMs()) {
          result = rex.getFailingResult();
          break;
        }
        try {
          Thread.sleep(retrySleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new MarkerException(e);
        }
      }
    } while (retrySleepTime > 0);

    ((AbstractDType) entry).updateEntry();

    return uncheckedCast(result);
  }

}
