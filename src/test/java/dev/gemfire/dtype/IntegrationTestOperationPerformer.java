/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import dev.gemfire.dtype.internal.AbstractDType;
import dev.gemfire.dtype.internal.DTypeCollectionsFunction;
import dev.gemfire.dtype.internal.DTypeFunction;
import dev.gemfire.dtype.internal.OperationPerformer;
import dev.gemfire.dtype.internal.RetryableException;

public class IntegrationTestOperationPerformer implements OperationPerformer {

  @Override
  public <T> T performOperation(DType entry, DTypeFunction fn, boolean isUpdate,
      String gemfireFunctionId) {

    DTypeCollectionsFunction realFn = (DTypeCollectionsFunction) fn;
    Object result = null;
    int retrySleepTime = 0;
    do {
      try {
        result = realFn.apply(entry);
      } catch (RetryableException rex) {
        retrySleepTime = rex.getRetrySleepTime();
        try {
          Thread.sleep(retrySleepTime);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    } while (retrySleepTime > 0);

    ((AbstractDType) entry).updateEntry();

    return uncheckedCast(result);
  }

}
