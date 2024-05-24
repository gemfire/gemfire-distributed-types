/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import dev.gemfire.dtype.internal.AbstractDType;
import dev.gemfire.dtype.internal.DTypeCollectionsFunction;
import dev.gemfire.dtype.internal.DTypeFunction;
import dev.gemfire.dtype.internal.OperationPerformer;

public class IntegrationTestOperationPerformer implements OperationPerformer {

  @Override
  public <T> T performOperation(DType entry, DTypeFunction fn, boolean isUpdate,
      String gemfireFunctionId) {

    DTypeCollectionsFunction realFn = (DTypeCollectionsFunction) fn;
    Object result = realFn.apply(entry);
    ((AbstractDType) entry).updateEntry();

    return uncheckedCast(result);
  }

}
