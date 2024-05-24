/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import dev.gemfire.dtype.DType;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;

/**
 * Concrete implementation that uses a function to forward the operation to the backend server.
 */
public class FunctionOperationPerformer implements OperationPerformer {

  private final Region<String, Object> region;
  private final String memberId;

  public FunctionOperationPerformer(Region<String, Object> region, String memberId) {
    this.region = region;
    this.memberId = memberId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T performOperation(DType entry, DTypeFunction fn, boolean isUpdate,
      String gemfireFunctionId) {
    Object[] args = new Object[] {entry.getName(), memberId, fn, isUpdate};
    Set<String> filter = Collections.singleton(entry.getName());

    ResultCollector<T, List<T>> collector =
        FunctionService.onRegion(region)
            .withFilter(filter)
            .setArguments(args)
            .execute(gemfireFunctionId);

    T result = collector.getResult().get(0);

    return result;
  }

}
