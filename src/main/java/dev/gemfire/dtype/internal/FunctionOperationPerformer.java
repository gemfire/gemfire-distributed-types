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
  private final String memberTag;

  public FunctionOperationPerformer(Region<String, Object> region, String memberTag) {
    this.region = region;
    this.memberTag = memberTag;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T performOperation(DType entry, DTypeFunction fn, OperationType operationType,
      String gemfireFunctionId) {
    Object[] args = new Object[] {entry.getName(), memberTag, fn, operationType};
    Set<String> filter = Collections.singleton(entry.getName());

    T result;
    ResultCollector<T, List<T>> collector =
        FunctionService.onRegion(region)
            .withFilter(filter)
            .setArguments(args)
            .execute(gemfireFunctionId);

    result = collector.getResult().get(0);
    if (result instanceof RuntimeException) {
      throw (RuntimeException) result;
    }

    return result;
  }

}
