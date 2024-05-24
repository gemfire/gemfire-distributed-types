/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;

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

    Object result;
    synchronized (entry) {
      if (isUpdate) {
        entry.setDelta(fn);
        result = fn.apply(entry);
        region.put(name, entry);
      } else {
        result = fn.apply(entry);
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
