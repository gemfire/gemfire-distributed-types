/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import dev.gemfire.dtype.DType;

/**
 * Abstraction that provides the calling interface between clients and servers.
 */
public interface OperationPerformer {

  <T> T performOperation(DType entry, DTypeFunction fn, OperationType operationType,
                         String gemFireFunctionId);

}
