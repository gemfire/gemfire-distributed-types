/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.Serializable;
import java.util.function.Function;

import dev.gemfire.dtype.DType;

public interface DTypeCollectionsFunction extends Function<DType, Object>, Serializable,
    DTypeFunction {

}
