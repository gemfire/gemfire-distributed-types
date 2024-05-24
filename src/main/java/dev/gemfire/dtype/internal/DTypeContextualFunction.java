/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.Serializable;
import java.util.function.BiFunction;

import dev.gemfire.dtype.DType;

/**
 * Interface for an operation (function) that will act on an {@link AbstractDType} and receive
 * a context as argument.
 */
public interface DTypeContextualFunction extends BiFunction<DType, DTypeFunctionContext, Object>,
    Serializable, DTypeFunction {

}
