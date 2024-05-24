/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.Serializable;
import java.util.function.Consumer;

public interface SerializableConsumer<E> extends Consumer<E>, Serializable {
}
