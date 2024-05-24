/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

public interface DAtomicLong {

  long get();

  void set(long value);

  long getAndAdd(long delta);

  boolean compareAndSet(long expect, long update);

}
