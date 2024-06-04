/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

/**
 * A distributed and highly-available implementation of {@code AtomicLong}s.
 */
public interface DAtomicLong {

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  long get();

  /**
   * Set the given value atomically.
   *
   * @param value the value to set
   */
  void set(long value);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  long getAndAdd(long delta);

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  long getAndSet(long newValue);

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  long addAndGet(long delta);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful. False return indicates that
   *         the actual value was not equal to the expected value.
   */
  boolean compareAndSet(long expect, long update);

}
