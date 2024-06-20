/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An exception used to signal that the executing operation should be retried.
 */
public class RetryableException extends RuntimeException {

  private static final Supplier<Object> NULL_SUPPLIER = () -> null;
  private final int retrySleepTime;
  private final long maxTimeToRetry;
  private final TimeUnit timeUnit;
  private final Supplier<Object> failingResult;

  public RetryableException(int retrySleepTime) {
    this(retrySleepTime, Long.MAX_VALUE, TimeUnit.MILLISECONDS, NULL_SUPPLIER);
  }

  public RetryableException(int retrySleepTime, long maxTimeToRetry, TimeUnit timeUnit) {
    this(retrySleepTime, maxTimeToRetry, timeUnit, NULL_SUPPLIER);
  }

  public RetryableException(int retrySleepTime, long maxTimeToRetry, TimeUnit timeUnit,
      Supplier<Object> failingResult) {
    this.retrySleepTime = retrySleepTime;
    this.maxTimeToRetry = maxTimeToRetry;
    this.timeUnit = timeUnit;
    this.failingResult = failingResult;
  }

  public int getRetrySleepTime() {
    return retrySleepTime;
  }

  public long getMaxTimeToRetryMs() {
    return timeUnit.toMillis(maxTimeToRetry);
  }

  public Object getFailingResult() {
    return failingResult.get();
  }

}
