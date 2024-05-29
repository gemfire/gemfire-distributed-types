/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

public class RetryableException extends RuntimeException {

  private final int retrySleepTime;

  public RetryableException(int retrySleepTime) {
    this.retrySleepTime = retrySleepTime;
  }

  public int getRetrySleepTime() {
    return retrySleepTime;
  }

}
