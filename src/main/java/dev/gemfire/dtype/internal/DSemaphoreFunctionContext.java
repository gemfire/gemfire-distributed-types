/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.Serializable;

/**
 * Concrete implementation of a {@link DTypeFunctionContext} used to hold the calling client's
 * memberId and the {@link DSemaphoreTracker}
 */
public class DSemaphoreFunctionContext implements DTypeFunctionContext, Serializable {

  private final String memberId;
  private final DSemaphoreTracker tracker;

  public DSemaphoreFunctionContext(String memberId, DSemaphoreTracker tracker) {
    this.memberId = memberId;
    this.tracker = tracker;
  }

  public String getMemberId() {
    return memberId;
  }

  public DSemaphoreTracker getTracker() {
    return tracker;
  }

}
