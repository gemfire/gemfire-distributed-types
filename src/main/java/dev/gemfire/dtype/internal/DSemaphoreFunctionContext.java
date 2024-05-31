/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.io.Serializable;

/**
 * Concrete implementation of a {@link DTypeFunctionContext} used to hold the calling client's
 * memberTag and the {@link DSemaphoreTracker}
 */
public class DSemaphoreFunctionContext implements DTypeFunctionContext, Serializable {

  private final String memberTag;
  private final DSemaphoreTracker tracker;

  public DSemaphoreFunctionContext(String memberTag, DSemaphoreTracker tracker) {
    this.memberTag = memberTag;
    this.tracker = tracker;
  }

  public String getMemberTag() {
    return memberTag;
  }

  public DSemaphoreTracker getTracker() {
    return tracker;
  }

}
