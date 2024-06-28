/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

/**
 * If an operation generates an InterruptedException on a server, it is wrapped in an
 * UncheckedInterruptedException and propagated back to the client.
 */
public class UncheckedInterruptedException extends RuntimeException {

  public UncheckedInterruptedException(Throwable cause) {
    super(cause);
  }

}
