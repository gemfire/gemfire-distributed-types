/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

/**
 * If an operation generates an InterruptedException on a server, it is wrapped in an
 * UncheckInterruptedException and propagated back to the client.
 */
public class UncheckInterruptedException extends RuntimeException {

  public UncheckInterruptedException(Throwable cause) {
    super(cause);
  }

}
