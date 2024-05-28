/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

/**
 * If an operation generates an exception on a server, it is wrapped in a MarkerException. This
 * allows the client to discover the underlying cause and deliver that to the user.
 */
public class MarkerException extends RuntimeException {

  public MarkerException(Throwable cause) {
    super(cause);
  }

}
