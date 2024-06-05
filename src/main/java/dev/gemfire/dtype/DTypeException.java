/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

/**
 * General exception class for distributed types
 */
public class DTypeException extends RuntimeException {

  private static final long serialVersionUID = 3528364650887141831L;

  public DTypeException(String message) {
    super(message);
  }

}
