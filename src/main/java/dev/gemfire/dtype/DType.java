/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

/**
 * Base interface for all Distributed Types
 */
public interface DType {

  /**
   * Get the name of this instance.
   *
   * @return the name of the instance
   */
  String getName();

  /**
   * Destroy this instance. Once destroyed, other references to the named instance, (including in
   * other VMs), should not be used anymore.
   */
  void destroy();

}
