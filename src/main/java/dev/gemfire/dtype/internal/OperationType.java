package dev.gemfire.dtype.internal;

/**
 * Enum used to identify what of operation is being executed
 */
public enum OperationType {

  /**
   * QUERY will not result in a region.put
   */
  QUERY,
  /**
   * Normal update that results in a region put and sets the delta
   */
  UPDATE,

  /**
   * An update that should not set delta
   */
  NO_DELTA_UPDATE
}
