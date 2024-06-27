package dev.gemfire.dtype;

import dev.gemfire.dtype.internal.DSnowflakeImpl;

/**
 * DSnowflake is a cluster-unique ID generator based on the Twitter/X
 * <a href="https://en.wikipedia.org/wiki/Snowflake_ID">Snowflake</a> design. Generated IDs are long
 * primitive values and are k-ordered (roughly ordered). IDs are in the range from 0 to
 * Long.MAX_VALUE.
 * <p>
 * The layout of IDs is as follows:
 * <ul>
 * <li>timestamp (41 bits by default)</li>
 * <li>machine ID (10 bits by default)</li>
 * <li>sequence number (12 bits by default)</li>
 * </ul>
 * The layout can be adjusted by creating DSnowflakes with custom bit lengths for the machine ID and
 * the sequence number.
 * <p>
 * The timestamp is stored relative to a specific epoch start which, by default, is 1 Jan, 2020
 * ({@link #DEFAULT_EPOCH_START}).
 * <p>
 * The default bit sizes allow for generating 4096 (2^12) sequence IDs per millisecond. If that rate
 * is exceeded, generation will block until the next millisecond.
 * <p>
 * The current implementation generates the default machine ID from the nanosecond timestamp on the
 * system. Thus, DSnowflakes created on the same system, will have different machine IDs. If
 * required, a custom machine ID can be provided.
 * <p>
 * A builder pattern is used to create custom DSnowflake instance:
 *
 * <pre>
 * DSnowflake flake = DSnowflake.builder()
 *     .withEpochStart(1_000_000)
 *     .withMachineId(37)
 *     .withSequenceBits(15)
 *     .build();
 * </pre>
 */
public interface DSnowflake extends DType {

  int DEFAULT_MACHINE_BITS = 10;
  int DEFAULT_SEQUENCE_BITS = 12;
  /**
   * The default epoch start - 1 Jan, 2020
   */
  long DEFAULT_EPOCH_START = 1577836800000L;

  /**
   * Generate a new ID.
   *
   * @return the next ID
   */
  long nextId();

  /**
   * Parse a sequence ID into its individual components based on the configured bit lengths. Note
   * that the timestamp will be adjusted by the epoch start to reflect the 'correct' time.
   *
   * @param sequence the sequence ID to parse
   * @return a long[3] array consisting of timestamp, machine ID and sequence number
   */
  long[] parse(long sequence);

  /**
   * Provides a Builder used to create customized DSnowflakes.
   *
   * @return a new Builder
   */
  static Builder builder() {
    return new DSnowflakeImpl.BuilderImpl();
  }

  /**
   * Builder used to create custom DSnowflakes.
   */
  interface Builder {
    /**
     * Specify a custom epoch start
     *
     * @param epochStart a custom epoch start
     * @return this
     */
    Builder withEpochStart(long epochStart);

    /**
     * Specify a custom machine ID
     *
     * @param machineId a custom machine ID
     * @return this
     */
    Builder withMachineId(long machineId);

    /**
     * Specify the number of bits to use for the machine ID
     *
     * @param machineBits bits to use for the machine ID
     * @return this
     */
    Builder withMachineBits(int machineBits);

    /**
     * Specify the number of bits to use for the sequence number
     *
     * @param sequenceBits buts to use for the sequence number
     * @return this
     */
    Builder withSequenceBits(int sequenceBits);

    /**
     * Create a DSnowflake instance
     *
     * @return a new DSnowflake instance
     */
    DSnowflake build();
  }
}
