package dev.gemfire.dtype.internal;

import java.time.Instant;

import dev.gemfire.dtype.DSnowflake;

public class DSnowflakeImpl implements DSnowflake {

  private static final long HIGH_BITS = (1L << 63) - 1;

  private final long epochStart;
  private final int machineBits;
  private final long machineId;
  private final long maxMachineId;
  private final int sequenceBits;
  private final long maxSequenceId;

  private long sequenceId = -1;
  private long lastTimestamp;

  public DSnowflakeImpl() {
    this(DEFAULT_EPOCH_START, 0, DEFAULT_MACHINE_BITS, DEFAULT_SEQUENCE_BITS);
  }

  private DSnowflakeImpl(long epochStart, long machineId, int machineBits, int sequenceBits) {
    this.epochStart = epochStart;
    this.machineBits = machineBits;
    this.sequenceBits = sequenceBits;

    maxMachineId = (1L << this.machineBits) - 1;

    long tmpMachineId = (machineId == 0) ? (System.nanoTime() & maxMachineId) : machineId;
    tmpMachineId <<= sequenceBits;
    this.machineId = tmpMachineId;

    maxSequenceId = (1L << this.sequenceBits) - 1;
  }

  @Override
  public String getName() {
    return "snowflake-" + machineId;
  }

  @Override
  public synchronized long nextId() {
    long timestamp = getTimestamp();
    if (timestamp == lastTimestamp) {
      sequenceId = (sequenceId + 1) & maxSequenceId;
      if (sequenceId == 0) {
        while ((timestamp = getTimestamp()) == lastTimestamp) {
          Thread.yield();
        }
      }
    } else {
      sequenceId = 0;
    }

    lastTimestamp = timestamp;

    return (timestamp << (machineBits + sequenceBits) | machineId | sequenceId) & HIGH_BITS;
  }

  public long[] parse(long seqId) {
    long timestamp = (seqId >> (machineBits + sequenceBits)) + epochStart;
    long machineId = (seqId >> sequenceBits) & maxMachineId;
    long sequence = seqId & ((1L << sequenceBits) - 1);

    return new long[] {timestamp, machineId, sequence};
  }

  private long getTimestamp() {
    return Instant.now().toEpochMilli() - epochStart;
  }

  public static class BuilderImpl implements Builder {
    private long epochStart = DEFAULT_EPOCH_START;
    private long machineId = 0;
    private int machineBits = DEFAULT_MACHINE_BITS;
    private int sequenceBits = DEFAULT_SEQUENCE_BITS;

    public Builder withEpochStart(long epochStart) {
      this.epochStart = epochStart;
      return this;
    }

    public Builder withMachineId(long machineId) {
      this.machineId = machineId;
      return this;
    }

    public Builder withMachineBits(int machineBits) {
      this.machineBits = machineBits;
      return this;
    }

    public Builder withSequenceBits(int sequenceBits) {
      this.sequenceBits = sequenceBits;
      return this;
    }

    public DSnowflake build() {
      return new DSnowflakeImpl(epochStart, machineId, machineBits, sequenceBits);
    }
  }
}
