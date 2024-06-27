package dev.gemfire.dtype.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import dev.gemfire.dtype.DSnowflake;
import org.assertj.core.data.Offset;
import org.junit.Test;

import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;

public class DSnowflakeTest {

  @Test
  public void basicSequenceIdsIncrement() {
    DSnowflake flake = new DSnowflakeImpl();

    long seq1 = flake.nextId();
    long seq2 = flake.nextId();
    assertThat(seq2).isGreaterThan(seq1);

    long[] parsedSeq1 = flake.parse(seq1);
    long[] parsedSeq2 = flake.parse(seq2);

    // Machine IDs must be the same
    assertThat(parsedSeq2[1]).isEqualTo(parsedSeq1[1]);

    // If the timestamps are equal then the sequenceIds must not be equal
    if (parsedSeq1[0] == parsedSeq2[0]) {
      assertThat(parsedSeq1[2]).as("sequence IDs should not be equal if timestamps are equal")
          .isNotEqualTo(parsedSeq2[2]);
      assertThat(parsedSeq2[2]).isGreaterThan(0);
    } else {
      // Timestamps are not equal so sequenceIds would be reset
      assertThat(parsedSeq1[2]).isEqualTo(0);
      assertThat(parsedSeq2[2]).isEqualTo(0);
    }
  }

  @Test
  public void multipleSequenceIdsIncrement() {
    DSnowflake flake = new DSnowflakeImpl();

    for (int i = 0; i < 1_000_000; i++) {
      assertThat(flake.nextId()).isLessThan(flake.nextId());
    }
  }

  @Test
  public void testCustomEpochAndMachineId() {
    DSnowflake flake = DSnowflake.builder()
        .withEpochStart(10_000_000)
        .withMachineId(37)
        .build();

    long now = Instant.now().toEpochMilli();
    long seq = flake.nextId();
    long[] parsed = flake.parse(seq);

    assertThat(parsed[0]).isCloseTo(now, Offset.offset(10L));
    assertThat(parsed[1]).isEqualTo(37);
    assertThat(parsed[2]).isEqualTo(0);
  }

  @Test
  public void concurrentSequenceIdsIncrement() {
    DSnowflake flake = DSnowflake.builder()
        .withMachineBits(6)
        .withSequenceBits(24)
        .build();

    new ConcurrentLoopingThreads(1_000_000,
        i -> assertThat(flake.nextId()).isLessThan(flake.nextId()),
        i -> assertThat(flake.nextId()).isLessThan(flake.nextId()),
        i -> assertThat(flake.nextId()).isLessThan(flake.nextId()),
        i -> assertThat(flake.nextId()).isLessThan(flake.nextId()),
        i -> assertThat(flake.nextId()).isLessThan(flake.nextId())).run();
  }

}
