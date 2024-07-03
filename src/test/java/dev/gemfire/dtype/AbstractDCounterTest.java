package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;

public abstract class AbstractDCounterTest {

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testAdd() {
    DCounter counter = getFactory().createDCounter(testName.getMethodName());

    counter.increment(3);
    assertThat(counter.get()).isEqualTo(3);

    counter.increment(-2);
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void testConcurrentUpdatesToSameInstance() {
    DCounter counter = getFactory().createDCounter(testName.getMethodName());

    int iterations = 10_000;
    new ConcurrentLoopingThreads(iterations,
        i -> counter.increment(1),
        i -> counter.increment(1),
        i -> counter.increment(1),
        i -> counter.increment(1),
        i -> counter.increment(1))
            .run();

    assertThat(counter.get()).isEqualTo(iterations * 5);
  }

  @Test
  public void testConcurrentUpdatesToDifferentInstances() {
    DCounter counter1 = getFactory().createDCounter(testName.getMethodName());
    DCounter counter2 = getFactory().createDCounter(testName.getMethodName());
    DCounter counter3 = getFactory().createDCounter(testName.getMethodName());
    DCounter counter4 = getFactory().createDCounter(testName.getMethodName());
    DCounter counter5 = getFactory().createDCounter(testName.getMethodName());

    int iterations = 10_000;
    new ConcurrentLoopingThreads(iterations,
        i -> counter1.increment(1),
        i -> counter2.increment(1),
        i -> counter3.increment(1),
        i -> counter4.increment(1),
        i -> counter5.increment(1))
            .run();

    assertThat(counter1.get()).isEqualTo(iterations * 5);
  }

}
