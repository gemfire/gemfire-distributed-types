package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractDCountDownLatchTest {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public TestName testName = new TestName();

  abstract DTypeFactory getFactory();

  @Test
  public void testCountDown() throws Exception {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 1);

    ref.countDown();

    assertThat(ref.getCount()).isEqualTo(0);
    assertThat(ref.await(1, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testAwait() throws Exception {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 1);

    Future<Void> future = executor.submit(() -> ref.await());
    ref.countDown();

    assertThat(ref.getCount()).isEqualTo(0);
    assertThatNoException().isThrownBy(() -> future.get(1, TimeUnit.MINUTES));
    assertThat(ref.await(1, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void testAwaitWithMultipleThreads() {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 1);

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(() -> ref.await()));
    }

    Awaitility.await().atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(ref.getWaiters()).isEqualTo(10));

    ref.countDown();

    Awaitility.await().atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> assertThat(ref.getWaiters()).isEqualTo(0));

    for (Future<Void> future : futures) {
      assertThatNoException().isThrownBy(() -> future.get(1, TimeUnit.MINUTES));
    }
  }

  @Test
  public void testAwaitWithTimeoutExceeded() throws Exception {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 1);

    Future<Boolean> future = executor.submit(() -> {
      try {
        return ref.await(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(future.get()).isFalse();
  }

  @Test
  public void testAwaitWithTimeout() throws Exception {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 1);

    Future<Boolean> future = executor.submit(() -> {
      try {
        return ref.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    ref.countDown();
    assertThat(future.get()).isTrue();
  }

  @Test
  public void testSetCount() {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 0);

    assertThat(ref.setCount(1)).isTrue();
    assertThat(ref.setCount(1)).isFalse();
  }

  @Test
  public void testCountDownConcurrency() {
    int latches = 100_000;
    DCountDownLatch ref1 = getFactory().createDCountDownLatch(testName.getMethodName(), latches);
    DCountDownLatch ref2 = getFactory().createDCountDownLatch(testName.getMethodName(), latches);
    DCountDownLatch ref3 = getFactory().createDCountDownLatch(testName.getMethodName(), latches);
    DCountDownLatch ref4 = getFactory().createDCountDownLatch(testName.getMethodName(), latches);
    DCountDownLatch ref5 = getFactory().createDCountDownLatch(testName.getMethodName(), latches);

    int iterations = 10_000;
    new ConcurrentLoopingThreads(iterations,
        i -> ref1.countDown(),
        i -> ref2.countDown(),
        i -> ref3.countDown(),
        i -> ref4.countDown(),
        i -> ref5.countDown()).run();

    assertThat(ref1.getCount()).isEqualTo(latches - (iterations * 5));
  }

}
