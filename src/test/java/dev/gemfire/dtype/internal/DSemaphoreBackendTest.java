/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import static org.assertj.core.api.Assertions.assertThat;


import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class DSemaphoreBackendTest {

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String MEMBER = "member-id";
  private DSemaphoreFunctionContext context;
  private DSemaphoreTracker tracker;

  @Before
  public void setUp() {
    tracker = new DSemaphoreTracker();
    context = new DSemaphoreFunctionContext(MEMBER, tracker);
  }

  @Test
  public void setPermits() {
      DSemaphoreBackend semaphore = new DSemaphoreBackend(testName.getMethodName());
    semaphore.setPermits(3);

    assertThat(semaphore.availablePermits()).isEqualTo(3);
    assertThat(semaphore.getQueueLength()).isEqualTo(0);
  }

  @Test
  public void acquireAndRelease() {
    DSemaphoreBackend semaphore = new DSemaphoreBackend(testName.getMethodName());
    semaphore.setPermits(1);

    semaphore.acquire(context, 1);
    assertThat(semaphore.availablePermits()).isEqualTo(0);
    assertThat(tracker.getSemaphores(MEMBER)).hasSize(1);

    semaphore.release(context, 1);
    assertThat(semaphore.availablePermits()).isEqualTo(1);
    assertThat(tracker.getSemaphores(MEMBER)).isEmpty();
  }

  @Test
  public void permitsAreDrained() {
    DSemaphoreBackend semaphore = new DSemaphoreBackend(testName.getMethodName());
    semaphore.setPermits(3);

    semaphore.acquire(context, 1);

    assertThat(semaphore.drainPermits(context)).isEqualTo(2);
    assertThat(semaphore.availablePermits()).isEqualTo(0);

    semaphore.release(context, 3);
    assertThat(semaphore.availablePermits()).isEqualTo(3);
  }

  @Test
  public void testDestroy() {
    DSemaphoreBackend semaphore = new DSemaphoreBackend(testName.getMethodName());
    semaphore.setPermits(1);
    semaphore.acquire(context, 1);
    assertThat(tracker.memberSemaphores).isNotEmpty();

    semaphore.destroy(context);
    assertThat(tracker.memberSemaphores).isEmpty();
  }

}
