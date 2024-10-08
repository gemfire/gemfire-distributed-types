/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class DSemaphoreDUnitTest {

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final Properties props = new Properties();
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static DTypeFactory factory;
  private static ClientCache client;

  private String semaphoreName;

  @BeforeClass
  public static void setup() throws Exception {
    locator = cluster.startLocatorVM(0);

    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "dev.gemfire.dtype.**");

    server1 = cluster.startServerVM(1, props, locator.getPort());
    server2 = cluster.startServerVM(2, props, locator.getPort());

    client = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort())
        .create();

    factory = new DTypeFactory(client);
  }

  @Before
  public void before() {
    semaphoreName = testName.getMethodName();
  }

  @Test
  public void testAcquire() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);

    semaphore.acquire();

    assertThat(semaphore.availablePermits()).isEqualTo(0);
    assertThat(semaphore.tryAcquire()).isFalse();

    semaphore.release();

    assertThat(semaphore.availablePermits()).isEqualTo(1);
  }

  @Test
  public void testAcquireUsingMultipleThreads() throws Exception {
    DSemaphore semaphore1 = factory.createDSemaphore(semaphoreName, 1);
    semaphore1.acquire();

    DSemaphore semaphore2 = factory.createDSemaphore(semaphoreName, 1);
    Future<Void> future = executor.submit(() -> semaphore2.acquire());

    Awaitility.await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(semaphore2.getQueueLength()).isEqualTo(1));

    semaphore1.release();

    assertThatNoException().isThrownBy(() -> future.get(60, TimeUnit.SECONDS));
    semaphore1.release();
  }

  @Test
  public void testSemaphorePermitsAreRecoveredAfterServerCrash() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);
    semaphore.acquire();

    MemberVM primary = TestUtils.getServerForKey(semaphoreName, server1, server2);

    primary.stop();

    assertThat(semaphore.availablePermits()).isEqualTo(0);

    semaphore.release();
    assertThat(semaphore.availablePermits()).isEqualTo(1);

    if (primary.equals(server1)) {
      server1 = cluster.startServerVM(1, props, locator.getPort());
    } else {
      server2 = cluster.startServerVM(2, props, locator.getPort());
    }
  }

  @Test
  public void testSemaphorePermitsAreReleasedAfterClientDisconnect() throws Exception {
    int locatorPort = locator.getPort();
    ClientVM client1 = cluster.startClientVM(3, x -> x.withLocatorConnection(locatorPort));
    String localName = semaphoreName;
    client1.invoke(() -> {
      DTypeFactory factory = new DTypeFactory(ClusterStartupRule.getClientCache());
      DSemaphore sem = factory.createDSemaphore(localName, 1);
      sem.acquire();
    });

    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);
    Future<Void> future = executor.submit(() -> semaphore.acquire());
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(semaphore.getQueueLength()).isEqualTo(1));

    client1.invoke(() -> {
      ClusterStartupRule.getClientCache().close();
    });

    assertThatNoException().isThrownBy(() -> future.get(60, TimeUnit.SECONDS));
  }

  @Test
  public void testSemaphorePermitsAreReleasedAfterClientCrashes() throws Exception {
    int locatorPort = locator.getPort();
    ClientVM client1 = cluster.startClientVM(3, x -> x.withLocatorConnection(locatorPort));
    String localName = semaphoreName;
    client1.invoke(() -> {
      DTypeFactory factory = new DTypeFactory(ClusterStartupRule.getClientCache());
      DSemaphore sem = factory.createDSemaphore(localName, 1);
      sem.acquire();
    });

    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);
    Future<Void> future = executor.submit(() -> semaphore.acquire());
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(semaphore.getQueueLength()).isEqualTo(1));

    client1.getVM().bounceForcibly();

    assertThatNoException().isThrownBy(() -> future.get(60, TimeUnit.SECONDS));
  }

  @Test
  public void testSemaphoreIsDestroyed() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);

    semaphore.destroy();

    assertThat(semaphore.availablePermits()).isEqualTo(0);
  }

  @Test
  public void testDestroyedSemaphoreReleasesBlockedClients() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);

    Future<Void> future = executor.submit(() -> semaphore.acquire(2));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(semaphore.availablePermits()).isEqualTo(1));

    semaphore.destroy();

    assertThatThrownBy(() -> future.get()).hasRootCauseMessage("semaphore is destroyed");
  }

  @Test
  public void testDrainAll() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 3);

    assertThat(semaphore.drainPermits()).isEqualTo(3);
    assertThat(semaphore.availablePermits()).isEqualTo(0);
  }

  @Test
  public void testConcurrentSemaphoreAcquireRelease() {
    DSemaphore semaphore = factory.createDSemaphore(semaphoreName, 1);

    new ConcurrentLoopingThreads(1000,
        i -> acquireRelease(semaphore),
        i -> acquireRelease(semaphore),
        i -> acquireRelease(semaphore)).run();

    assertThat(semaphore.availablePermits()).isEqualTo(1);
  }

  private void acquireRelease(DSemaphore semaphore) {
    semaphore.acquire();
    semaphore.release();
  }

}
