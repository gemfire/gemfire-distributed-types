/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class DSemaphoreDUnitTest {

  private static final String SEM_NAME = "semi";

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static Properties props = new Properties();
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static DTypeFactory factory;

  @BeforeClass
  public static void setup() {
    locator = cluster.startLocatorVM(0);

    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "dev.gemfire.dtype.**");

    server1 = cluster.startServerVM(1, props, locator.getPort());
    server2 = cluster.startServerVM(2, props, locator.getPort());

    ClientCache client = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort())
        .create();

    factory = new DTypeFactory(client);
  }

  @After
  public void cleanup() {
    factory.destroy(SEM_NAME);
  }

  @Test
  public void testAcquire() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);

    semaphore.acquire();

    assertThat(semaphore.availablePermits()).isEqualTo(0);
    assertThat(semaphore.tryAcquire()).isFalse();

    semaphore.release();

    assertThat(semaphore.availablePermits()).isEqualTo(1);
  }

  @Test
  public void testAcquireUsingMultipleThreads() throws Exception {
    DSemaphore semaphore1 = factory.createDSemaphore(SEM_NAME, 1);
    semaphore1.acquire();

    DSemaphore semaphore2 = factory.createDSemaphore(SEM_NAME, 1);
    Future<Void> future = executor.submit(() -> semaphore2.acquire());

    semaphore1.release();

    assertThatNoException().isThrownBy(() -> future.get(60, TimeUnit.SECONDS));
    semaphore1.release();
  }

  @Test
  public void testSemaphorePermitsAreRecoveredAfterServerCrash() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);
    semaphore.acquire();

    MemberVM primary = getServerForKey(SEM_NAME, server1, server2);
    assertThat(primary).as("Cannot find primary for key:" + SEM_NAME).isNotNull();

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
    client1.invoke(() -> {
      DTypeFactory factory = new DTypeFactory(ClusterStartupRule.getClientCache());
      DSemaphore sem = factory.createDSemaphore(SEM_NAME, 1);
      sem.acquire();
    });

    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);
    Future<Void> future = executor.submit(() -> semaphore.acquire());
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(semaphore.getQueueLength()).isEqualTo(1));

    client1.invoke(() -> {
      ClusterStartupRule.getClientCache().close();
    });

    assertThatNoException().isThrownBy(() -> future.get(60, TimeUnit.SECONDS));
  }

  @Test
  public void testSemaphoreIsDestroyed() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);

    semaphore.destroy();

    assertThat(semaphore.availablePermits()).isEqualTo(0);
  }

  @Test
  public void testDestroyedSemaphoreReleasesBlockedClients() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);

    Future<Void> future = executor.submit(() -> semaphore.acquire(2));
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(semaphore.availablePermits()).isEqualTo(1));

    semaphore.destroy();

    assertThatThrownBy(() -> future.get()).hasRootCauseMessage("semaphore is destroyed");
  }

  @Test
  public void testDrainAll() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 3);

    assertThat(semaphore.drainPermits()).isEqualTo(3);
    assertThat(semaphore.availablePermits()).isEqualTo(0);
  }

  @Test
  public void testConcurrentSemaphoreAcquireRelease() {
    DSemaphore semaphore = factory.createDSemaphore(SEM_NAME, 1);

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

  private MemberVM getServerForKey(String key, MemberVM... vms) {
    for (MemberVM vm : vms) {
      boolean foundMember = vm.invoke("Get primary for key:" + key, () -> {
        InternalCache cache = ClusterStartupRule.getCache();
        Region<String, Object> region = cache.getRegion(DTypeFactory.DTYPES_REGION);
        DistributedMember m = PartitionRegionHelper.getPrimaryMemberForKey(region, key);

        return m != null
            ? cache.getDistributedSystem().getDistributedMember().getName().equals(m.getName())
            : false;
      });
      if (foundMember) {
        return vm;
      }
    }

    return null;
  }

}
