/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.util.concurrent.ConcurrentLoopingThreads;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DAtomicLongDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator;
  private static DTypeFactory factory;

  @BeforeClass
  public static void setup() {
    locator = cluster.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "dev.gemfire.dtype.**");

    cluster.startServerVM(1, props, locator.getPort());
    cluster.startServerVM(2, props, locator.getPort());

    ClientCache client = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort())
        .create();

    factory = new DTypeFactory(client);
  }

  @Test
  public void testAtomicLong() {
    DAtomicLong superLong = factory.createAtomicLong("super-long");

    superLong.set(5);
    assertThat(superLong.get()).isEqualTo(5);

    DAtomicLong superLong2 = factory.createAtomicLong("super-long");
    assertThat(superLong2.get()).isEqualTo(5);

    assertThat(superLong.getAndAdd(3)).isEqualTo(5);
    assertThat(superLong2.get()).isEqualTo(8);

    assertThat(superLong2.compareAndSet(8, 9)).isTrue();
  }

  @Test
  public void testAtomicLongConcurrentUpdates() {
    DAtomicLong long1 = factory.createAtomicLong("long");
    DAtomicLong long2 = factory.createAtomicLong("long");
    DAtomicLong long3 = factory.createAtomicLong("long");
    DAtomicLong long4 = factory.createAtomicLong("long");
    DAtomicLong long5 = factory.createAtomicLong("long");

    new ConcurrentLoopingThreads(10_000,
        i -> long1.getAndAdd(1),
        i -> long2.getAndAdd(1),
        i -> long3.getAndAdd(1),
        i -> long4.getAndAdd(1),
        i -> long5.getAndAdd(1))
            .run();

    assertThat(long1.get()).isEqualTo(50_000);
    assertThat(long2.get()).isEqualTo(50_000);
    assertThat(long3.get()).isEqualTo(50_000);
    assertThat(long4.get()).isEqualTo(50_000);
    assertThat(long5.get()).isEqualTo(50_000);
  }
}
