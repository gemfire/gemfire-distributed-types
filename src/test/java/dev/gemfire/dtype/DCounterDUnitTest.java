/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class DCounterDUnitTest extends AbstractDCounterTest {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static DTypeFactory factory;
  private static Properties props;
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  @BeforeClass
  public static void setup() {
    locator = cluster.startLocatorVM(0,
        x -> x.withSystemProperty("gemfire.member-weight", "20"));

    props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "dev.gemfire.dtype.**");

    server1 = cluster.startServerVM(1, props, locator.getPort());
    server2 = cluster.startServerVM(2, props, locator.getPort());

    ClientCache client = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort())
        .create();

    factory = new DTypeFactory(client);
  }

  @Override
  DTypeFactory getFactory() {
    return factory;
  }

  @Test
  public void testCounterUpdatesWhenServerCrashes() throws Exception {
    DCounter counter = getFactory().createDCounter(testName.getMethodName());

    MemberVM primary = TestUtils.getServerForKey(testName.getMethodName(), server1, server2);

    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger localCount = new AtomicInteger(0);
    Future<Void> future = executor.submit(() -> {
      while (running.get()) {
        counter.increment(1);
        localCount.incrementAndGet();
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // ignored
        }
      }
    });

    Awaitility.await().until(() -> localCount.get() > 100);

    primary.getVM().bounceForcibly();

    if (primary.equals(server1)) {
      server1 = cluster.startServerVM(1, props, locator.getPort());
    } else {
      server2 = cluster.startServerVM(2, props, locator.getPort());
    }

    running.set(false);
    future.get();

    assertThat(localCount.get()).isEqualTo(counter.get());
  }
}
