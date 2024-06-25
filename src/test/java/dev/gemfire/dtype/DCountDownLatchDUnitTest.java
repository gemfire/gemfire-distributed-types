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
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DCountDownLatchDUnitTest extends AbstractDCountDownLatchTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static Properties props;
  private static DTypeFactory factory;
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  @BeforeClass
  public static void setup() {
    locator = cluster.startLocatorVM(0);

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
  public void testServerStopPreservesLatchCount() {
    DCountDownLatch ref = getFactory().createDCountDownLatch(testName.getMethodName(), 3);

    assertThat(ref.getCount()).isEqualTo(3);

    MemberVM primary = TestUtils.getServerForKey(testName.getMethodName(), server1, server2);

    primary.stop();

    assertThat(ref.getCount()).isEqualTo(3);

    if (primary.equals(server1)) {
      server1 = cluster.startServerVM(1, props, locator.getPort());
    } else {
      server2 = cluster.startServerVM(2, props, locator.getPort());
    }
  }
}