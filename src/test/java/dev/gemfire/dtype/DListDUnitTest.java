/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DListDUnitTest extends AbstractDListTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static DTypeFactory factory;

  @BeforeClass
  public static void setup() {
    MemberVM locator = cluster.startLocatorVM(0);

    Properties props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "dev.gemfire.dtype.**");

    cluster.startServerVM(1, props, locator.getPort());
    cluster.startServerVM(2, props, locator.getPort());

    ClientCache client = new ClientCacheFactory()
        .addPoolLocator("localhost", locator.getPort())
        .create();

    factory = new DTypeFactory(client);
  }

  @Override
  DTypeFactory getFactory() {
    return factory;
  }

}
