/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DSetDUnitTest {

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
  public void testSet() {
    DSet<String> set = factory.createDSet("set");

    set.add("one");
    set.add("two");

    assertThat(set.size()).isEqualTo(2);

    DSet<String> set2 = factory.createDSet("set");
    assertThat(set2.size()).isEqualTo(2);
  }

  @Test
  public void testWithObjects() {
    DSet<UUID> set = factory.createDSet("uuid-set");

    UUID uuid = UUID.randomUUID();

    set.add(uuid);

    DSet<UUID> set2 = factory.createDSet("uuid-set");
    UUID uuid2 = set2.stream().findFirst().get();

    assertThat(uuid2).isEqualTo(uuid);
  }

  @Test
  public void testRemoveUsingIterator() {
    DSet<String> set = factory.createDSet("iterating-set");
    for (int i = 0; i < 10; i++) {
      set.add("value-" + i);
    }

    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }

    assertThat(set.isEmpty()).isTrue();

    DSet<String> set2 = factory.createDSet("iterating-set");
    assertThat(set2.isEmpty()).isTrue();
  }

  @Test
  public void testSetToArray() {
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID uuid3 = UUID.randomUUID();

    DSet<UUID> set = factory.createDSet("uuid-set-2");
    set.add(uuid1);
    set.add(uuid2);
    set.add(uuid3);

    assertThat(set.toArray()).contains(uuid1, uuid2, uuid3);

    DSet<UUID> list2 = factory.createDSet("uuid-set-2");
    assertThat(list2.toArray()).contains(uuid1, uuid2, uuid3);
  }

}
