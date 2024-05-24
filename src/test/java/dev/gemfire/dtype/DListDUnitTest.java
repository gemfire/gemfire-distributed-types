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

public class DListDUnitTest {

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
  public void testList() {
    DList<String> list = factory.createDList("list");

    list.add("one");
    list.add("two");

    assertThat(list.size()).isEqualTo(2);

    DList<String> list2 = factory.createDList("list");
    assertThat(list2.size()).isEqualTo(2);
  }

  @Test
  public void testWithObjects() {
    DList<UUID> list = factory.createDList("uuid-list");

    UUID uuid = UUID.randomUUID();

    list.add(uuid);

    DList<UUID> list2 = factory.createDList("uuid-list");
    UUID uuid2 = list2.get(0);

    assertThat(uuid2).isEqualTo(uuid);
  }

  @Test
  public void testRemoveUsingIterator() {
    DList<String> list = factory.createDList("iterating-list");
    for (int i = 0; i < 10; i++) {
      list.add("value-" + i);
    }

    Iterator<String> iterator = list.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      iterator.remove();
    }

    assertThat(list.isEmpty()).isTrue();

    DList<String> list2 = factory.createDList("iterating-list");
    assertThat(list2.isEmpty()).isTrue();
  }

  @Test
  public void testListToArray() {
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID uuid3 = UUID.randomUUID();

    DList<UUID> list = factory.createDList("uuid-list-2");
    list.add(uuid1);
    list.add(uuid2);
    list.add(uuid3);

    assertThat(list.toArray()).contains(uuid1, uuid2, uuid3);

    DList<UUID> list2 = factory.createDList("uuid-list-2");
    assertThat(list2.toArray()).contains(uuid1, uuid2, uuid3);
  }

}
