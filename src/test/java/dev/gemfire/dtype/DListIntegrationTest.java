/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DListIntegrationTest {

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule();

  private static DTypeFactory factory;
  private static DList<Object> list;

  @BeforeClass
  public static void setupClass() {
    server.startServer();
    factory = new DTypeFactory(server.getCache(),
        (region, memberId) -> new IntegrationTestOperationPerformer());
    list = factory.createDList("list");
  }

  @Before
  public void setup() {
    list.clear();
  }

  @Test
  public void testAdd() {
    list.add("foo");
    assertThat(list.size()).isEqualTo(1);

    list.add("bar");
    assertThat(list.size()).isEqualTo(2);
  }

  @Test
  public void testRemove() {
    list.add("foo");
    list.add("bar");
    list.add("baz");

    list.remove("foo");
    assertThat(list.contains("foo")).isFalse();
    assertThat(list.size()).isEqualTo(2);

    assertThat(list.get(0)).isEqualTo("bar");
    assertThat(list.get(1)).isEqualTo("baz");
  }

  @Test
  public void testRemoveIndex() {
    list.add("foo");
    list.add("bar");
    list.add("baz");

    String removed = (String) list.remove(1);

    assertThat(removed).isEqualTo("bar");
    assertThat(list.size()).isEqualTo(2);
  }

}
