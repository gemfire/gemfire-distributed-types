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

public class DSetIntegrationTest {

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule();

  private static DTypeFactory factory;
  private static DSet<Object> set;

  @BeforeClass
  public static void setupClass() {
    server.startServer();
    factory = new DTypeFactory(server.getCache(),
        (region, memberTag) -> new IntegrationTestOperationPerformer());
    set = factory.createDSet("set");
  }

  @Before
  public void setup() {
    set.clear();
  }

  @Test
  public void testAdd() {
    set.add("foo");
    assertThat(set.size()).isEqualTo(1);

    set.add("bar");
    assertThat(set.size()).isEqualTo(2);
  }

  @Test
  public void testRemove() {
    set.add("foo");
    set.add("bar");
    set.add("baz");

    set.remove("foo");
    assertThat(set.contains("foo")).isFalse();
    assertThat(set.size()).isEqualTo(2);

    assertThat(set.contains("bar")).isTrue();
    assertThat(set.contains("baz")).isTrue();
  }

}
