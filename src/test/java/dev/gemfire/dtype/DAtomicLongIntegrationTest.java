/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DAtomicLongIntegrationTest {

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule();

  private static DTypeFactory factory;

  @BeforeClass
  public static void setupClass() {
    server.startServer();
    factory = new DTypeFactory(server.getCache());
  }

  @Test
  public void testAtomicLong() {
    DAtomicLong superLong = factory.createAtomicLong("long");
    superLong.set(5);
    assertThat(superLong.get()).isEqualTo(5);
  }

}
