/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DListIntegrationTest extends AbstractDListTest {

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule();

  private static DTypeFactory factory;

  @BeforeClass
  public static void setupClass() {
    server.startServer();
    factory = new DTypeFactory(server.getCache(),
        (region, memberTag) -> new IntegrationTestOperationPerformer());
  }

  DTypeFactory getFactory() {
    return factory;
  }

  @Ignore
  @Test
  public void testIteratingRemove() {
    // Ignored as it produces a ConcurrentModificationException when run as an integration test
  }

}
