/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import static dev.gemfire.dtype.DTypeFactory.DTYPES_REGION;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.management.membership.ClientMembership;

public class DTypeService implements CacheService {

  private static final Logger logger = LogService.getLogger();

  private InternalCache cache;

  @Override
  public boolean init(Cache cache) {
    this.cache = (InternalCache) cache;

    if (this.cache.isClient() || this.cache.getDistributedSystem().getName().contains("locator")) {
      return false;
    }

    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(DTYPES_REGION);

    DSemaphoreTracker tracker = new DSemaphoreTracker();
    ClientMembership.registerClientMembershipListener(tracker);

    FunctionService.registerFunction(new CollectionsBackendFunction());
    FunctionService.registerFunction(new SemaphoreBackendFunction(tracker));

    logger.info("Initialized service for GemFire Distributed Types");

    return true;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return DTypeService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }
}
