package dev.gemfire.dtype;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class TestUtils {
  static MemberVM getServerForKey(String key, MemberVM... vms) {
    for (MemberVM vm : vms) {
      boolean foundMember = vm.invoke("Get primary for key:" + key, () -> {
        InternalCache cache = ClusterStartupRule.getCache();
        Region<String, Object> region = cache.getRegion(DTypeFactory.DTYPES_REGION);
        DistributedMember m = PartitionRegionHelper.getPrimaryMemberForKey(region, key);

        return m != null
            ? cache.getDistributedSystem().getDistributedMember().getName().equals(m.getName())
            : false;
      });
      if (foundMember) {
        return vm;
      }
    }

    throw new AssertionError("could not find primary member for key:" + key);
  }

}
