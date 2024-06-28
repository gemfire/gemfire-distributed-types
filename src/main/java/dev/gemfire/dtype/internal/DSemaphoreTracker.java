/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

package dev.gemfire.dtype.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;

/**
 * This class is responsible for releasing permits of clients that have either crashed or simply
 * 'gone away' without explicitly releasing their permits.
 * <p>
 * It maintains a map of client memberIds and the backend semaphore instances that the clients
 * have acquired permits for.
 */
public class DSemaphoreTracker implements ClientMembershipListener {

  private static final Logger logger = LogService.getLogger();

  Map<String, Set<DSemaphoreBackend>> memberSemaphores = new ConcurrentHashMap<>();

  public void add(String clientMember, DSemaphoreBackend semaphore) {
    memberSemaphores.computeIfAbsent(clientMember, k -> new HashSet<>()).add(semaphore);
  }

  public void remove(String clientMember, DSemaphoreBackend semaphore) {
    Set<DSemaphoreBackend> semaphoreSet = memberSemaphores.get(clientMember);
    if (semaphoreSet != null) {
      semaphoreSet.remove(semaphore);
      if (semaphoreSet.isEmpty()) {
        memberSemaphores.remove(clientMember);
      }
    }
  }

  public Set<DSemaphoreBackend> getSemaphores(String clientMember) {
    return memberSemaphores.getOrDefault(clientMember, Collections.emptySet());
  }

  @Override
  public void memberJoined(ClientMembershipEvent event) {}

  @Override
  public void memberLeft(ClientMembershipEvent event) {
    releaseAll(event.getMember());
  }

  @Override
  public void memberCrashed(ClientMembershipEvent event) {
    releaseAll(event.getMember());
  }

  private void releaseAll(DistributedMember member) {
    String memberTag = ((MemberIdentifier) member).getUniqueTag();
    if (memberTag == null) {
      return;
    }

    Set<DSemaphoreBackend> semaphores = memberSemaphores.remove(memberTag);
    if (semaphores == null || semaphores.isEmpty()) {
      return;
    }

    logger.info("Releasing {} semaphore(s) held by {}", semaphores.size(), member);
    semaphores.forEach(s -> s.releaseAll(((MemberIdentifier) member).getUniqueTag()));
  }

}
