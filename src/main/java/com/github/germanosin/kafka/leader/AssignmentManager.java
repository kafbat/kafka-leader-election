package com.github.germanosin.kafka.leader;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Timer;

public interface AssignmentManager<A extends Assignment, M extends MemberIdentity>
    extends AutoCloseable {

  /**
   * Invoked when a new assignment is created by joining the group. This is
   * invoked for both successful and unsuccessful assignments.
   */
  void onAssigned(A assignment, int generation);

  /**
   * Invoked when a rebalance operation starts, revoking leadership.
   */
  void onRevoked(Timer timer);

  Map<M, A> assign(List<M> identities);

  boolean isInitialisation();

  boolean isAlive(Timer timer);

  boolean await(Duration timeout) throws LeaderTimeoutException, InterruptedException;

}
