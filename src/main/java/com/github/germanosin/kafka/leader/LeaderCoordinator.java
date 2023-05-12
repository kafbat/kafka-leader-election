package com.github.germanosin.kafka.leader;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

@Slf4j
public class LeaderCoordinator<A extends Assignment, M extends MemberIdentity>
    extends AbstractCoordinator implements Closeable {

  public static final String BD_SUBPROTOCOL_V0 = "v0";

  private final M identity;
  private final Metrics metrics;
  private A assignmentSnapshot;
  private final AssignmentManager<A, M> assignmentManager;
  private final LeaderProtocol<A, M> leaderProtocol;

  public LeaderCoordinator(
      LogContext logContext,
      ConsumerNetworkClient client,
      String groupId,
      Duration rebalanceTimeout,
      Duration sessionTimeout,
      Duration heartbeatInterval,
      Metrics metrics,
      String metricGrpPrefix,
      Time time,
      long retryBackoffMs,
      M identity,
      AssignmentManager<A, M> assignmentManager,
      LeaderProtocol<A, M> leaderProtocol
  ) {
    super(
        new GroupRebalanceConfig(
            (int) sessionTimeout.toMillis(),
            (int) rebalanceTimeout.toMillis(),
            (int) heartbeatInterval.toMillis(),
            groupId,
            Optional.empty(),
            retryBackoffMs,
            true
        ),
        logContext,
        client,
        metrics,
        metricGrpPrefix,
        time
    );
    this.identity = identity;
    this.assignmentSnapshot = null;
    this.assignmentManager = assignmentManager;
    this.leaderProtocol = leaderProtocol;
    this.metrics = metrics;
  }

  @Override
  protected String protocolType() {
    return "bd";
  }

  public void poll(long timeout) {
    // poll for io until the timeout expires
    final long start = time.milliseconds();
    long now = start;
    long remaining;

    do {
      if (coordinatorUnknown()) {
        ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        now = time.milliseconds();
      }

      if (rejoinNeededOrPending()) {
        ensureActiveGroup();
        now = time.milliseconds();
      }

      pollHeartbeat(now);

      long elapsed = now - start;
      remaining = timeout - elapsed;

      // Note that because the network client is shared with the background heartbeat thread,
      // we do not want to block in poll longer than the time to the next heartbeat.
      client.poll(time.timer(Math.min(Math.max(0, remaining), timeToNextHeartbeat(now))));

      elapsed = time.milliseconds() - start;
      remaining = timeout - elapsed;
    } while (remaining > 0);
  }

  @Override
  protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
    ByteBuffer metadata = leaderProtocol.serializeMetadata(identity);
    return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
        Collections.singletonList(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName(BD_SUBPROTOCOL_V0)
            .setMetadata(metadata.array())).iterator());
  }

  @Override
  protected void onJoinComplete(int generation, String memberId, String protocol,
                                ByteBuffer memberAssignment) {
    assignmentSnapshot = leaderProtocol.deserializeAssignment(memberAssignment);
    assignmentManager.onAssigned(assignmentSnapshot, generation);
  }


  @Override
  protected boolean onJoinPrepare(Timer timer, int generation, String memberId) {
    log.debug("Revoking previous assignment {}", assignmentSnapshot);
    if (assignmentSnapshot != null) {
      assignmentManager.onRevoked(timer);
    }
    return true;
  }

  @Override
  protected Map<String, ByteBuffer> onLeaderElected(
      String leaderId, String protocol,
      List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata,
      boolean skipAssignment
  ) {
    log.debug("Performing assignment");

    List<M> members = new ArrayList<>(allMemberMetadata.size());
    Map<String, String> memberIds = new HashMap<>();

    for (JoinGroupResponseData.JoinGroupResponseMember entry : allMemberMetadata) {
      M identity = leaderProtocol.deserializeMetadata(entry.metadata());
      members.add(identity);
      memberIds.put(identity.getId(), entry.memberId());
    }

    Map<M, A> assignments = assignmentManager.assign(members);

    Map<String, ByteBuffer> groupAssignment = new HashMap<>();

    for (Map.Entry<M, A> entry : assignments.entrySet()) {
      String memberId = memberIds.get(entry.getKey().getId());
      groupAssignment.put(
          memberId,
          leaderProtocol.serializeAssignment(entry.getValue())
      );
    }

    log.info(
        "assignments: {}", assignments
    );

    return groupAssignment;
  }

  @Override
  protected void close(Timer timer) {
    super.close(timer);
    this.metrics.close();
  }
}
