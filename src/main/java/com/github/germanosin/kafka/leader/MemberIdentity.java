package com.github.germanosin.kafka.leader;

public interface MemberIdentity {

  String getId();

  boolean getLeaderEligibility();
}
