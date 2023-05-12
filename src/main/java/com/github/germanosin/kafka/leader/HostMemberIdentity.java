package com.github.germanosin.kafka.leader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HostMemberIdentity implements MemberIdentity {
  @Builder.Default
  private Integer version = 0;
  private String host;
  @Builder.Default
  private boolean leaderEligibility = true;

  @JsonIgnore
  public String getId() {
    return host;
  }

  @Override
  public boolean getLeaderEligibility() {
    return leaderEligibility;
  }
}
