package com.github.germanosin.kafka.leader.tasks;

import com.github.germanosin.kafka.leader.Assignment;
import com.github.germanosin.kafka.leader.MemberIdentity;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class TaskAssignment implements Assignment {
  public static final int CURRENT_VERSION = 1;
  public static final short NO_ERROR = 0;
  public static final short DUPLICATE_URLS = 1;

  private int version;
  private short error;
  private List<String> tasks;
}
