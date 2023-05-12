package com.github.germanosin.kafka.leader;

import java.nio.ByteBuffer;

public interface LeaderProtocol<A extends Assignment, M extends MemberIdentity> {
  ByteBuffer serializeAssignment(A assignment);

  A deserializeAssignment(ByteBuffer buffer);

  ByteBuffer serializeMetadata(M identity);

  M deserializeMetadata(byte[] buffer);
}
