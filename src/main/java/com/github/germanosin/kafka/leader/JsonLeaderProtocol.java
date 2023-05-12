package com.github.germanosin.kafka.leader;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import lombok.SneakyThrows;

public class JsonLeaderProtocol<A extends Assignment, M extends MemberIdentity>
    implements LeaderProtocol<A, M> {
  private final ObjectMapper mapper;
  private final Class<M> identityClass;

  private final Class<A> assignmentClass;

  public JsonLeaderProtocol(ObjectMapper mapper,
                            Class<M> identityClass,
                            Class<A> assignmentClass) {
    this.mapper = mapper;
    this.identityClass = identityClass;
    this.assignmentClass = assignmentClass;
  }

  @Override
  @SneakyThrows
  public ByteBuffer serializeMetadata(M identity) {
    return ByteBuffer.wrap(
        mapper.writeValueAsBytes(identity)
    );
  }

  @Override
  @SneakyThrows
  public M deserializeMetadata(byte[] buffer) {
    return mapper.readValue(buffer, this.identityClass);
  }

  @SneakyThrows
  public ByteBuffer serializeAssignment(Assignment assignment) {
    return ByteBuffer.wrap(
        mapper.writeValueAsBytes(assignment)
    );
  }

  @SneakyThrows
  public A deserializeAssignment(ByteBuffer buffer) {
    byte[] jsonBytes = new byte[buffer.remaining()];
    buffer.get(jsonBytes);
    return mapper.readValue(jsonBytes, this.assignmentClass);
  }
}
