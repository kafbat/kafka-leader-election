package com.github.germanosin.kafka.leader;

public interface Assignment {
  int getVersion();

  short getError();
}
