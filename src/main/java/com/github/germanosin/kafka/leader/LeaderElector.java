package com.github.germanosin.kafka.leader;

import java.io.Closeable;

public interface LeaderElector extends Closeable {
  void init();

  void await() throws LeaderTimeoutException;
}
