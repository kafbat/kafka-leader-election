package com.github.germanosin.kafka.leader.tasks;

public interface Task extends Runnable, AutoCloseable {
  boolean isAlive();

  boolean isStarted();
}
