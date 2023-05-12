package com.github.germanosin.kafka.leader;

public class LeaderException extends RuntimeException {
  public LeaderException(String message, Throwable e) {
    super(message, e);
  }
}
