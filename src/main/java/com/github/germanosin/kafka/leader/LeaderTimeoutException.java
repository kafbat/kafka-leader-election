package com.github.germanosin.kafka.leader;

public class LeaderTimeoutException extends Exception {
  public LeaderTimeoutException(String message) {
    super(message);
  }
}
