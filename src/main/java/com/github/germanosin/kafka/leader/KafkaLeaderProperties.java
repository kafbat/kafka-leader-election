package com.github.germanosin.kafka.leader;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@Data
@Builder
public class KafkaLeaderProperties {
  private final String groupId;
  private final Map<String, Object> consumerConfigs;
  private final Duration initTimeout;

  public ConsumerConfig getConsumerConfig() {
    Map<String, Object> configs = new HashMap<>(consumerConfigs);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class
    );
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class
    );
    return new ConsumerConfig(configs);
  }
}
