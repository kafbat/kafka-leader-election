# kafka-leader-election

kafka-leader-election is an open-source Java library that provides a robust and easy-to-use mechanism for performing leader election in Kafka clusters. This library simplifies the development of Kafka applications where one instance needs to be the leader for processing specific tasks.

## Features

- **Automated Leader Election**: Automate leader election process in your Kafka clusters with minimal coding.
- **Highly Configurable**: Provides a variety of configuration options to suit different use cases.
- **Fault Tolerance**: Ensures that the system remains operational even if a leader instance fails.

## Prerequisites

- Java JDK 11 or above
- Apache Kafka 2.x.x

## Usage

Add kafka-leader-election to your project's dependency list.

```xml
<!-- pom.xml -->
<dependency>
    <groupId>com.github.germanosin</groupId>
    <artifactId>kafka-leader-election</artifactId>
    <version>0.1.1</version>
</dependency>
```

Example usage in a Java application:

```java

    KafkaLeaderProperties properties =
        KafkaLeaderProperties.builder()
            .consumerConfigs(
                Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                )                
            )
            .groupId("groupId")
            .initTimeout(Duration.ofSeconds(30))
            .build();

    AssignmentManager<TaskAssignment, HostMemberIdentity> taskManager =
        new DefaultLeaderTasksManager<>(
            Map.of("task1", run(inc1, 1), "task2", run(inc2, 2))
        );

    JsonLeaderProtocol<TaskAssignment, HostMemberIdentity> protocol =
        new JsonLeaderProtocol<>(new ObjectMapper(), HostMemberIdentity.class, TaskAssignment.class);
            
    KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leader = new KafkaLeaderElector<>(
        taskManager,
        protocol,
        properties,
        HostMemberIdentity.builder()
            .host("host1")
            .build()
    );

    leader.init();
    leader.await();
```

## Configuration

kafka-leader-election allows various configurations.

```java
KafkaLeaderProperties
    consumerConfigs() // Map of Kafka consumer configs (you could reuse it from KafkaProperties.buildConsumerProperties())
    groupId() // Consumer group id (should be same for all members)
    initTimeout() // timeout for leader election process
```

## License

kafka-leader-election is released under the Apache 2 License. See the [LICENSE](https://github.com/germanosin/kafka-leader-election/blob/main/LICENSE) file for more details.

## Support

For any questions or issues, please create a [GitHub Issue](https://github.com/germanosin/kafka-leader-election/issues/new).

