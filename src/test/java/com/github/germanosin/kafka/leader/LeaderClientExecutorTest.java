package com.github.germanosin.kafka.leader;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.tasks.DefaultLeaderTasksManager;
import com.github.germanosin.kafka.leader.tasks.Task;
import com.github.germanosin.kafka.leader.tasks.TaskAssignment;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class LeaderClientExecutorTest {
  @ClassRule
  public static final KafkaContainer kafka = new KafkaContainer(
      DockerImageName.parse("confluentinc/cp-kafka:7.2.0")
  )
      .withNetworkAliases("kafka")
      .withNetwork(Network.newNetwork())
      .waitingFor(Wait.forListeningPort());

  private final Map<Integer, Boolean> exception = new ConcurrentHashMap<>();

  @Test
  public void testLeaderMultipleTasks() throws LeaderTimeoutException, LeaderException {
    AtomicLong inc1 = new AtomicLong(0);
    AtomicLong inc2 = new AtomicLong(0);
    AtomicLong inc3 = new AtomicLong(0);
    AtomicLong inc4 = new AtomicLong(0);

    String consumer = String.format("consumer-%s", UUID.randomUUID());

    KafkaLeaderProperties properties =
        KafkaLeaderProperties.builder()
            .consumerConfigs(configs())
            .groupId(consumer)
            .initTimeout(Duration.ofSeconds(30))
            .build();

    AssignmentManager<TaskAssignment, HostMemberIdentity> taskManager1 =
        new DefaultLeaderTasksManager<>(
            Map.of("task1", run(inc1, 1), "task2", run(inc2, 2))
        );

    AssignmentManager<TaskAssignment, HostMemberIdentity> taskManager2 =
        new DefaultLeaderTasksManager<>(
            Map.of("task1", run(inc3, 3), "task2", run(inc4, 4))
        );

    JsonLeaderProtocol<TaskAssignment, HostMemberIdentity> protocol =
        new JsonLeaderProtocol<>(new ObjectMapper(), HostMemberIdentity.class,
            TaskAssignment.class);

    final KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leader1 = new KafkaLeaderElector<>(
        taskManager1,
        protocol,
        properties,
        HostMemberIdentity.builder()
            .host("host1")
            .build()
    );
    final KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leader2 = new KafkaLeaderElector<>(
        taskManager2,
        protocol,
        properties,
        HostMemberIdentity.builder()
            .host("host2")
            .build()
    );

    leader1.init();
    leader2.init();

    leader1.await();
    leader2.await();


    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> inc1.get() > 0 || inc3.get() > 0);

    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> inc2.get() > 0 || inc4.get() > 0);

    boolean firstStarted = inc1.get() > 0;

    AtomicLong check1;
    AtomicLong check2;
    KafkaLeaderElector<TaskAssignment, HostMemberIdentity> working;
    KafkaLeaderElector<TaskAssignment, HostMemberIdentity> closing;

    if (firstStarted) {
      closing = leader1;
      working = leader2;
      check1 = inc3;
      check2 = inc4;

    } else {
      closing = leader2;
      working = leader1;
      check1 = inc1;
      check2 = inc2;
    }

    final long prev1Value = check1.get();
    final long prev2Value = check2.get();

    closing.close();

    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> check1.get() > prev1Value && check2.get() > prev2Value);

    working.close();

  }

  @Test
  public void testLeader() throws LeaderTimeoutException, LeaderException {
    AtomicLong inc = new AtomicLong(0);
    AtomicLong inc2 = new AtomicLong(0);
    String consumer = String.format("consumer-%s", UUID.randomUUID());

    KafkaLeaderProperties properties =
        KafkaLeaderProperties.builder()
            .consumerConfigs(configs())
            .groupId(consumer)
            .initTimeout(Duration.ofSeconds(30))
            .build();

    JsonLeaderProtocol<TaskAssignment, HostMemberIdentity> protocol =
        new JsonLeaderProtocol<>(new ObjectMapper(), HostMemberIdentity.class,
            TaskAssignment.class);

    final KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leader1 = new KafkaLeaderElector<>(
        wrap(run(inc, 1)),
        protocol,
        properties,
        HostMemberIdentity.builder()
            .host("host1")
            .build()
    );
    final KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leader2 = new KafkaLeaderElector<>(
        wrap(run(inc2, 2)),
        protocol,
        properties,
        HostMemberIdentity.builder()
            .host("host2")
            .build()
    );

    leader1.init();
    leader2.init();

    leader1.await();
    leader2.await();


    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> inc.get() > 0 || inc2.get() > 0);

    log.info("INC-1: {}", inc.get());
    log.info("INC-2: {}", inc2.get());

    boolean firstInc = inc.get() > 0;

    if (firstInc) {
      final long inc2Value = inc2.get();
      exception.put(1, true);

      await().atMost(60, TimeUnit.SECONDS)
          .until(() -> leader1.getRestarts() > 0);

      leader1.close();

      await().atMost(60, TimeUnit.SECONDS)
          .until(() -> inc2.get() > inc2Value);

      leader2.close();
    } else {
      final long incValue = inc.get();
      exception.put(2, true);

      await().atMost(30, TimeUnit.SECONDS)
          .until(() -> leader2.getRestarts() > 0);

      leader2.close();

      await().atMost(30, TimeUnit.SECONDS)
          .until(() -> inc.get() > incValue);

      leader1.close();
    }
  }

  public AssignmentManager<TaskAssignment, HostMemberIdentity> wrap(Task task) {
    return new DefaultLeaderTasksManager<>(
        Map.of(
            "default", task
        )
    );
  }

  public Task run(AtomicLong inc, int n) {
    return new Task() {

      private volatile boolean inited = false;

      @Override
      public void close() {
      }

      @Override
      public boolean isAlive() {
        return true;
      }

      @Override
      public boolean isStarted() {
        return inited;
      }

      @Override
      public void run() {
        this.inited = true;
        log.info("Started inc: {}", n);
        try {
          while (!Thread.currentThread().isInterrupted()) {
            inc.incrementAndGet();
            Thread.sleep(100);
            if (exception.get(n) != null && exception.get(n)) {
              throw new RuntimeException("Test");
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          log.info("Ended inc {}", n);
        }
      }
    };
  }

  private Map<String, Object> configs() {
    return Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
    );
  }
}
