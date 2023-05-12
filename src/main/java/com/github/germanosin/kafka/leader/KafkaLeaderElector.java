package com.github.germanosin.kafka.leader;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;


@Slf4j
public class KafkaLeaderElector<A extends Assignment, M extends MemberIdentity>
    implements LeaderElector {

  private static final AtomicInteger BD_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final String METRIC_GRP_PREFIX = "kafka.betdev";

  private static final Duration REBALANCE_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(3);

  private final ConsumerNetworkClient client;
  private final Long retryBackoffMs;
  private final KafkaLeaderProperties properties;
  private final Metrics metrics;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicInteger restarts = new AtomicInteger(0);
  private final AssignmentManager<A, M> assignmentManager;
  private final M identity;
  private final LeaderProtocol<A, M> leaderProtocol;
  private final LogContext logContext;
  private final Time time;
  private final Thread pollingThread;
  private final Duration rebalanceTimeout;
  private final Duration sessionTimeout;
  private final Duration heartbeatInterval;

  public KafkaLeaderElector(
      AssignmentManager<A, M> assignmentManager,
      LeaderProtocol<A, M> leaderProtocol,
      KafkaLeaderProperties properties,
      M identity
  ) {
    this(assignmentManager, properties, leaderProtocol, identity,
        REBALANCE_TIMEOUT, SESSION_TIMEOUT, HEARTBEAT_INTERVAL);
  }

  public KafkaLeaderElector(
      AssignmentManager<A, M> assignmentManager,
      KafkaLeaderProperties properties,
      LeaderProtocol<A, M> leaderProtocol,
      M identity,
      Duration rebalanceTimeout,
      Duration sessionTimeout,
      Duration heartbeatInterval
  ) {
    this.properties = properties;
    this.assignmentManager = assignmentManager;
    this.identity = identity;

    final String clientId =
        "bd-" + identity.getId() + "-" + BD_CLIENT_ID_SEQUENCE.getAndIncrement();
    this.metrics = new Metrics();
    this.leaderProtocol = leaderProtocol;
    this.rebalanceTimeout = rebalanceTimeout;
    this.sessionTimeout = sessionTimeout;
    this.heartbeatInterval = heartbeatInterval;

    final ConsumerConfig clientConfig = properties.getConsumerConfig();

    this.time = Time.SYSTEM;

    this.logContext = new LogContext(
        "[clientId=" + clientId + ", groupId=" + properties.getGroupId() + "] "
    );

    this.retryBackoffMs = clientConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

    Metadata metadata = new Metadata(
        this.retryBackoffMs,
        clientConfig.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
        this.logContext,
        new ClusterResourceListeners()
    );

    List<String> bootstrapServers = clientConfig.getList(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
    );

    List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
        bootstrapServers,
        clientConfig.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)
    );

    metadata.bootstrap(addresses);

    ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(
        clientConfig,
        time, this.logContext
    );

    long maxIdleMs = clientConfig.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);
    NetworkClient netClient = new NetworkClient(
        new Selector(
            maxIdleMs,
            metrics,
            time,
            METRIC_GRP_PREFIX,
            channelBuilder,
            this.logContext
        ),
        metadata,
        clientId,
        100, // a fixed large enough value will suffice
        clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
        clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        clientConfig.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
        clientConfig.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
        clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
        clientConfig.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
        clientConfig.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
        this.time,
        true,
        new ApiVersions(),
        logContext
    );

    this.client = new ConsumerNetworkClient(
        this.logContext,
        netClient,
        metadata,
        time,
        retryBackoffMs,
        clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
        Integer.MAX_VALUE
    );

    this.pollingThread = new Thread(null, this::poller, "LeaderPoller-" + clientId);

    log.debug("Group member created");
  }

  private LeaderCoordinator<A, M> createCoordinator() {
    return new LeaderCoordinator<>(
        this.logContext,
        this.client,
        this.properties.getGroupId(),
        rebalanceTimeout,
        sessionTimeout,
        heartbeatInterval,
        new Metrics(),
        METRIC_GRP_PREFIX,
        this.time,
        this.retryBackoffMs,
        this.identity,
        assignmentManager,
        this.leaderProtocol
    );
  }


  @Override
  public void init() {
    log.debug("Initializing group member");
    pollingThread.start();
  }

  private void poller() {
    try {
      while (!stopped.get()) {
        try (LeaderCoordinator<A, M> coordinator = createCoordinator()) {
          while (!stopped.get()) {
            coordinator.poll(500);
            Timer timer = Time.SYSTEM.timer(this.rebalanceTimeout.toMillis());
            if (!assignmentManager.isInitialisation() && !this.stopped.get()) {
              if (!assignmentManager.isAlive(timer)) {
                this.assignmentManager.onRevoked(timer);
                this.restarts.incrementAndGet();
                log.warn("Leader election restarts.");
                break;
              }
            }
          }
        }
      }
    } catch (WakeupException e) {
      // Skip wakeup exception
    } catch (Throwable t) {
      log.error("Unexpected exception in group processing thread", t);
    }
  }

  public void await() throws LeaderTimeoutException, LeaderException {
    try {
      if (!assignmentManager.await(properties.getInitTimeout())) {
        throw new LeaderTimeoutException("Timed out waiting for join group to complete");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LeaderException("Interrupted while waiting for join group to complete", e);
    }

    log.debug("Group member initialized and joined group");

  }

  @Override
  @SneakyThrows
  public void close() {
    if (stopped.get()) {
      return;
    }
    stop();
  }

  private void stop() throws InterruptedException {
    log.trace("Stopping the schema registry group member.");

    // Interrupt any outstanding poll calls
    if (client != null) {
      client.wakeup();
    }

    try {
      assignmentManager.close();
    } catch (Exception e) {
      log.error("Error on task-manager close", e);
    }

    this.stopped.set(true);
    pollingThread.interrupt();
    Instant start = Instant.now();
    while (
        pollingThread.isAlive()
            && (Instant.now().toEpochMilli() - start.toEpochMilli()) < 30 * 1000) {
      Thread.sleep(100);
    }

    // Do final cleanup
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    closeQuietly(metrics, "consumer metrics", firstException);
    closeQuietly(client, "consumer network client", firstException);

    if (firstException.get() != null) {
      throw new KafkaException(
          "Failed to stop the group member",
          firstException.get()
      );
    } else {
      log.debug("The Group member has stopped.");
    }
  }

  private static void closeQuietly(AutoCloseable closeable,
                                   String name,
                                   AtomicReference<Throwable> firstException
  ) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Throwable t) {
        firstException.compareAndSet(null, t);
        log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
      }
    }
  }

  public int getRestarts() {
    return this.restarts.get();
  }
}
