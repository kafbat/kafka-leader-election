package com.github.germanosin.kafka.leader.tasks;

import static com.github.germanosin.kafka.leader.tasks.TaskAssignment.CURRENT_VERSION;
import static com.github.germanosin.kafka.leader.tasks.TaskAssignment.NO_ERROR;

import com.github.germanosin.kafka.leader.AssignmentManager;
import com.github.germanosin.kafka.leader.LeaderTimeoutException;
import com.github.germanosin.kafka.leader.MemberIdentity;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

@Slf4j
public class DefaultLeaderTasksManager<M extends MemberIdentity>
    implements AssignmentManager<TaskAssignment, M> {
  private final Map<String, ? extends Task> tasks;
  private final CloseableExecutorService executor =
      new CloseableExecutorService(Executors.newCachedThreadPool());
  private final Map<String, Future<?>> ownTasks = new HashMap<>();
  private final Consumer<DefaultLeaderTasksManager<?>> terminationFunction;

  private final AtomicBoolean initialisation = new AtomicBoolean(true);

  private final CountDownLatch joinedLatch = new CountDownLatch(1);

  private static void systemExit(DefaultLeaderTasksManager<?> manager) {
    log.error("Abnormal process termination");
    System.exit(1);
  }

  public DefaultLeaderTasksManager(Map<String, ? extends Task> tasks) {
    this(tasks, DefaultLeaderTasksManager::systemExit);
  }

  public DefaultLeaderTasksManager(Map<String, ? extends Task> tasks,
                                   Consumer<DefaultLeaderTasksManager<?>> terminationFunction) {
    this.tasks = tasks;
    this.terminationFunction = terminationFunction;
  }

  @Override
  public Map<M, TaskAssignment> assign(List<M> identities) {
    Map<M, List<String>> result = new HashMap<>();

    Iterator<String> iterator = this.tasks.keySet().iterator();

    int i = 0;
    while (iterator.hasNext()) {
      String task = iterator.next();
      M member = identities.get(i++ % identities.size());
      List<String> memberTasks = result.computeIfAbsent(member, (m) -> new ArrayList<>());
      memberTasks.add(task);
    }

    TaskAssignment build = TaskAssignment.builder()
        .version(CURRENT_VERSION)
        .error(NO_ERROR)
        .build();

    return identities.stream().collect(
        Collectors.toMap(
            e -> e,
            e -> build.toBuilder()
                .tasks(result.containsKey(e) ? result.get(e) : List.of())
                .build()
        )
    );
  }

  @Override
  public boolean isInitialisation() {
    return initialisation.get();
  }

  @Override
  public void onAssigned(TaskAssignment assignment, int generation) {
    if (assignment.getTasks() != null) {
      for (String task : assignment.getTasks()) {
        Task runnable = this.tasks.get(task);
        Future<?> future = this.executor.submit(runnable);
        this.ownTasks.put(task, future);
      }
    }
    joinedLatch.countDown();
    initialisation.set(false);
  }

  @Override
  public void onRevoked(Timer timer) {
    initialisation.set(true);
    shutdownAll(timer);
  }

  private void shutdownAll(Timer timer) {
    if (!this.ownTasks.isEmpty()) {
      ownTasks.forEach((k, v) -> v.cancel(true));

      for (Map.Entry<String, Future<?>> entry : ownTasks.entrySet()) {
        closeTask(entry.getKey(), false, timer);
      }

      this.ownTasks.clear();
    }
  }

  @Override
  public boolean await(Duration timeout) throws LeaderTimeoutException, InterruptedException {
    return joinedLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isAlive(Timer timer) {
    for (Map.Entry<String, Future<?>> ownTask : ownTasks.entrySet()) {
      Future<?> future = ownTask.getValue();
      Task runnable = tasks.get(ownTask.getKey());
      if (runnable.isStarted()) {
        if (!future.isDone() && !runnable.isAlive()) {
          final String taskName = ownTask.getKey();
          closeTask(taskName, true, timer);
          submitTask(taskName);
          log.warn("task {} was successfully restarted!", taskName);
        }
      }
    }

    return ownTasks.values().stream().noneMatch(Future::isDone);
  }

  private void submitTask(String task) {
    Task runnable = this.tasks.get(task);
    Future<?> future = this.executor.submit(runnable);
    log.info("New runnable task {} was submitted.", task);
    this.ownTasks.put(task, future);
  }

  private void closeTask(String task, boolean cancel, Timer timer) {
    Future<?> future = ownTasks.get(task);
    if (cancel) {
      future.cancel(true);
    }
    long remainingMs = timer.remainingMs();
    try {
      log.info("awaiting task {} to finish...", task);
      future.get(remainingMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      log.error("Error on waiting task {}, took longer than {}", task, remainingMs);
      this.terminationFunction.accept(this);
    } catch (Throwable e) {
      log.info("Error on task {} finishing...", task, e);
    }
    Task runnable = tasks.get(task);
    try {
      runnable.close();
    } catch (Exception ex) {
      log.error("Exception during revoke ", ex);
    }
  }

  @Override
  public void close() throws IOException {
    shutdownAll(Time.SYSTEM.timer(Duration.ofMinutes(2)));
    executor.shutdown();
    try {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted waiting for group processing thread to exit",
          e
      );
    }
  }
}
