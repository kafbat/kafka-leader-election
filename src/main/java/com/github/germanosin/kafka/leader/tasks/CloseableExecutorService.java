package com.github.germanosin.kafka.leader.tasks;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CloseableExecutorService implements Closeable {
  private final Set<Future<?>> futures = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ExecutorService executorService;
  private final boolean shutdownOnClose;
  protected final AtomicBoolean isOpen = new AtomicBoolean(true);


  public CloseableExecutorService(ExecutorService executorService) {
    this(executorService, false);
  }

  public CloseableExecutorService(ExecutorService executorService, boolean shutdownOnClose) {
    this.executorService = executorService;
    this.shutdownOnClose = shutdownOnClose;
  }

  public void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
    this.executorService.awaitTermination(timeout, unit);
  }

  protected class InternalFutureTask<T> extends FutureTask<T> {
    private final RunnableFuture<T> task;

    InternalFutureTask(RunnableFuture<T> task) {
      super(task, null);
      this.task = task;
      futures.add(task);
    }

    protected void done() {
      futures.remove(task);
    }
  }

  /**
   * Closes any tasks currently in progress.
   */
  @Override
  public void close() {
    isOpen.set(false);
    Iterator<Future<?>> iterator = futures.iterator();
    while (iterator.hasNext()) {
      Future<?> future = iterator.next();
      iterator.remove();
      if (!future.isDone() && !future.isCancelled() && !future.cancel(true)) {
        log.warn("Could not cancel {}", future);
      }
    }
    if (shutdownOnClose) {
      executorService.shutdownNow();
    }
  }

  /**
   * Submits a Runnable task for execution and returns a Future
   * representing that task.  Upon completion, this task may be
   * taken or polled.
   *
   * @param task the task to submit
   * @return a future to watch the task
   */
  public Future<?> submit(Runnable task) {
    InternalFutureTask<Void> futureTask =
        new InternalFutureTask<Void>(new FutureTask<Void>(task, null));
    executorService.execute(futureTask);
    return futureTask;
  }

  public void shutdown() {
    this.executorService.shutdown();
  }

}
