/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lineage.hub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.LineageVariables;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.lineage.spi.ILineageSink;
import org.apache.hop.lineage.spi.LineageSinkHealth;

/**
 * Central async hub for lineage events. When lineage is disabled (see {@link
 * LineageConfiguration}), {@link #emit(LineageEvent)} returns immediately without queuing.
 */
public final class LineageHub {

  private static final LineageHub INSTANCE = new LineageHub(null, null, null);

  private final ILogChannel log = new LogChannel("LineageHub");

  /** When non-null, used instead of loading sinks from the plugin registry (tests). */
  private final List<ILineageSink> explicitSinks;

  private final LineageConfiguration fixedConfiguration;

  // Lazily allocated in ensureWorkerStarted so the singleton picks up the resolved
  // HOP_LINEAGE_QUEUE_CAPACITY on first emit (Hop variables are not available at class-init time).
  private volatile BlockingQueue<Object> queue;

  private final AtomicLong droppedEvents = new AtomicLong();

  /**
   * Whether the JVM exit hook is in place. A sink delivers asynchronously, so a process that exits
   * right after its last run (hop-run calls System.exit) would lose whatever the sink still holds,
   * typically the runs' terminal events, unless the hub shuts its sinks down on the way out.
   */
  private final AtomicBoolean exitHookRegistered = new AtomicBoolean(false);

  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Set once the sinks failed to initialize. Without it every later emit() would start a fresh
   * worker that fails the same way and logs the same error, once per event.
   */
  private final AtomicBoolean sinkInitFailed = new AtomicBoolean(false);

  private volatile Thread worker;

  private volatile List<ILineageSink> sinks = List.of();
  private final AtomicBoolean sinksInitialized = new AtomicBoolean(false);

  /**
   * {@link #shutdown()} takes the write lock, {@link #emit(LineageEvent)} the read lock. Shutdowns
   * are serialized, and an event emitted during a shutdown waits for it to end instead of starting
   * a worker that the shutdown then loses track of. Not {@code this}: the worker takes that lock
   * while it initializes the sinks, and shutdown() waits for the worker.
   */
  private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();

  /**
   * Runs that may still emit events when the JVM exits, for example a hop-run that is stopping on a
   * termination signal. {@link #shutdownOnExit()} waits until each of these is counted down.
   */
  private final Set<CountDownLatch> exitHolds = ConcurrentHashMap.newKeySet();

  /**
   * The lineage sinks defined in metadata that runs selected, by {@link LineageSinkTarget#key()}.
   * Only settings: they are kept for the life of the JVM, so a run that started before a shutdown
   * still reaches its sinks after it.
   */
  private final Map<String, LineageSinkTarget> targets = new ConcurrentHashMap<>();

  /** The sinks started for {@link #targets}, by key, on the first event for each. */
  private final Map<String, ILineageSink> routedSinks = new ConcurrentHashMap<>();

  /**
   * The targets whose sink failed to start, with when and why. Their events are dropped until the
   * sink is tried again, {@link #RETRY_FAILED_SINK_MS} later.
   */
  private final Map<String, Failure> failedTargets = new ConcurrentHashMap<>();

  /**
   * How many runs hold each target (see {@link #useSink} and {@link #hold}). A target no run holds
   * whose name a newer target took is retired. Guarded by {@code routedSinks}.
   */
  private final Map<String, Integer> holds = new HashMap<>();

  /** The key of the most recent target of each sink name. */
  private final Map<String, String> latestKeys = new ConcurrentHashMap<>();

  /** How long a sink that failed to start is left alone before it is tried again. */
  static final long RETRY_FAILED_SINK_MS = 60_000L;

  /** {@link #RETRY_FAILED_SINK_MS}, shorter in tests. */
  long retryFailedSinkMs = RETRY_FAILED_SINK_MS;

  private record Failure(long timeMillis, String message) {}

  /** Releases the holds of a run that ended, in the queue after the run's last event. */
  private record ReleaseRequest(Set<String> keys) {}

  /** The plugin id of each sink configured with variables, for the status page. */
  private final Map<ILineageSink, String> sinkIds =
      Collections.synchronizedMap(new IdentityHashMap<>());

  /** Why the sinks configured with variables failed to initialize, or null. */
  private volatile String sinkInitError;

  /** Creates the sink of a target: from the plugin registry, or a stand-in in tests. */
  private final SinkFactory sinkFactory;

  /** Creates the sink of a {@link LineageSinkTarget}, not yet initialized. */
  @FunctionalInterface
  public interface SinkFactory {
    ILineageSink create(LineageSinkTarget target) throws HopException;
  }

  private LineageHub(
      LineageConfiguration fixedConfiguration,
      List<ILineageSink> explicitSinks,
      SinkFactory sinkFactory) {
    this.fixedConfiguration = fixedConfiguration;
    this.explicitSinks = explicitSinks;
    this.sinkFactory = sinkFactory != null ? sinkFactory : LineageHub::loadSink;
  }

  public static LineageHub getInstance() {
    return INSTANCE;
  }

  /**
   * Isolated hub for unit tests: fixed configuration and sink list, no singleton state.
   *
   * @param configuration hub tuning
   * @param sinks sinks to invoke (may be empty)
   */
  public static LineageHub newIsolatedForTesting(
      LineageConfiguration configuration, List<ILineageSink> sinks) {
    return new LineageHub(configuration, new ArrayList<>(sinks), null);
  }

  /**
   * Isolated hub for unit tests that also starts the sinks runs select, with {@code sinkFactory}
   * instead of the plugin registry.
   */
  public static LineageHub newIsolatedForTesting(
      LineageConfiguration configuration, List<ILineageSink> sinks, SinkFactory sinkFactory) {
    return new LineageHub(configuration, new ArrayList<>(sinks), sinkFactory);
  }

  /**
   * Makes a lineage sink defined in metadata available to the events that carry the key this
   * returns (see the {@code sinks} of {@link org.apache.hop.lineage.context.LineageContext}). The
   * sink starts when the first of them is delivered.
   */
  public String useSink(LineageSinkTarget target) {
    String key = target.key();
    List<ILineageSink> retired = new ArrayList<>();
    synchronized (routedSinks) {
      targets.putIfAbsent(key, target);
      String replaced = latestKeys.put(target.name(), key);
      holds.merge(key, 1, Integer::sum);
      // A version no run holds any more, say from before an edit between two runs: none of its
      // events are waiting, since a run lets go of its sinks after its last event.
      if (replaced != null && !replaced.equals(key) && !holds.containsKey(replaced)) {
        retire(replaced, retired);
      }
    }
    shutdownInBackground(retired);
    return key;
  }

  /**
   * Keeps the sinks of these keys for a run that reports to them, such as the sinks of the run that
   * started it. {@link #useSink} holds the sink it returns. Undo with {@link #release} when the run
   * ends.
   */
  public void hold(Set<String> keys) {
    synchronized (routedSinks) {
      for (String key : keys) {
        holds.merge(key, 1, Integer::sum);
      }
    }
  }

  /**
   * Lets go of the sinks a run held, once the events it emitted so far are delivered. A sink that
   * no run holds any more, and that a newer version of the same lineage sink replaced, is shut
   * down: its settings changed, say an edited URL or a new token.
   */
  public void release(Set<String> keys) {
    if (keys.isEmpty()) {
      return;
    }
    ReleaseRequest request = new ReleaseRequest(Set.copyOf(keys));
    boolean interrupted = false;
    shutdownLock.readLock().lock();
    try {
      BlockingQueue<Object> q = queue;
      if (running.get() && q != null) {
        // After the run's last events, also on an interrupted thread: let go before they are
        // delivered, and a replaced version would be shut down with them still waiting
        while (true) {
          try {
            q.put(request);
            return;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    applyRelease(request);
  }

  /**
   * The lineage sinks the hub delivers to, with how their delivery is going: the sinks configured
   * with variables, and the sinks runs selected in their run configurations.
   */
  public List<LineageSinkStatus> sinkStatus() {
    List<LineageSinkStatus> result = new ArrayList<>();
    if (resolveConfig().isEnabled()) {
      if (sinkInitError != null) {
        result.add(
            new LineageSinkStatus(
                String.join(",", resolveConfig().getSinkIds()),
                "",
                false,
                false,
                null,
                sinkInitError));
      }
      for (ILineageSink sink : sinksInitialized.get() ? sinks : List.<ILineageSink>of()) {
        String id = sinkIds.getOrDefault(sink, sink.getClass().getSimpleName());
        result.add(new LineageSinkStatus(id, pluginName(id), false, true, healthOf(sink), null));
      }
    }
    List<LineageSinkTarget> known;
    Map<String, ILineageSink> started;
    synchronized (routedSinks) {
      known = new ArrayList<>(targets.values());
      started = new HashMap<>(routedSinks);
    }
    for (LineageSinkTarget target : known) {
      String key = target.key();
      ILineageSink sink = started.get(key);
      result.add(
          new LineageSinkStatus(
              target.name(),
              pluginName(target.pluginId()),
              true,
              sink != null,
              sink == null ? null : healthOf(sink),
              failure(key)));
    }
    return result;
  }

  /** Events waiting in the hub's queue for the sinks. */
  public int getQueuedEventCount() {
    BlockingQueue<Object> q = queue;
    return q == null ? 0 : q.size();
  }

  private LineageSinkHealth healthOf(ILineageSink sink) {
    try {
      return sink.health();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static String pluginName(String pluginId) {
    IPlugin plugin =
        PluginRegistry.getInstance().findPluginWithId(LineageSinkPluginType.class, pluginId);
    return plugin == null ? pluginId : plugin.getName();
  }

  /** Why the sink of a key failed to start, while it waits to be tried again; null otherwise. */
  public String failure(String key) {
    Failure failure = failedTargets.get(key);
    return failure == null ? null : failure.message();
  }

  private void applyRelease(ReleaseRequest request) {
    List<ILineageSink> retired = new ArrayList<>();
    synchronized (routedSinks) {
      for (String key : request.keys()) {
        Integer held = holds.computeIfPresent(key, (k, n) -> n > 1 ? n - 1 : null);
        LineageSinkTarget target = targets.get(key);
        if (held != null || target == null || key.equals(latestKeys.get(target.name()))) {
          continue;
        }
        retire(key, retired);
      }
    }
    shutdownInBackground(retired);
  }

  /** Forgets a target; its sink, when it started, is added to {@code retired}. */
  private void retire(String key, List<ILineageSink> retired) {
    targets.remove(key);
    failedTargets.remove(key);
    ILineageSink sink = routedSinks.remove(key);
    if (sink != null) {
      retired.add(sink);
    }
  }

  private void shutdownInBackground(List<ILineageSink> retired) {
    if (!retired.isEmpty()) {
      // A shutdown waits for the sink's own queue to drain: not on the dispatcher thread.
      Thread thread =
          new Thread(
              () -> {
                for (ILineageSink sink : retired) {
                  shutdownQuietly(sink);
                }
              },
              "Hop-LineageHub-RetireSink");
      thread.setDaemon(true);
      thread.start();
    }
  }

  private void shutdownQuietly(ILineageSink sink) {
    try {
      sink.shutdown();
    } catch (Exception e) {
      log.logError("Error shutting down lineage sink " + sink.getClass().getName(), e);
    }
  }

  /** Invoked after {@link org.apache.hop.core.HopEnvironment} finishes plugin registration. */
  public void environmentReady() {
    LineageConfiguration.invalidate();
    sinksInitialized.set(false);
    sinkInitFailed.set(false);
    sinks = List.of();
    sinkIds.clear();
    sinkInitError = null;
    routedSinks.clear();
    failedTargets.clear();
  }

  /**
   * Whether lineage observation is switched on for every run, with {@code HOP_LINEAGE_ENABLED}. A
   * run can also report to sinks selected in its run configuration: callers with a run at hand ask
   * {@link org.apache.hop.lineage.LineageSinkRoutes#isEnabled} instead. Callers that would do
   * measurable work purely to build an event — e.g. accumulating per-row state — should check this
   * first; it is a cheap read of the memoized configuration.
   */
  public boolean isEnabled() {
    return resolveConfig().isEnabled();
  }

  /**
   * Queue a lineage event. No-op when lineage is disabled. Drops the event when the queue is full
   * and increments an internal drop counter.
   */
  public void emit(LineageEvent event) {
    LineageConfiguration cfg = resolveConfig();
    boolean routed = !event.getContext().getSinks().isEmpty();
    if (!routed && (!cfg.isEnabled() || sinkInitFailed.get())) {
      return;
    }
    shutdownLock.readLock().lock();
    try {
      ensureWorkerStarted(cfg);
      if (!queue.offer(event)) {
        droppedEvents.incrementAndGet();
        log.logError(
            "Lineage event dropped because the queue is full (capacity "
                + cfg.getQueueCapacity()
                + "). Total drops: "
                + droppedEvents.get());
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * Wait until queued events (including flush markers) have been processed by sinks. Safe to call
   * when disabled (no-op).
   */
  public void flush() {
    LineageConfiguration cfg = resolveConfig();
    if ((!cfg.isEnabled() && targets.isEmpty()) || !running.get()) {
      return;
    }
    FlushRequest request = new FlushRequest();
    try {
      queue.put(request);
      if (!running.get()) {
        // The worker stopped while the request was being queued: nobody will pick it up.
        queue.remove(request);
        return;
      }
      request.done.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.logDebug(
          "Lineage flush interrupted during shutdown; pending events will be drained on stop");
    }
  }

  /** Flush without blocking; logs if the queue is busy. */
  public void flushQuietly() {
    try {
      flush();
    } catch (Exception e) {
      log.logError("Error flushing lineage queue", e);
    }
  }

  /**
   * {@link #flushQuietly()}, but only once the queue is at least half full. A nested run (a
   * Pipeline Executor running a pipeline per row, say) calls this when it ends: it does not wait
   * for the sinks while they keep up, and waits for them before the queue fills up and events get
   * dropped.
   */
  public void flushIfBacklogged() {
    BlockingQueue<Object> q = queue;
    if (q == null || !running.get()) {
      return;
    }
    int size = q.size();
    if (size > 0 && size >= q.remainingCapacity()) {
      flushQuietly();
    }
  }

  /**
   * Stop the worker thread and shut down sinks. Calls are serialized: a second call waits until the
   * first one has delivered the remaining events, instead of shutting the sinks down under it. An
   * event emitted in the meantime is queued once this call is done, for the next worker or the next
   * shutdown to deliver.
   */
  public void shutdown() {
    shutdownLock.writeLock().lock();
    try {
      running.set(false);
      Thread t = worker;
      if (t != null) {
        t.interrupt();
        try {
          t.join(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      worker = null;
      drainRemaining();
      shutdownSinks();
      sinksInitialized.set(false);
      sinkInitFailed.set(false);
      sinks = List.of();
      sinkIds.clear();
      sinkInitError = null;
      LineageConfiguration.invalidate();
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  /**
   * Keep {@link #shutdownOnExit()} from shutting the hub down until {@code until} is counted down:
   * the JVM runs every shutdown hook at once, and a run that is still stopping emits its end only
   * after that. Call {@link #releaseShutdownOnExit(CountDownLatch)} once the run is over.
   */
  public void holdShutdownOnExit(CountDownLatch until) {
    exitHolds.add(until);
  }

  /** Undo {@link #holdShutdownOnExit(CountDownLatch)}. */
  public void releaseShutdownOnExit(CountDownLatch until) {
    exitHolds.remove(until);
  }

  /**
   * {@link #shutdown()} for a shutdown hook: first waits for the runs that hold it (see {@link
   * #holdShutdownOnExit(CountDownLatch)}), so that their last events are queued and delivered once,
   * instead of shutting the sinks down under a run that is still stopping.
   */
  public void shutdownOnExit() {
    try {
      for (CountDownLatch hold : exitHolds) {
        hold.await();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    shutdown();
  }

  /**
   * Synchronously dispatch any events still on the queue to the sinks, on the caller's thread.
   * Best-effort: only meaningful after the worker has stopped, so no events are added concurrently.
   */
  private void drainRemaining() {
    BlockingQueue<Object> q = queue;
    if (q == null) {
      return;
    }
    if ((!sinksInitialized.get() || sinks.isEmpty()) && targets.isEmpty()) {
      q.clear();
      return;
    }
    LineageConfiguration cfg = resolveConfig();
    List<Object> pending = new ArrayList<>();
    q.drainTo(pending);
    List<LineageEvent> batch = new ArrayList<>();
    for (Object o : pending) {
      if (o instanceof FlushRequest fr) {
        if (!batch.isEmpty()) {
          dispatchBatch(batch);
          batch.clear();
        }
        fr.done.countDown();
      } else if (o instanceof ReleaseRequest release) {
        if (!batch.isEmpty()) {
          dispatchBatch(batch);
          batch.clear();
        }
        applyRelease(release);
      } else {
        batch.add((LineageEvent) o);
        if (batch.size() >= cfg.getBatchMax()) {
          dispatchBatch(batch);
          batch.clear();
        }
      }
    }
    if (!batch.isEmpty()) {
      dispatchBatch(batch);
    }
  }

  public long getDroppedEventCount() {
    return droppedEvents.get();
  }

  private LineageConfiguration resolveConfig() {
    return fixedConfiguration != null ? fixedConfiguration : LineageConfiguration.resolve();
  }

  private void ensureWorkerStarted(LineageConfiguration cfg) {
    if (running.get()) {
      return;
    }
    synchronized (this) {
      if (running.get()) {
        return;
      }
      if (queue == null) {
        queue = new LinkedBlockingQueue<>(Math.max(1, cfg.getQueueCapacity()));
      }
      running.set(true);
      Thread thread = new Thread(this::runLoop, "Hop-LineageHub-Dispatcher");
      thread.setDaemon(true);
      worker = thread;
      thread.start();
      if (this == INSTANCE) {
        registerExitHook(Runtime.getRuntime()::addShutdownHook);
      }
    }
  }

  /**
   * Register the JVM exit hook, once. It goes through {@link #shutdownOnExit()}: the JVM starts
   * every shutdown hook at once, and hop-run may still be stopping its run on a termination signal.
   */
  void registerExitHook(Consumer<Thread> addShutdownHook) {
    if (!exitHookRegistered.compareAndSet(false, true)) {
      return;
    }
    try {
      addShutdownHook.accept(new Thread(this::shutdownOnExit, "Hop-LineageHub-Exit"));
    } catch (IllegalStateException e) {
      // The JVM is already exiting. Too late for a hook, but the event must still be queued.
      log.logDebug("The JVM is exiting, no lineage exit hook registered");
    }
  }

  private void runLoop() {
    try {
      dispatchUntilStopped();
    } finally {
      // Whatever ends this thread, callers must not keep waiting on a worker that is gone: with
      // running still true every flush() would sit out its full timeout, once per pipeline or
      // workflow completion, and every event would be queued for nobody.
      running.set(false);
      releaseWaitingFlushes();
    }
  }

  private void dispatchUntilStopped() {
    LineageConfiguration cfg = resolveConfig();
    try {
      initSinksIfNeeded();
    } catch (Exception | LinkageError e) {
      // A sink whose classes do not load (NoClassDefFoundError) ends up here too. It used to
      // kill this thread silently, leaving the hub "running" but dispatching nothing.
      sinkInitFailed.set(true);
      // None of them: some may have been initialized, the one that failed may be half-way.
      sinks = List.of();
      sinkInitError = String.valueOf(e);
      log.logError("Unable to initialize lineage sinks; lineage delivery is disabled", e);
      if (targets.isEmpty()) {
        return;
      }
      // The sinks runs selected in their run configurations are started on their own.
      sinksInitialized.set(true);
    }

    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        Object first = queue.poll(cfg.getBatchLingerMs(), TimeUnit.MILLISECONDS);
        if (first == null) {
          continue;
        }
        if (first instanceof FlushRequest fr) {
          processFlush(fr, cfg);
          continue;
        }
        if (first instanceof ReleaseRequest release) {
          applyRelease(release);
          continue;
        }
        List<LineageEvent> batch = new ArrayList<>();
        batch.add((LineageEvent) first);
        while (batch.size() < cfg.getBatchMax()) {
          Object o = queue.poll();
          if (o == null) {
            break;
          }
          if (o instanceof FlushRequest fr) {
            dispatchBatch(batch);
            batch.clear();
            processFlush(fr, cfg);
            break;
          }
          if (o instanceof ReleaseRequest release) {
            dispatchBatch(batch);
            batch.clear();
            applyRelease(release);
            break;
          }
          batch.add((LineageEvent) o);
        }
        if (!batch.isEmpty()) {
          dispatchBatch(batch);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.logError("Unexpected error in lineage dispatcher", e);
      }
    }
  }

  /** Wake up every flush() still waiting on this worker. Only meaningful once it has stopped. */
  private void releaseWaitingFlushes() {
    BlockingQueue<Object> q = queue;
    if (q == null) {
      return;
    }
    q.removeIf(
        o -> {
          if (o instanceof FlushRequest fr) {
            fr.done.countDown();
            return true;
          }
          return false;
        });
  }

  private void processFlush(FlushRequest fr, LineageConfiguration cfg) {
    List<Object> pending = new ArrayList<>();
    queue.drainTo(pending);
    List<LineageEvent> batch = new ArrayList<>();
    for (Object o : pending) {
      if (o instanceof FlushRequest nested) {
        if (!batch.isEmpty()) {
          dispatchBatch(batch);
          batch.clear();
        }
        nested.done.countDown();
      } else if (o instanceof ReleaseRequest release) {
        if (!batch.isEmpty()) {
          dispatchBatch(batch);
          batch.clear();
        }
        applyRelease(release);
      } else {
        batch.add((LineageEvent) o);
        if (batch.size() >= cfg.getBatchMax()) {
          dispatchBatch(batch);
          batch.clear();
        }
      }
    }
    if (!batch.isEmpty()) {
      dispatchBatch(batch);
    }
    fr.done.countDown();
  }

  private void dispatchBatch(List<LineageEvent> batch) {
    if (batch.isEmpty()) {
      return;
    }
    List<ILineageSink> active = sinksInitialized.get() ? sinks : List.of();
    for (ILineageSink sink : active) {
      deliver(sink, batch);
    }
    if (targets.isEmpty()) {
      return;
    }
    Map<String, List<LineageEvent>> routed = new LinkedHashMap<>();
    for (LineageEvent event : batch) {
      for (String key : event.getContext().getSinks()) {
        routed.computeIfAbsent(key, k -> new ArrayList<>()).add(event);
      }
    }
    for (Map.Entry<String, List<LineageEvent>> entry : routed.entrySet()) {
      ILineageSink sink = routedSink(entry.getKey());
      if (sink != null) {
        deliver(sink, entry.getValue());
      }
    }
  }

  private void deliver(ILineageSink sink, List<LineageEvent> events) {
    try {
      sink.accept(new ArrayList<>(events));
    } catch (Exception e) {
      log.logError("Lineage sink failed: " + sink.getClass().getName(), e);
    }
  }

  /**
   * The started sink of a target, starting it on its first event. Null when the target is unknown
   * or its sink failed to start; that is logged, the sink is tried again a minute later, and the
   * other sinks carry on.
   */
  private ILineageSink routedSink(String key) {
    ILineageSink sink = routedSinks.get(key);
    if (sink != null || waitsForRetry(key)) {
      return sink;
    }
    synchronized (routedSinks) {
      sink = routedSinks.get(key);
      LineageSinkTarget target = targets.get(key);
      if (sink != null || target == null || waitsForRetry(key)) {
        return sink;
      }
      failedTargets.remove(key);
      try {
        sink = sinkFactory.create(target);
        Variables settings = new Variables();
        target.settings().forEach(settings::setVariable);
        sink.init(settings, log);
        routedSinks.put(key, sink);
        return sink;
      } catch (Exception | LinkageError e) {
        failedTargets.put(key, new Failure(System.currentTimeMillis(), String.valueOf(e)));
        if (sink != null) {
          // It may have started a thread or opened its spool before it failed.
          shutdownQuietly(sink);
        }
        log.logError(
            "Unable to start lineage sink '"
                + target.name()
                + "'; the runs that selected it don't report their lineage to it. It is tried"
                + " again in a minute.",
            e);
        return null;
      }
    }
  }

  private boolean waitsForRetry(String key) {
    Failure failure = failedTargets.get(key);
    return failure != null && System.currentTimeMillis() - failure.timeMillis() < retryFailedSinkMs;
  }

  /** The sink plugin of a target, from the plugin registry. */
  private static ILineageSink loadSink(LineageSinkTarget target) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId(LineageSinkPluginType.class, target.pluginId());
    if (plugin == null) {
      throw new HopException(
          "The lineage sink type '"
              + target.pluginId()
              + "' is not available: is its plugin installed?");
    }
    return registry.loadClass(plugin, ILineageSink.class);
  }

  private void initSinksIfNeeded() throws HopException {
    if (sinksInitialized.get()) {
      return;
    }
    synchronized (this) {
      if (sinksInitialized.get()) {
        return;
      }
      if (!resolveConfig().isEnabled()) {
        // Only the sinks runs selected in their run configurations: they start on their own.
        sinks = List.of();
        sinksInitialized.set(true);
        return;
      }
      IVariables variables = LineageVariables.engineVariables();

      if (explicitSinks != null) {
        sinks = List.copyOf(explicitSinks);
        for (ILineageSink sink : sinks) {
          sinkIds.put(sink, sink.getClass().getSimpleName());
          sink.init(variables, log);
        }
        sinksInitialized.set(true);
        return;
      }

      List<ILineageSink> loaded = new ArrayList<>();
      LineageConfiguration cfg = resolveConfig();
      PluginRegistry registry = PluginRegistry.getInstance();
      for (IPlugin plugin : registry.getPlugins(LineageSinkPluginType.class)) {
        String id = plugin.getIds()[0].toLowerCase(Locale.ROOT);
        if (!cfg.getSinkIds().isEmpty() && !cfg.getSinkIds().contains(id)) {
          continue;
        }
        try {
          ILineageSink sink = registry.loadClass(plugin, ILineageSink.class);
          sinkIds.put(sink, plugin.getIds()[0]);
          sink.init(variables, log);
          loaded.add(sink);
        } catch (HopPluginException e) {
          throw new HopException("Unable to load lineage sink plugin " + plugin.getName(), e);
        } catch (LinkageError e) {
          // A sink built on another plugin (DataHub on OpenLineage) installed without it.
          throw new HopException(
              "Unable to load lineage sink plugin "
                  + plugin.getName()
                  + ": a class it needs is missing ("
                  + e.getMessage()
                  + "). Check that the plugins it builds on are installed as well.",
              e);
        }
      }
      sinks = List.copyOf(loaded);
      sinksInitialized.set(true);
    }
  }

  private void shutdownSinks() {
    List<ILineageSink> all = new ArrayList<>(sinks);
    synchronized (routedSinks) {
      all.addAll(routedSinks.values());
      routedSinks.clear();
      failedTargets.clear();
    }
    for (ILineageSink sink : all) {
      shutdownQuietly(sink);
    }
  }

  private static final class FlushRequest {
    final CountDownLatch done = new CountDownLatch(1);
  }
}
