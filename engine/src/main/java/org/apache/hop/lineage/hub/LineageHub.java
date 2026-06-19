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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.lineage.spi.ILineageSink;

/**
 * Central async hub for lineage events. When lineage is disabled (see {@link
 * LineageConfiguration}), {@link #emit(LineageEvent)} returns immediately without queuing.
 */
public final class LineageHub {

  private static final LineageHub INSTANCE = new LineageHub(null, null);

  private final ILogChannel log = new LogChannel("LineageHub");

  /** When non-null, used instead of loading sinks from the plugin registry (tests). */
  private final List<ILineageSink> explicitSinks;

  private final LineageConfiguration fixedConfiguration;

  // Lazily allocated in ensureWorkerStarted so the singleton picks up the resolved
  // HOP_LINEAGE_QUEUE_CAPACITY on first emit (Hop variables are not available at class-init time).
  private volatile BlockingQueue<Object> queue;

  private final AtomicLong droppedEvents = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Thread worker;

  private volatile List<ILineageSink> sinks = List.of();
  private final AtomicBoolean sinksInitialized = new AtomicBoolean(false);

  private LineageHub(LineageConfiguration fixedConfiguration, List<ILineageSink> explicitSinks) {
    this.fixedConfiguration = fixedConfiguration;
    this.explicitSinks = explicitSinks;
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
  static LineageHub newIsolatedForTesting(
      LineageConfiguration configuration, List<ILineageSink> sinks) {
    return new LineageHub(configuration, new ArrayList<>(sinks));
  }

  /** Invoked after {@link org.apache.hop.core.HopEnvironment} finishes plugin registration. */
  public void environmentReady() {
    sinksInitialized.set(false);
    sinks = List.of();
  }

  /**
   * Queue a lineage event. No-op when lineage is disabled. Drops the event when the queue is full
   * and increments an internal drop counter.
   */
  public void emit(LineageEvent event) {
    LineageConfiguration cfg = resolveConfig();
    if (!cfg.isEnabled()) {
      return;
    }
    ensureWorkerStarted(cfg);
    if (!queue.offer(event)) {
      droppedEvents.incrementAndGet();
      log.logError(
          "Lineage event dropped because the queue is full (capacity "
              + cfg.getQueueCapacity()
              + "). Total drops: "
              + droppedEvents.get());
    }
  }

  /**
   * Wait until queued events (including flush markers) have been processed by sinks. Safe to call
   * when disabled (no-op).
   */
  public void flush() {
    LineageConfiguration cfg = resolveConfig();
    if (!cfg.isEnabled() || !running.get()) {
      return;
    }
    FlushRequest request = new FlushRequest();
    try {
      queue.put(request);
      request.done.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.logError("Interrupted while flushing lineage queue", e);
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

  /** Stop the worker thread and shut down sinks. */
  public void shutdown() {
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
    shutdownSinks();
    BlockingQueue<Object> q = queue;
    if (q != null) {
      q.clear();
    }
    sinksInitialized.set(false);
    sinks = List.of();
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
    }
  }

  private void runLoop() {
    LineageConfiguration cfg = resolveConfig();
    try {
      initSinksIfNeeded();
    } catch (HopException e) {
      log.logError("Unable to initialize lineage sinks; lineage delivery is disabled", e);
      running.set(false);
      return;
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
    running.set(false);
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
    List<ILineageSink> active = sinks;
    for (ILineageSink sink : active) {
      try {
        sink.accept(new ArrayList<>(batch));
      } catch (Exception e) {
        log.logError("Lineage sink failed: " + sink.getClass().getName(), e);
      }
    }
  }

  private void initSinksIfNeeded() throws HopException {
    if (sinksInitialized.get()) {
      return;
    }
    synchronized (this) {
      if (sinksInitialized.get()) {
        return;
      }
      Variables variables = new Variables();
      variables.initializeFrom(null);

      if (explicitSinks != null) {
        sinks = List.copyOf(explicitSinks);
        for (ILineageSink sink : sinks) {
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
          sink.init(variables, log);
          loaded.add(sink);
        } catch (HopPluginException e) {
          throw new HopException("Unable to load lineage sink plugin " + plugin.getName(), e);
        }
      }
      sinks = List.copyOf(loaded);
      sinksInitialized.set(true);
    }
  }

  private void shutdownSinks() {
    for (ILineageSink sink : sinks) {
      try {
        sink.shutdown();
      } catch (Exception e) {
        log.logError("Error shutting down lineage sink " + sink.getClass().getName(), e);
      }
    }
  }

  private static final class FlushRequest {
    final CountDownLatch done = new CountDownLatch(1);
  }
}
