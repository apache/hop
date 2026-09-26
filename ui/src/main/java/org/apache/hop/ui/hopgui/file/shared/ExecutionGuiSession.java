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

package org.apache.hop.ui.hopgui.file.shared;

import java.util.Timer;
import java.util.TimerTask;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Binds GUI refresh timers to the execution currently on screen.
 *
 * <p>{@link #adopt(Object, Runnable)} and {@link #stopIfCurrent(Object, Runnable)} share one lock,
 * so a finished listener from the previous engine cannot stop the timers of the engine that
 * replaced it. {@code stop} and anything run from {@link #scheduleWhileCurrent} must not take the
 * graph lock or wait on the GUI thread. The GUI thread takes this lock while adopting an engine,
 * and a listener that waits for the GUI while holding it deadlocks.
 */
public final class ExecutionGuiSession {

  /** One engine and the generation assigned to it, read under the session lock. */
  public record Snapshot(Object engine, int generation) {}

  private final Object lock = new Object();
  private Object engine;
  private int generation;

  /**
   * Show {@code engine}. {@code bind} runs under the lock before the session publishes the engine,
   * so readers either see the previous pair or this one.
   */
  public Snapshot adopt(Object engine, Runnable bind) {
    synchronized (lock) {
      if (bind != null) {
        bind.run();
      }
      this.engine = engine;
      return new Snapshot(engine, ++generation);
    }
  }

  public Snapshot adopt(Object engine) {
    return adopt(engine, null);
  }

  /** Engine and generation as of one lock acquisition. */
  public Snapshot current() {
    synchronized (lock) {
      return new Snapshot(engine, generation);
    }
  }

  public boolean isCurrent(Object engine, int generation) {
    synchronized (lock) {
      return engine != null && this.engine == engine && this.generation == generation;
    }
  }

  /** True when {@code engine} is the one on screen, ignoring generation. */
  public boolean isCurrentEngine(Object engine) {
    synchronized (lock) {
      return engine != null && this.engine == engine;
    }
  }

  /**
   * Run {@code action} only while {@code engine} is still the one adopted at {@code generation}.
   * The lock is held for the whole action.
   */
  public boolean runIfCurrent(Object engine, int generation, Runnable action) {
    synchronized (lock) {
      if (engine == null || this.engine != engine || this.generation != generation) {
        return false;
      }
      if (action != null) {
        action.run();
      }
      return true;
    }
  }

  /**
   * Run {@code stop} only if {@code engine} is still the one on screen. The lock is held across the
   * check and {@code stop}. {@code stop} must not take the graph lock or wait on the GUI thread.
   */
  public boolean stopIfCurrent(Object engine, Runnable stop) {
    synchronized (lock) {
      if (engine == null || this.engine != engine) {
        return false;
      }
      if (stop != null) {
        stop.run();
      }
      return true;
    }
  }

  /**
   * Replace the caller's timer with one that runs {@code onTick} while {@code snapshot} is current.
   * A tick that finds a newer engine cancels the timer, so a listener that no longer owns the
   * screen does not have to. {@code allow}, {@code replace} and {@code onTick} run on the timer
   * thread or under the session lock and must not wait on the GUI thread.
   *
   * @param allow checked under the lock before the timer is stored. When it returns false, nothing
   *     is scheduled.
   * @param replace stores the new timer and drops the previous one. Called under the lock.
   * @return false when nothing was scheduled
   */
  public boolean scheduleWhileCurrent(
      Snapshot snapshot,
      String threadName,
      long periodMs,
      BooleanSupplier allow,
      Consumer<Timer> replace,
      Runnable onTick) {
    if (snapshot == null || snapshot.engine() == null || replace == null || onTick == null) {
      return false;
    }
    Timer timer = new Timer(threadName);
    TimerTask task =
        new TimerTask() {
          @Override
          public void run() {
            if (!isCurrent(snapshot.engine(), snapshot.generation())) {
              timer.cancel();
              return;
            }
            onTick.run();
          }
        };
    boolean[] started = {false};
    runIfCurrent(
        snapshot.engine(),
        snapshot.generation(),
        () -> {
          if (allow != null && !allow.getAsBoolean()) {
            return;
          }
          replace.accept(timer);
          timer.schedule(task, 0L, periodMs);
          started[0] = true;
        });
    if (!started[0]) {
      timer.cancel();
    }
    return started[0];
  }
}
