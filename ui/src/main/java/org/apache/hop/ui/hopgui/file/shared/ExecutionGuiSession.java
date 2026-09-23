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

/**
 * Binds GUI refresh timers to the execution currently on screen.
 *
 * <p>{@link #adopt(Object, Runnable)} and {@link #stopIfCurrent(Object, Runnable)} share one lock.
 * A finished listener from the previous engine therefore cannot stop the timers of the engine that
 * replaced it. The runnable must not wait on the GUI thread: that thread may be blocked in {@code
 * adopt}.
 */
public final class ExecutionGuiSession {

  private final Object lock = new Object();
  private Object engine;
  private int generation;

  /**
   * Show {@code engine}. {@code bind} runs under the lock before the session publishes the engine,
   * so readers either see the previous pair or this one.
   *
   * @return generation timer tasks must carry
   */
  public int adopt(Object engine, Runnable bind) {
    synchronized (lock) {
      if (bind != null) {
        bind.run();
      }
      this.engine = engine;
      return ++generation;
    }
  }

  public int adopt(Object engine) {
    return adopt(engine, null);
  }

  public int generation() {
    synchronized (lock) {
      return generation;
    }
  }

  public boolean isCurrent(Object engine, int generation) {
    synchronized (lock) {
      return engine != null && this.engine == engine && this.generation == generation;
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
   * check and {@code stop}.
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
}
