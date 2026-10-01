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

package org.apache.hop.core.scope;

/**
 * Holds one "current" value, for the ambient state Hop keeps outside of any object it can hand
 * around: the metadata provider in use, the VFS namespace to resolve files in.
 *
 * <p>How far "current" reaches depends on how Hop is running, and that is the only part that
 * differs. In a client, or in a server for state set once at startup, it is the whole process. For
 * state an execution owns it is the thread and the threads it starts. In Hop Web one JVM serves
 * many people at once and neither answer is right: there it has to be the session, or one user's
 * project silently becomes another's.
 *
 * <p>Each piece of ambient state owns its scope and picks the default that suits it - see {@link
 * #process()} and {@link #inheritedByThreads()}. A runtime that needs different reach implements
 * this once and hands an instance to each of them.
 *
 * @param <T> the type of the value held
 */
public interface IHopScope<T> {

  /**
   * The current value, or null when nothing is set.
   *
   * @return the current value, may be null
   */
  T get();

  /**
   * Set the current value.
   *
   * @param value the value to set
   */
  void set(T value);

  /** Clear the current value. */
  void remove();

  /**
   * A scope reaching the whole process: one value, shared by everything. The right default for
   * state that is set once at startup.
   *
   * @param <T> the type of the value held
   * @return a new process wide scope
   */
  static <T> IHopScope<T> process() {
    return new ProcessScope<>();
  }

  /**
   * A scope reaching the current thread and the threads it starts. The right default for state an
   * execution owns: {@code Pipeline} and {@code Workflow} hand their work to threads they start
   * themselves, so those inherit it, while a second execution running alongside keeps its own.
   *
   * @param <T> the type of the value held
   * @return a new thread scope
   */
  static <T> IHopScope<T> inheritedByThreads() {
    return new ThreadScope<>();
  }

  /**
   * @see IHopScope#process()
   */
  class ProcessScope<T> implements IHopScope<T> {
    private volatile T value;

    @Override
    public T get() {
      return value;
    }

    @Override
    public void set(T value) {
      this.value = value;
    }

    @Override
    public void remove() {
      this.value = null;
    }
  }

  /**
   * @see IHopScope#inheritedByThreads()
   */
  class ThreadScope<T> implements IHopScope<T> {
    private final InheritableThreadLocal<T> value = new InheritableThreadLocal<>();

    @Override
    public T get() {
      return value.get();
    }

    @Override
    public void set(T value) {
      this.value.set(value);
    }

    @Override
    public void remove() {
      value.remove();
    }
  }
}
