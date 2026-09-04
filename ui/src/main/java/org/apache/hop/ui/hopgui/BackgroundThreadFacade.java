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

package org.apache.hop.ui.hopgui;

/**
 * Starts the background threads dialogs use to look up their input fields, in a way that keeps
 * session scoped GUI state reachable.
 *
 * <p>A dialog that fetches the fields of the previous transform does so on a thread of its own so
 * the dialog can already be shown. That thread then reads GUI state - {@link
 * org.apache.hop.ui.core.PropsUi} through {@code ConstUi.sortFieldNames}, images from {@link
 * org.apache.hop.ui.core.gui.GuiResource} - which in Hop Web belongs to a RAP {@code UISession}.
 * RAP hands out session state by looking up the session bound to the calling thread, and a plain
 * {@code new Thread(runnable).start()} has none: the lookup throws {@code IllegalStateException:
 * Invalid thread access}, the thread dies and the dialog opens with empty combo boxes, its only
 * trace an uncaught exception in the server log.
 *
 * <p>So the session is captured here, on the UI thread that starts the work, and made current again
 * inside the new thread. On the desktop there is no session and this is a plain thread.
 *
 * <p>This does not make the thread a UI thread: widgets still have to be touched through {@code
 * Display.asyncExec}. It only carries over the session identity that tells Hop Web whose GUI this
 * work belongs to.
 */
public abstract class BackgroundThreadFacade {

  private static final BackgroundThreadFacade IMPL = load();

  private static BackgroundThreadFacade load() {
    try {
      return (BackgroundThreadFacade)
          ImplementationLoader.newInstance(BackgroundThreadFacade.class);
    } catch (Throwable e) {
      // Unit tests of the hop-ui module have neither rcp nor rap on the classpath, and outside
      // Hop Web there is no session to carry over anyway.
      return new BackgroundThreadFacade() {
        @Override
        Runnable bindInternal(Runnable runnable) {
          return runnable;
        }
      };
    }
  }

  /**
   * Runs the given work on a new thread, from which session scoped GUI state stays reachable.
   *
   * <p>Call this on the UI thread: that is where the session it carries over is read.
   *
   * @param runnable the work to run in the background
   * @return the thread that was started, already running
   */
  public static Thread start(Runnable runnable) {
    return start(runnable, null);
  }

  /**
   * Runs the given work on a new named thread, from which session scoped GUI state stays reachable.
   *
   * @param runnable the work to run in the background
   * @param threadName name for the new thread, or null to let the JVM name it
   * @return the thread that was started, already running
   */
  public static Thread start(Runnable runnable, String threadName) {
    Runnable bound = bind(runnable);
    Thread thread = threadName == null ? new Thread(bound) : new Thread(bound, threadName);
    thread.start();
    return thread;
  }

  /**
   * The same work, wrapped so that it keeps the current session when it runs on another thread. For
   * callers that create the thread themselves because they configure it first.
   *
   * <p>Call this on the UI thread, not on the thread that will run the work.
   *
   * @param runnable the work to wrap
   * @return the wrapped work
   */
  public static Runnable bind(Runnable runnable) {
    return IMPL.bindInternal(runnable);
  }

  abstract Runnable bindInternal(Runnable runnable);
}
