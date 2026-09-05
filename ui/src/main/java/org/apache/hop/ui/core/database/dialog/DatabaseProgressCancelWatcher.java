/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.database.dialog;

import java.util.function.Supplier;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.database.Database;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.util.EnvironmentUtils;

/**
 * Background watcher that cancels a running {@link Database} query when the user presses Cancel on
 * a {@link ProgressMonitorDialog}.
 *
 * <p>Hop Web's {@code ProgressMonitorDialog.run} executes inline and never builds a shell, so a
 * loop of {@code getShell() == null} would never end. Do not start a watcher there.
 */
public final class DatabaseProgressCancelWatcher {

  private DatabaseProgressCancelWatcher() {}

  /**
   * Start a named daemon thread that calls {@link Database#cancelQuery()} when the monitor is
   * cancelled. No-op on Hop Web.
   *
   * @param pmd the progress dialog whose Cancel button is watched
   * @param database the connection to cancel; may return {@code null} until connect
   * @param threadName thread name for dumps (e.g. {@code Hop-SqlEditor-CancelWatcher})
   */
  public static void startIfDesktop(
      ProgressMonitorDialog pmd, Supplier<Database> database, String threadName) {
    if (pmd == null || EnvironmentUtils.getInstance().isWeb()) {
      return;
    }
    Thread thread =
        new Thread(
            () -> {
              IProgressMonitor monitor = pmd.getProgressMonitor();
              while (pmd.getShell() == null
                  || (!pmd.getShell().isDisposed() && !monitor.isCanceled())) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
              if (monitor.isCanceled()) {
                Database db = database.get();
                if (db != null) {
                  try {
                    db.cancelQuery();
                  } catch (Exception ignored) {
                    // ignore
                  }
                }
              }
            },
            threadName);
    thread.setDaemon(true);
    thread.start();
  }
}
