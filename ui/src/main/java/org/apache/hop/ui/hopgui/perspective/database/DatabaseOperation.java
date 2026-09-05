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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.ProgressNullMonitorListener;
import org.apache.hop.core.database.Database;

/**
 * One long-running database action (connect, list schemas, execute SQL, …). Cancel calls {@link
 * Database#cancelQuery()} on the JDBC instance attached to this operation, not the tree.
 */
@Getter
public class DatabaseOperation {

  public enum Status {
    RUNNING,
    DONE,
    FAILED,
    CANCELLED
  }

  @FunctionalInterface
  public interface Work {
    void run(DatabaseOperation operation) throws Exception;
  }

  private final String id = UUID.randomUUID().toString();
  private final String description;
  private final String connectionName;
  private final long startTime = System.currentTimeMillis();

  private volatile Status status = Status.RUNNING;
  private volatile long endTime;
  private volatile String errorMessage;
  private volatile boolean cancelled;

  private final AtomicReference<Database> database = new AtomicReference<>();

  public DatabaseOperation(String description, String connectionName) {
    this.description = description;
    this.connectionName = connectionName == null ? "" : connectionName;
  }

  public void attachDatabase(Database db) {
    database.set(db);
  }

  public void cancel() {
    cancelled = true;
    Database db = database.get();
    if (db != null) {
      try {
        db.cancelQuery();
      } catch (Exception ignored) {
        // Best-effort; the worker still sees isCancelled().
      }
    }
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public IProgressMonitor newMonitor() {
    return new CancelMonitor();
  }

  public void complete() {
    status = cancelled ? Status.CANCELLED : Status.DONE;
    endTime = System.currentTimeMillis();
  }

  public void fail(String message) {
    status = cancelled ? Status.CANCELLED : Status.FAILED;
    errorMessage = message;
    endTime = System.currentTimeMillis();
  }

  public long elapsedMillis() {
    long end = endTime > 0 ? endTime : System.currentTimeMillis();
    return Math.max(0, end - startTime);
  }

  public boolean isFinished() {
    return status != Status.RUNNING;
  }

  private final class CancelMonitor extends ProgressNullMonitorListener {
    @Override
    public boolean isCanceled() {
      return cancelled;
    }
  }
}
