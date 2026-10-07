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

package org.apache.hop.ui.hopgui.vfs.explorer;

import lombok.Getter;

/** One listing or file change. Stop interrupts the worker; it does not close the file system. */
@Getter
public class VfsExplorerOperation {

  public enum Status {
    RUNNING,
    DONE,
    FAILED,
    CANCELLED
  }

  private final String description;
  private final String location;
  private final long startTime = System.currentTimeMillis();

  private volatile Status status = Status.RUNNING;
  private volatile long endTime;
  private volatile String errorMessage;
  private volatile String detail;
  private volatile boolean cancelled;
  private volatile Thread worker;

  public VfsExplorerOperation(String description, String location) {
    this.description = description == null ? "" : description;
    this.location = location == null ? "" : location;
  }

  public void attachThread(Thread thread) {
    worker = thread;
    if (cancelled && thread != null) {
      thread.interrupt();
    }
  }

  public void cancel() {
    cancelled = true;
    Thread thread = worker;
    if (thread != null) {
      thread.interrupt();
    }
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public void complete() {
    status = cancelled ? Status.CANCELLED : Status.DONE;
    endTime = System.currentTimeMillis();
    worker = null;
  }

  public void fail(String message, String detail) {
    status = cancelled ? Status.CANCELLED : Status.FAILED;
    errorMessage = message;
    this.detail = detail;
    endTime = System.currentTimeMillis();
    worker = null;
  }

  public boolean isFinished() {
    return status != Status.RUNNING;
  }

  public long elapsedMillis() {
    long end = endTime > 0 ? endTime : System.currentTimeMillis();
    return Math.max(0, end - startTime);
  }
}
