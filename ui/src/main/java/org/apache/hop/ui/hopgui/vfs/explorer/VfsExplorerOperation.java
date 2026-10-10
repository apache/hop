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
import org.apache.hop.core.Const;

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

  /**
   * Record {@code thrown} and stop the clock. A missing library is an {@link Error}, and {@link
   * ExceptionInInitializerError} often has no message of its own, so the text includes the cause.
   */
  public void fail(Throwable thrown) {
    fail(describe(thrown), Const.getClassicStackTrace(thrown));
  }

  /**
   * Stop wins over the failure: the user already interrupted this operation. Otherwise the failure
   * is recorded, including an {@link Error} from a VFS driver.
   */
  void failUnlessCancelled(Throwable thrown) {
    if (isCancelled()) {
      complete();
      return;
    }
    fail(thrown);
  }

  /**
   * Status text for a driver failure. Walks to the first cause that has a message, and names an
   * {@link Error} or {@link ClassNotFoundException} so a bare missing-class name is readable.
   */
  static String describe(Throwable thrown) {
    if (thrown == null) {
      return "";
    }
    String message = messageOf(thrown);
    Throwable cause = thrown.getCause();
    if (cause != null && cause != thrown && isDriverFailure(cause)) {
      String causeText = describe(cause);
      if (!causeText.isEmpty() && !message.contains(causeText)) {
        message = message + ": " + causeText;
      }
    }
    return message;
  }

  private static String messageOf(Throwable thrown) {
    Throwable reported = thrown;
    while (isBlank(reported.getMessage())
        && reported.getCause() != null
        && reported.getCause() != reported) {
      reported = reported.getCause();
    }
    String message = reported.getMessage();
    if (isBlank(message)) {
      message = reported.toString();
    }
    if (reported instanceof Error || reported instanceof ClassNotFoundException) {
      String type = reported.getClass().getSimpleName();
      if (!message.startsWith(type)) {
        message = type + ": " + message;
      }
    }
    return message;
  }

  private static boolean isDriverFailure(Throwable thrown) {
    Throwable current = thrown;
    while (current != null) {
      if (current instanceof Error || current instanceof ClassNotFoundException) {
        return true;
      }
      Throwable cause = current.getCause();
      current = cause == current ? null : cause;
    }
    return false;
  }

  private static boolean isBlank(String text) {
    return text == null || text.isBlank();
  }

  public boolean isFinished() {
    return status != Status.RUNNING;
  }

  public long elapsedMillis() {
    long end = endTime > 0 ? endTime : System.currentTimeMillis();
    return Math.max(0, end - startTime);
  }
}
