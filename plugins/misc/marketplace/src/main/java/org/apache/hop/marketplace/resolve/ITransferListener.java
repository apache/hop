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

package org.apache.hop.marketplace.resolve;

/**
 * Byte-level progress of a single HTTP download, so callers can show a moving progress bar and
 * cancel a transfer in flight.
 *
 * <p>Deliberately free of any UI type: the CLI can log lines, the GUI adapts it to an {@code
 * IProgressMonitor}. Every method has a no-op default so {@link #NONE} and lambdas stay cheap.
 *
 * <p>Callbacks arrive on the thread doing the download, which is <strong>not</strong> the SWT UI
 * thread. Implementations that touch widgets must marshal, and should throttle: {@link
 * #transferred(long, long)} fires once per network chunk.
 */
public interface ITransferListener {

  /** A listener that reports nothing and never cancels. */
  ITransferListener NONE = new ITransferListener() {};

  /**
   * A transfer is about to begin.
   *
   * @param label human-readable name of what is being fetched (a GAV, a file name)
   * @param totalBytes size from {@code Content-Length}, or -1 when the server does not say (chunked
   *     or compressed transfer). Listeners should switch to an indeterminate display for -1.
   */
  default void started(String label, long totalBytes) {
    // no-op
  }

  /**
   * Progress of the current transfer.
   *
   * @param bytesSoFar bytes written to the target file so far
   * @param totalBytes same value as passed to {@link #started(String, long)}, or -1 if unknown
   */
  default void transferred(long bytesSoFar, long totalBytes) {
    // no-op
  }

  /**
   * Polled between chunks. Returning true aborts the download; the partially written file is
   * removed and the caller sees a {@code HopException}.
   */
  default boolean isCancelled() {
    return false;
  }
}
