/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.util.Set;

/**
 * Interruptible idle wait; listing stays on the transform thread with its VFS execution context.
 */
public class VfsPollingWatcher implements FileWatcher {
  private boolean closed;

  @Override
  public synchronized WatchBatch poll(long timeoutMillis) throws InterruptedException {
    if (!closed && timeoutMillis > 0) {
      wait(timeoutMillis);
    }
    return new WatchBatch(Set.of(), false, 0);
  }

  @Override
  public synchronized void close() {
    closed = true;
    notifyAll();
  }
}
