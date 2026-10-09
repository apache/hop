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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import lombok.Value;

/** Hints are bounded. A reconciliation flag replaces excess hints without losing final state. */
public interface FileWatcher extends AutoCloseable {
  WatchBatch poll(long timeoutMillis) throws IOException, InterruptedException;

  default void reconcileDirectories() throws IOException {}

  @Override
  void close() throws IOException;

  @Value
  class WatchBatch {
    Set<Path> paths;
    boolean reconciliationRequired;
    long overflowCount;
  }
}
