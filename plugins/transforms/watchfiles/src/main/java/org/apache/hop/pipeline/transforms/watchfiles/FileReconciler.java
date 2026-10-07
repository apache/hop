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

import java.util.Map;
import java.util.function.Consumer;

/** Only complete successful snapshots may be passed here: a failed listing is not a deletion. */
public class FileReconciler {
  public void compare(
      Map<String, FileState> observed,
      Map<String, FileState> current,
      long now,
      Consumer<FileChangeEvent> changes) {
    for (FileState file : current.values()) {
      FileState previous = observed.get(file.getUri());
      if (previous == null) {
        changes.accept(new FileChangeEvent(FileEventType.CREATED, file, null, now));
      } else if (!file.sameVersion(previous)) {
        changes.accept(new FileChangeEvent(FileEventType.MODIFIED, file, previous, now));
      }
    }
    for (FileState file : observed.values()) {
      if (!current.containsKey(file.getUri())) {
        changes.accept(new FileChangeEvent(FileEventType.DELETED, null, file, now));
      }
    }
  }
}
