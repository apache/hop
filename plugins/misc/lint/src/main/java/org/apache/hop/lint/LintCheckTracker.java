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
package org.apache.hop.lint;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Tracks which files need linting based on last modification vs last check time. */
public class LintCheckTracker {

  private final Map<String, Long> lastCheckTimes = new ConcurrentHashMap<>();
  private final Map<String, Long> lastModifiedTimes = new ConcurrentHashMap<>();

  public boolean needsLinting(String filePath) {
    if (filePath == null || filePath.isEmpty()) {
      return false;
    }
    File file = new File(filePath);
    if (!file.exists()) {
      return false;
    }
    long lastModified = file.lastModified();
    Long lastCheck = lastCheckTimes.get(normalize(filePath));
    return lastCheck == null || lastModified > lastCheck;
  }

  public void markChecked(String filePath) {
    if (filePath == null || filePath.isEmpty()) {
      return;
    }
    File file = new File(filePath);
    if (file.exists()) {
      String key = normalize(filePath);
      lastCheckTimes.put(key, System.currentTimeMillis());
      lastModifiedTimes.put(key, file.lastModified());
    }
  }

  public List<String> filterFilesNeedingCheck(List<String> filePaths) {
    return filePaths.stream().filter(this::needsLinting).collect(Collectors.toList());
  }

  public void invalidate(String filePath) {
    if (filePath != null) {
      lastCheckTimes.remove(normalize(filePath));
    }
  }

  private String normalize(String filePath) {
    try {
      return new File(filePath).getAbsolutePath();
    } catch (Exception e) {
      return filePath;
    }
  }
}
