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

package org.apache.hop.git.provider;

import java.util.ArrayList;
import java.util.List;

public enum GitResourceType {
  COMMITS("commits"),
  COMMIT_FILES("commit_files"),
  ISSUES("issues"),
  PULL_REQUESTS("pull_requests"),
  ISSUE_COMMENTS("issue_comments"),
  PR_COMMENTS("pr_comments"),
  ISSUE_EVENTS("issue_events");

  private final String entityType;

  GitResourceType(String entityType) {
    this.entityType = entityType;
  }

  public String getEntityType() {
    return entityType;
  }

  public static String[] labels() {
    GitResourceType[] values = values();
    String[] labels = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      labels[i] = values[i].name();
    }
    return labels;
  }

  public static GitResourceType fromStored(String stored) {
    if (stored == null || stored.isBlank()) {
      return COMMITS;
    }
    return valueOf(stored.trim());
  }

  public static String[] remoteLabels() {
    List<String> labels = new ArrayList<>();
    for (GitResourceType type : values()) {
      if (type != COMMIT_FILES) {
        labels.add(type.name());
      }
    }
    return labels.toArray(String[]::new);
  }

  public static String[] localLabels() {
    return new String[] {COMMITS.name(), COMMIT_FILES.name()};
  }

  public boolean isLocalOnly() {
    return this == COMMIT_FILES;
  }
}
