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

import lombok.Getter;

/** Normalized row from a Git hosting provider API (commit, issue, or pull request). */
@Getter
public class GitResourceRecord {

  private final String provider;
  private final String entityType;
  private final String repoOwner;
  private final String repoName;
  private final String id;
  private final long number;
  private final String title;
  private final String state;
  private final String author;
  private final String createdAt;
  private final String updatedAt;
  private final String closedAt;
  private final String url;
  private final String body;
  private final String sha;
  private final String sourceBranch;
  private final String targetBranch;
  private final String merged;
  private final String rawJson;

  public GitResourceRecord(
      String provider,
      String entityType,
      String repoOwner,
      String repoName,
      String id,
      long number,
      String title,
      String state,
      String author,
      String createdAt,
      String updatedAt,
      String closedAt,
      String url,
      String body,
      String sha,
      String sourceBranch,
      String targetBranch,
      String merged,
      String rawJson) {
    this.provider = provider;
    this.entityType = entityType;
    this.repoOwner = repoOwner;
    this.repoName = repoName;
    this.id = id;
    this.number = number;
    this.title = title;
    this.state = state;
    this.author = author;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.closedAt = closedAt;
    this.url = url;
    this.body = body;
    this.sha = sha;
    this.sourceBranch = sourceBranch;
    this.targetBranch = targetBranch;
    this.merged = merged;
    this.rawJson = rawJson;
  }

  public Object[] toRow() {
    return toRow(true);
  }

  public Object[] toRow(boolean includeRawJson) {
    Object[] row = new Object[GitInputFields.fieldCount(includeRawJson)];
    row[0] = provider;
    row[1] = entityType;
    row[2] = repoOwner;
    row[3] = repoName;
    row[4] = id;
    row[5] = number;
    row[6] = title;
    row[7] = state;
    row[8] = author;
    row[9] = GitTimestamps.toDate(createdAt);
    row[10] = GitTimestamps.toDate(updatedAt);
    row[11] = GitTimestamps.toDate(closedAt);
    row[12] = url;
    row[13] = body;
    row[14] = sha;
    row[15] = sourceBranch;
    row[16] = targetBranch;
    row[17] = merged;
    if (includeRawJson) {
      row[GitInputFields.RAW_JSON_FIELD_INDEX] = rawJson;
    }
    return row;
  }
}
