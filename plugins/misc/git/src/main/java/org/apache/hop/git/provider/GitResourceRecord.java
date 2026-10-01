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

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.apache.hop.git.provider.GitInputFields.Field;

/**
 * Normalized row from a Git hosting provider or a local clone.
 *
 * <p>A record carries every value any resource type can report; {@link #toRow} then keeps the ones
 * the type's layout actually asks for. A provider that does not report a field simply never sets
 * it, and the field arrives in the row as an empty value rather than as a missing column.
 */
@Getter
@Builder
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
  private final String authorEmail;
  private final String authorLogin;
  private final String committer;
  private final String committerEmail;
  private final String labels;
  private final String assignees;
  private final String createdAt;
  private final String updatedAt;
  private final String closedAt;
  private final String mergedAt;
  private final String url;
  private final String body;
  private final String sha;
  private final String sourceBranch;
  private final String targetBranch;

  /** Whether a pull request was merged. Null when the row is not a pull request. */
  private final Boolean merged;

  /** Whether a commit has more than one parent. Null when the row is not a commit. */
  private final Boolean isMerge;

  private final String rawJson;

  /** Builds the output row for a resource type, in the order {@link GitInputFields} declares. */
  public Object[] toRow(GitResourceType resourceType, boolean includeRawJson) {
    List<Field> layout = GitInputFields.layout(resourceType, includeRawJson);
    Object[] row = new Object[layout.size()];
    for (int i = 0; i < layout.size(); i++) {
      row[i] = valueOf(layout.get(i));
    }
    return row;
  }

  /**
   * The value for one field. Strings are never null, so a provider that does not report a field
   * yields a blank cell rather than a null; timestamps are parsed to real Dates, and an unparseable
   * one becomes null with the original text left in {@code raw_json}.
   */
  private Object valueOf(Field field) {
    return switch (field) {
      case PROVIDER -> text(provider);
      case ENTITY_TYPE -> text(entityType);
      case REPO_OWNER -> text(repoOwner);
      case REPO_NAME -> text(repoName);
      case ID -> text(id);
      case NUMBER -> number;
      case TITLE -> text(title);
      case STATE -> text(state);
      case AUTHOR -> text(author);
      case AUTHOR_EMAIL -> text(authorEmail);
      case AUTHOR_LOGIN -> text(authorLogin);
      case COMMITTER -> text(committer);
      case COMMITTER_EMAIL -> text(committerEmail);
      case LABELS -> text(labels);
      case ASSIGNEES -> text(assignees);
      case CREATED_AT -> GitTimestamps.toDate(createdAt);
      case UPDATED_AT -> GitTimestamps.toDate(updatedAt);
      case CLOSED_AT -> GitTimestamps.toDate(closedAt);
      case MERGED_AT -> GitTimestamps.toDate(mergedAt);
      case URL -> text(url);
      case BODY -> text(body);
      case SHA -> text(sha);
      case SOURCE_BRANCH -> text(sourceBranch);
      case TARGET_BRANCH -> text(targetBranch);
      case MERGED -> merged;
      case IS_MERGE -> isMerge;
      case RAW_JSON -> text(rawJson);
    };
  }

  private static String text(String value) {
    return value == null ? "" : value;
  }
}
