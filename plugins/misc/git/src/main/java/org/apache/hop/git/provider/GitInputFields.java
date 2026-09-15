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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.row.IValueMeta;

/**
 * The output row layout, which is chosen per {@link GitResourceType} rather than shared by all of
 * them.
 *
 * <p>A single row shared by every resource type would have to be the union of everything any type
 * can report, so a commit row would carry empty label and assignee columns and an issue row would
 * carry empty branch and merge columns. Each type therefore declares only the fields it can
 * actually fill.
 *
 * <p>Within a type the layout is still identical across providers, so a pipeline written against
 * GitHub keeps working when it is pointed at GitLab or Bitbucket. A field a given provider does not
 * report is an empty value, not a missing column.
 */
public final class GitInputFields {

  private GitInputFields() {}

  /** A single output field: its name in the row, its Hop type, and its display length. */
  @Getter
  public enum Field {
    PROVIDER("provider", IValueMeta.TYPE_STRING, 32),
    ENTITY_TYPE("entity_type", IValueMeta.TYPE_STRING, 32),
    REPO_OWNER("repo_owner", IValueMeta.TYPE_STRING, 128),
    REPO_NAME("repo_name", IValueMeta.TYPE_STRING, 128),
    ID("id", IValueMeta.TYPE_STRING, 64),
    NUMBER("number", IValueMeta.TYPE_INTEGER, -1),
    TITLE("title", IValueMeta.TYPE_STRING, 512),
    STATE("state", IValueMeta.TYPE_STRING, 32),
    AUTHOR("author", IValueMeta.TYPE_STRING, 128),
    AUTHOR_EMAIL("author_email", IValueMeta.TYPE_STRING, 256),
    AUTHOR_LOGIN("author_login", IValueMeta.TYPE_STRING, 128),
    COMMITTER("committer", IValueMeta.TYPE_STRING, 128),
    COMMITTER_EMAIL("committer_email", IValueMeta.TYPE_STRING, 256),
    LABELS("labels", IValueMeta.TYPE_STRING, 512),
    ASSIGNEES("assignees", IValueMeta.TYPE_STRING, 512),
    CREATED_AT("created_at", IValueMeta.TYPE_DATE, -1),
    UPDATED_AT("updated_at", IValueMeta.TYPE_DATE, -1),
    CLOSED_AT("closed_at", IValueMeta.TYPE_DATE, -1),
    MERGED_AT("merged_at", IValueMeta.TYPE_DATE, -1),
    URL("url", IValueMeta.TYPE_STRING, 512),
    BODY("body", IValueMeta.TYPE_STRING, -1),
    SHA("sha", IValueMeta.TYPE_STRING, 64),
    SOURCE_BRANCH("source_branch", IValueMeta.TYPE_STRING, 128),
    TARGET_BRANCH("target_branch", IValueMeta.TYPE_STRING, 128),
    MERGED("merged", IValueMeta.TYPE_BOOLEAN, -1),
    IS_MERGE("is_merge", IValueMeta.TYPE_BOOLEAN, -1),
    RAW_JSON("raw_json", IValueMeta.TYPE_STRING, -1);

    private final String fieldName;
    private final int type;
    private final int length;

    Field(String fieldName, int type, int length) {
      this.fieldName = fieldName;
      this.type = type;
      this.length = length;
    }
  }

  /**
   * The fields every type reports, in the order they lead each row. {@code id} is the provider
   * identifier and {@code url} the link back to the item.
   */
  private static final List<Field> IDENTITY =
      List.of(Field.PROVIDER, Field.ENTITY_TYPE, Field.REPO_OWNER, Field.REPO_NAME, Field.ID);

  private static final Map<GitResourceType, List<Field>> LAYOUTS =
      new EnumMap<>(GitResourceType.class);

  static {
    // A commit has two idents, author and committer, which differ on a rebase, a squash, a
    // cherry-pick or a merge made through a provider's web UI.
    LAYOUTS.put(
        GitResourceType.COMMITS,
        concat(
            Field.SHA,
            Field.TITLE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_EMAIL,
            Field.AUTHOR_LOGIN,
            Field.COMMITTER,
            Field.COMMITTER_EMAIL,
            Field.CREATED_AT,
            Field.IS_MERGE,
            Field.URL));

    // One row per changed file: title is the path, state the change type, body the previous path
    // of a rename or a copy.
    LAYOUTS.put(
        GitResourceType.COMMIT_FILES,
        concat(
            Field.SHA,
            Field.TITLE,
            Field.STATE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_EMAIL,
            Field.AUTHOR_LOGIN,
            Field.CREATED_AT,
            Field.URL));

    // Issues and pull requests identify people by account, so there is no e-mail to report.
    LAYOUTS.put(
        GitResourceType.ISSUES,
        concat(
            Field.NUMBER,
            Field.TITLE,
            Field.STATE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_LOGIN,
            Field.LABELS,
            Field.ASSIGNEES,
            Field.CREATED_AT,
            Field.UPDATED_AT,
            Field.CLOSED_AT,
            Field.URL));

    LAYOUTS.put(
        GitResourceType.PULL_REQUESTS,
        concat(
            Field.NUMBER,
            Field.TITLE,
            Field.STATE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_LOGIN,
            Field.LABELS,
            Field.ASSIGNEES,
            Field.SOURCE_BRANCH,
            Field.TARGET_BRANCH,
            Field.MERGED,
            Field.MERGED_AT,
            Field.CREATED_AT,
            Field.UPDATED_AT,
            Field.CLOSED_AT,
            Field.URL));

    // For a comment, number is the parent issue or pull request.
    List<Field> comment =
        concat(
            Field.NUMBER,
            Field.TITLE,
            Field.STATE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_LOGIN,
            Field.CREATED_AT,
            Field.UPDATED_AT,
            Field.URL);
    LAYOUTS.put(GitResourceType.ISSUE_COMMENTS, comment);
    LAYOUTS.put(GitResourceType.PR_COMMENTS, comment);

    // For an event, title is the event type and state its detail.
    LAYOUTS.put(
        GitResourceType.ISSUE_EVENTS,
        concat(
            Field.NUMBER,
            Field.TITLE,
            Field.STATE,
            Field.BODY,
            Field.AUTHOR,
            Field.AUTHOR_LOGIN,
            Field.CREATED_AT,
            Field.URL));
  }

  private static List<Field> concat(Field... typeFields) {
    return java.util.stream.Stream.concat(IDENTITY.stream(), java.util.stream.Stream.of(typeFields))
        .toList();
  }

  /**
   * The output fields for a resource type, in row order. {@code raw_json} is appended last when it
   * is requested, so dropping it never shifts the fields in front of it.
   */
  public static List<Field> layout(GitResourceType resourceType, boolean includeRawJson) {
    List<Field> fields = LAYOUTS.get(resourceType);
    if (fields == null) {
      throw new IllegalArgumentException("No output layout for resource type " + resourceType);
    }
    if (!includeRawJson) {
      return fields;
    }
    return java.util.stream.Stream.concat(
            fields.stream(), java.util.stream.Stream.of(Field.RAW_JSON))
        .toList();
  }

  /** Number of output fields for a resource type. */
  public static int fieldCount(GitResourceType resourceType, boolean includeRawJson) {
    return layout(resourceType, includeRawJson).size();
  }

  /** Field names for a resource type, for the dialog and for tests. */
  public static String[] fieldNames(GitResourceType resourceType, boolean includeRawJson) {
    return layout(resourceType, includeRawJson).stream()
        .map(Field::getFieldName)
        .toArray(String[]::new);
  }
}
