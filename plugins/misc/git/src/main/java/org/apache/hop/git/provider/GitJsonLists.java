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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Flattens the small JSON arrays that providers use for labels, assignees and commit parents into
 * the single output field the row carries.
 */
final class GitJsonLists {

  /** Separator between the entries of a flattened list field. */
  static final String SEPARATOR = ", ";

  private GitJsonLists() {}

  /**
   * Joins a list of labels or assignees into one comma-separated value.
   *
   * <p>Providers disagree on the shape: GitHub and Gitea return objects to read {@code nameKey}
   * from, GitLab returns plain strings, and a single-valued field such as a Bitbucket assignee is
   * an object rather than an array. All three are accepted, and anything else yields an empty value
   * rather than a failed row.
   */
  static String names(JSONObject json, String arrayKey, String nameKey) {
    if (json == null) {
      return "";
    }
    Object value = json.get(arrayKey);
    if (value instanceof JSONObject single) {
      return GitApiHttp.getString(single, nameKey);
    }
    if (!(value instanceof JSONArray array)) {
      return "";
    }
    List<String> names = new ArrayList<>(array.size());
    for (Object entry : array) {
      String name =
          switch (entry) {
            case JSONObject object -> GitApiHttp.getString(object, nameKey);
            case String plain -> plain;
            case null, default -> "";
          };
      if (!name.isBlank()) {
        names.add(name);
      }
    }
    return String.join(SEPARATOR, names);
  }

  /**
   * True when a commit has more than one parent. Providers report the parents as an array of
   * objects ({@code parents}) or of ids ({@code parent_ids}); an absent array means the payload
   * does not say, which is reported as false rather than as a merge.
   */
  static boolean mergeFlag(JSONObject json, String arrayKey) {
    if (json == null || !(json.get(arrayKey) instanceof JSONArray array)) {
      return false;
    }
    return array.size() > 1;
  }
}
