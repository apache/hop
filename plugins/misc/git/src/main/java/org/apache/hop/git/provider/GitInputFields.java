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

import org.apache.hop.core.row.IValueMeta;

/** Describes the normalized output row layout shared by every provider. */
public final class GitInputFields {

  private GitInputFields() {}

  public static final String[] FIELD_NAMES = {
    "provider",
    "entity_type",
    "repo_owner",
    "repo_name",
    "id",
    "number",
    "title",
    "state",
    "author",
    "created_at",
    "updated_at",
    "closed_at",
    "url",
    "body",
    "sha",
    "source_branch",
    "target_branch",
    "merged",
    "raw_json"
  };

  public static final int[] FIELD_TYPES = {
    IValueMeta.TYPE_STRING, // provider
    IValueMeta.TYPE_STRING, // entity_type
    IValueMeta.TYPE_STRING, // repo_owner
    IValueMeta.TYPE_STRING, // repo_name
    IValueMeta.TYPE_STRING, // id
    IValueMeta.TYPE_INTEGER, // number
    IValueMeta.TYPE_STRING, // title
    IValueMeta.TYPE_STRING, // state
    IValueMeta.TYPE_STRING, // author
    IValueMeta.TYPE_DATE, // created_at
    IValueMeta.TYPE_DATE, // updated_at
    IValueMeta.TYPE_DATE, // closed_at
    IValueMeta.TYPE_STRING, // url
    IValueMeta.TYPE_STRING, // body
    IValueMeta.TYPE_STRING, // sha
    IValueMeta.TYPE_STRING, // source_branch
    IValueMeta.TYPE_STRING, // target_branch
    IValueMeta.TYPE_STRING, // merged
    IValueMeta.TYPE_STRING // raw_json
  };

  /** Display length per field; {@code -1} leaves the length unset. */
  public static final int[] FIELD_LENGTHS = {
    32, 32, 128, 128, 64, -1, 512, 32, 128, -1, -1, -1, 512, -1, 64, 128, 128, 8, -1
  };

  /** Index of {@code raw_json} in {@link #FIELD_NAMES}. */
  public static final int RAW_JSON_FIELD_INDEX = FIELD_NAMES.length - 1;

  /** Number of output fields, which depends on whether {@code raw_json} is included. */
  public static int fieldCount(boolean includeRawJson) {
    return includeRawJson ? FIELD_NAMES.length : FIELD_NAMES.length - 1;
  }
}
