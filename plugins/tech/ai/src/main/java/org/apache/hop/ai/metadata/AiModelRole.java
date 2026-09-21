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
package org.apache.hop.ai.metadata;

/**
 * What a model on an {@link AiProvider} is used for. A transform resolves the role it needs rather
 * than asking the user to pick one, so a single provider can serve a chat transform, an embedding
 * transform and a reranker at the same time.
 *
 * <p>The constants deliberately do not override {@code toString()}. Generated dialogs fill an enum
 * combo with {@code toString()} and read it back with {@code Enum.valueOf}, which only accepts the
 * constant name.
 */
public enum AiModelRole {
  /** Conversation and completion. The role {@code modelName} falls back to. */
  CHAT,

  /** Turning text into an embedding vector. */
  EMBEDDING,

  /** Scoring a passage against a query, used by rerankers. */
  SCORING,

  /** Image generation. */
  IMAGE,

  /** Content moderation. */
  MODERATION;

  /**
   * Resolves a stored value to a role, tolerating case and unknown values.
   *
   * @param value the stored role name
   * @return the matching role, or {@link #CHAT} when the value is empty or unrecognised
   */
  public static AiModelRole fromString(String value) {
    if (value == null || value.isEmpty()) {
      return CHAT;
    }
    for (AiModelRole role : values()) {
      if (role.name().equalsIgnoreCase(value.trim())) {
        return role;
      }
    }
    return CHAT;
  }
}
