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
package org.apache.hop.ai.transforms.embedtext;

/**
 * How the embedding is put on the output row.
 *
 * <p>The constants deliberately do not override {@code toString()}. Generated dialogs fill an enum
 * combo with {@code toString()} and read it back with {@code Enum.valueOf}, which only accepts the
 * constant name.
 */
public enum EmbedTextOutputFormat {
  /** A String holding a JSON array, readable anywhere and accepted by pgvector upsert. */
  STRING,

  /** A field of the Vector value type, which avoids rendering the numbers to text and back. */
  VECTOR;

  public static EmbedTextOutputFormat fromString(String value) {
    if (value == null || value.isEmpty()) {
      return STRING;
    }
    for (EmbedTextOutputFormat format : values()) {
      if (format.name().equalsIgnoreCase(value.trim())) {
        return format;
      }
    }
    return STRING;
  }
}
