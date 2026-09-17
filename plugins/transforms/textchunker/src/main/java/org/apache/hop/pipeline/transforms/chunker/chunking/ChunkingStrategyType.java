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
package org.apache.hop.pipeline.transforms.chunker.chunking;

/** Enumeration of available chunking strategy types. */
public enum ChunkingStrategyType {
  /** Split text on fixed character count, respecting word boundaries. */
  CHARACTER("Character"),

  /** Split text on paragraph boundaries (double newlines). */
  PARAGRAPH("Paragraph"),

  /**
   * Split on document structure (headings) with breadcrumb prefixes; character fallback per
   * section.
   */
  STRUCTURE("Structure");

  /**
   * Display text kept only so {@link #fromString(String)} still accepts values written before the
   * constant names were used. It is deliberately NOT exposed through {@code toString()}: the
   * generated dialogs fill an enum combo with {@code toString()} and read it back with {@code
   * Enum.valueOf}, so anything other than the constant name makes the combo unreadable and a
   * changed selection is silently dropped.
   */
  private final String description;

  ChunkingStrategyType(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Gets the strategy type from its string representation.
   *
   * @param type The string type
   * @return The corresponding strategy type
   */
  public static ChunkingStrategyType fromString(String type) {
    if (type == null || type.isEmpty()) {
      return CHARACTER;
    }
    for (ChunkingStrategyType strategyType : values()) {
      if (strategyType.name().equalsIgnoreCase(type)
          || strategyType.getDescription().equalsIgnoreCase(type)) {
        return strategyType;
      }
    }
    return CHARACTER;
  }

  /**
   * Gets all available strategy types as an array.
   *
   * @return Array of all strategy types
   */
  public static ChunkingStrategyType[] getAll() {
    return values();
  }
}
