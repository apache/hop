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
package org.apache.hop.pipeline.transforms.chunker;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Represents a text chunk with metadata for tracking. */
@Getter
@AllArgsConstructor
public class Chunk {

  /** The text content of this chunk. */
  private final String content;

  /** The index of this chunk in the sequence (0-based). */
  private final int index;

  /** The starting position of this chunk in the original text. */
  private final int startPosition;

  /** The ending position (exclusive) of this chunk in the original text. */
  private final int endPosition;

  /**
   * Gets the length of the chunk content in characters.
   *
   * @return The character count
   */
  public int getLength() {
    return content != null ? content.length() : 0;
  }

  @Override
  public String toString() {
    return "Chunk{"
        + "index="
        + index
        + ", start="
        + startPosition
        + ", end="
        + endPosition
        + ", length="
        + getLength()
        + ", content='"
        + (content.length() > 50 ? content.substring(0, 50) + "..." : content)
        + '\''
        + '}';
  }
}
