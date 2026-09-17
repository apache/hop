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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.Chunk;

/**
 * Chunking strategy that splits text on fixed character count while respecting word boundaries.
 * This ensures that chunks don't split words in the middle.
 */
public class CharacterChunkingStrategy implements ChunkingStrategy {

  /** Characters that are considered word separators. */
  @Override
  public List<Chunk> chunk(String text, int maxSize, int overlap) {
    List<Chunk> chunks = new ArrayList<>();

    if (text == null || text.isEmpty() || maxSize <= 0) {
      return chunks;
    }

    if (overlap < 0) {
      chunks.add(new Chunk(text, 0, 0, text.length()));
      return chunks;
    }

    overlap = Math.min(overlap, maxSize - 1);

    int start = 0;
    int chunkIndex = 0;
    int textLength = text.length();

    while (start < textLength) {
      int end = Math.min(start + maxSize, textLength);

      if (end == textLength) {
        String chunkContent = text.substring(start, end);
        chunks.add(new Chunk(chunkContent, chunkIndex++, start, end));
        break;
      }

      int separatorPos = -1;
      for (int i = end - 1; i >= start; i--) {
        if (Character.isWhitespace(text.charAt(i))) {
          separatorPos = i;
          break;
        }
      }

      int chunkEnd = separatorPos >= start ? separatorPos + 1 : end;

      if (chunkEnd <= start) {
        chunkEnd = Math.min(start + maxSize, textLength);
      }

      String chunkContent = text.substring(start, chunkEnd);
      chunks.add(new Chunk(chunkContent, chunkIndex++, start, chunkEnd));

      if (chunkEnd >= textLength) {
        break;
      }

      start = overlap > 0 ? Math.max(start + 1, chunkEnd - overlap) : chunkEnd;
    }

    return chunks;
  }

  @Override
  public ChunkingStrategyType getType() {
    return ChunkingStrategyType.CHARACTER;
  }
}
