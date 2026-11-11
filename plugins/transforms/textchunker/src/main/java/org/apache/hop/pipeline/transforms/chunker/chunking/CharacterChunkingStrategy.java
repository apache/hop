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
 *
 * <p>The overlap carries whole words, at most {@code overlap} characters, from the end of one chunk
 * into the next. Only a word longer than the chunk size, which has to be cut anyway, is overlapped
 * on characters. Every chunk ends past the end of the previous one, chunks that hold only
 * whitespace are dropped, and a surrogate pair is never cut in half.
 */
public class CharacterChunkingStrategy implements ChunkingStrategy {

  @Override
  public List<Chunk> chunk(String text, int maxSize, int overlap) {
    List<Chunk> chunks = new ArrayList<>();

    if (text == null || text.isEmpty() || maxSize <= 0) {
      return chunks;
    }

    if (overlap < 0) {
      if (!text.isBlank()) {
        chunks.add(new Chunk(text, 0, 0, text.length()));
      }
      return chunks;
    }

    overlap = Math.min(overlap, maxSize - 1);

    int textLength = text.length();
    int start = 0;
    int previousEnd = 0;
    int chunkIndex = 0;

    while (start < textLength) {
      int chunkEnd = chunkEnd(text, start, maxSize);

      if (start < previousEnd && chunkEnd <= previousEnd) {
        // The overlap would only repeat text the previous chunk already holds: no new text would
        // reach this chunk. Continue where the previous chunk ended instead.
        start = previousEnd;
        continue;
      }

      String chunkContent = text.substring(start, chunkEnd);
      if (!chunkContent.isBlank()) {
        chunks.add(new Chunk(chunkContent, chunkIndex++, start, chunkEnd));
      }

      if (chunkEnd >= textLength) {
        break;
      }

      previousEnd = chunkEnd;
      start = nextStart(text, start, chunkEnd, overlap);
    }

    return chunks;
  }

  /**
   * The end of the chunk that starts at {@code start}: just after the last whitespace within {@code
   * maxSize} characters, or a hard cut when there is none, moved so it does not split a surrogate
   * pair.
   */
  private static int chunkEnd(String text, int start, int maxSize) {
    int textLength = text.length();
    int end = Math.min(start + maxSize, textLength);
    if (end == textLength) {
      return end;
    }

    for (int i = end - 1; i >= start; i--) {
      if (Character.isWhitespace(text.charAt(i))) {
        return i + 1;
      }
    }

    if (splitsSurrogatePair(text, end)) {
      // Keep the pair together: in this chunk if there is room for something before it,
      // otherwise in this chunk even though it then holds one character more than the maximum.
      return end - 1 > start ? end - 1 : end + 1;
    }
    return end;
  }

  /**
   * Where the chunk after {@code [start, chunkEnd)} starts: the first word that begins in the last
   * {@code overlap} characters of the chunk, or {@code chunkEnd} when no word begins there. When
   * the chunk was cut inside a word that is too long to back off from, the overlap stays on
   * characters, as no word boundary is available.
   */
  private static int nextStart(String text, int start, int chunkEnd, int overlap) {
    if (overlap <= 0) {
      return chunkEnd;
    }

    int candidate = Math.max(start + 1, chunkEnd - overlap);

    int wordStart = candidate;
    if (!Character.isWhitespace(text.charAt(wordStart - 1))) {
      // Inside a word: skip to its end.
      while (wordStart < chunkEnd && !Character.isWhitespace(text.charAt(wordStart))) {
        wordStart++;
      }
    }
    while (wordStart < chunkEnd && Character.isWhitespace(text.charAt(wordStart))) {
      wordStart++;
    }
    if (wordStart < chunkEnd) {
      return wordStart;
    }

    boolean cutInsideWord =
        chunkEnd < text.length()
            && !Character.isWhitespace(text.charAt(chunkEnd - 1))
            && !Character.isWhitespace(text.charAt(chunkEnd));
    if (!cutInsideWord) {
      return chunkEnd;
    }
    if (splitsSurrogatePair(text, candidate)) {
      return candidate - 1 > start ? candidate - 1 : candidate + 1;
    }
    return candidate;
  }

  /** True when {@code position} falls between the two halves of a surrogate pair. */
  private static boolean splitsSurrogatePair(String text, int position) {
    return position > 0
        && position < text.length()
        && Character.isHighSurrogate(text.charAt(position - 1))
        && Character.isLowSurrogate(text.charAt(position));
  }

  @Override
  public ChunkingStrategyType getType() {
    return ChunkingStrategyType.CHARACTER;
  }
}
