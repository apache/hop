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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.pipeline.transforms.chunker.Chunk;

/**
 * Chunking strategy that splits text on paragraph boundaries.
 *
 * <p>Paragraphs are identified by blank lines. A paragraph that on its own exceeds {@code maxSize}
 * is split further with {@link CharacterChunkingStrategy}, because a chunk larger than the limit
 * would be rejected downstream by the embedding model rather than simply being large.
 *
 * <p>Consecutive very short paragraphs are grouped together while they fit within {@code maxSize}.
 */
public class ParagraphChunkingStrategy implements ChunkingStrategy {

  /**
   * A blank line. {@code \\R} matches any line ending, so positions stay valid in the original text
   * and no CRLF normalisation pass is needed.
   */
  private static final Pattern PARAGRAPH_SEPARATOR = Pattern.compile("\\R\\s*\\R");

  private static final int SHORT_PARAGRAPH_THRESHOLD = 10;

  private final CharacterChunkingStrategy fallback = new CharacterChunkingStrategy();

  @Override
  public List<Chunk> chunk(String text, int maxSize, int overlap) {
    List<Chunk> chunks = new ArrayList<>();

    if (text == null || text.isEmpty()) {
      return chunks;
    }
    if (maxSize <= 0) {
      chunks.add(new Chunk(text, 0, 0, text.length()));
      return chunks;
    }

    List<Paragraph> paragraphs = splitParagraphs(text);
    if (paragraphs.isEmpty()) {
      chunks.add(new Chunk(text, 0, 0, text.length()));
      return chunks;
    }

    StringBuilder current = null;
    int currentStart = 0;
    boolean shortGroup = false;
    int chunkIndex = 0;

    for (Paragraph paragraph : paragraphs) {
      boolean isShort = paragraph.length() < SHORT_PARAGRAPH_THRESHOLD;

      if (paragraph.length() > maxSize) {
        // Emit whatever is buffered, then split the oversized paragraph on characters.
        if (current != null) {
          chunks.add(toChunk(current, chunkIndex++, currentStart));
          current = null;
        }
        for (Chunk part : fallback.chunk(paragraph.text, maxSize, overlap)) {
          chunks.add(
              new Chunk(
                  part.getContent(),
                  chunkIndex++,
                  paragraph.start + part.getStartPosition(),
                  paragraph.start + part.getEndPosition()));
        }
        shortGroup = false;
        continue;
      }

      if (current == null) {
        current = new StringBuilder(paragraph.text);
        currentStart = paragraph.start;
        shortGroup = isShort;
      } else if (isShort && shortGroup && current.length() + 2 + paragraph.length() <= maxSize) {
        current.append("\n\n").append(paragraph.text);
      } else {
        chunks.add(toChunk(current, chunkIndex++, currentStart));
        current = new StringBuilder(paragraph.text);
        currentStart = paragraph.start;
        shortGroup = isShort;
      }
    }

    if (current != null && current.length() > 0) {
      chunks.add(toChunk(current, chunkIndex, currentStart));
    }

    return chunks;
  }

  private static Chunk toChunk(StringBuilder content, int index, int start) {
    String value = content.toString();
    return new Chunk(value, index, start, start + value.length());
  }

  /**
   * Splits on blank lines in a single pass, keeping each paragraph's offset in the original text.
   */
  private static List<Paragraph> splitParagraphs(String text) {
    List<Paragraph> paragraphs = new ArrayList<>();
    Matcher matcher = PARAGRAPH_SEPARATOR.matcher(text);
    int start = 0;
    while (matcher.find()) {
      addParagraph(paragraphs, text, start, matcher.start());
      start = matcher.end();
    }
    addParagraph(paragraphs, text, start, text.length());
    return paragraphs;
  }

  private static void addParagraph(List<Paragraph> paragraphs, String text, int start, int end) {
    if (end > start) {
      paragraphs.add(new Paragraph(text.substring(start, end), start));
    }
  }

  private record Paragraph(String text, int start) {
    int length() {
      return text.length();
    }
  }

  @Override
  public ChunkingStrategyType getType() {
    return ChunkingStrategyType.PARAGRAPH;
  }
}
