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
 * <p>Consecutive paragraphs are packed into one chunk while they fit within {@code maxSize}, which
 * is the usual contract for an embedding chunker: it keeps related text together and avoids
 * emitting many small chunks that each carry little context.
 */
public class ParagraphChunkingStrategy implements ChunkingStrategy {

  /**
   * A blank line. {@code \\R} matches any line ending, so positions stay valid in the original text
   * and no CRLF normalisation pass is needed.
   */
  private static final Pattern PARAGRAPH_SEPARATOR = Pattern.compile("\\R\\s*\\R");

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

    Paragraph first = null;
    Paragraph last = null;
    int chunkIndex = 0;

    for (Paragraph paragraph : paragraphs) {
      if (paragraph.length() > maxSize) {
        // Emit whatever is buffered, then split the oversized paragraph on characters.
        if (first != null) {
          chunks.add(slice(text, first, last, chunkIndex++));
          first = null;
          last = null;
        }
        for (Chunk part : fallback.chunk(paragraph.text, maxSize, overlap)) {
          chunks.add(
              new Chunk(
                  part.getContent(),
                  chunkIndex++,
                  paragraph.start + part.getStartPosition(),
                  paragraph.start + part.getEndPosition()));
        }
        continue;
      }

      if (first == null) {
        first = paragraph;
        last = paragraph;
      } else if (paragraph.end() - first.start() <= maxSize) {
        last = paragraph;
      } else {
        chunks.add(slice(text, first, last, chunkIndex++));
        first = paragraph;
        last = paragraph;
      }
    }

    if (first != null) {
      chunks.add(slice(text, first, last, chunkIndex));
    }

    return chunks;
  }

  /**
   * A packed chunk is the source text between the first and last paragraph it covers. Taking the
   * slice rather than rejoining with a fixed separator keeps the reported start and end positions
   * addressing the original text, whatever whitespace separated the paragraphs there.
   */
  private static Chunk slice(String text, Paragraph first, Paragraph last, int index) {
    return new Chunk(text.substring(first.start(), last.end()), index, first.start(), last.end());
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
    int end() {
      return start + text.length();
    }

    int length() {
      return text.length();
    }
  }

  @Override
  public ChunkingStrategyType getType() {
    return ChunkingStrategyType.PARAGRAPH;
  }
}
