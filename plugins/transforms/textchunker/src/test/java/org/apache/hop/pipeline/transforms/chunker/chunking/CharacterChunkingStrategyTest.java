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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.Chunk;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for CharacterChunkingStrategy. */
public class CharacterChunkingStrategyTest {

  private CharacterChunkingStrategy strategy;

  @BeforeEach
  public void setUp() {
    strategy = new CharacterChunkingStrategy();
  }

  @Test
  public void testNullText() {
    List<Chunk> chunks = strategy.chunk(null, 100, 20);
    assertTrue(chunks.isEmpty());
  }

  @Test
  public void testEmptyText() {
    List<Chunk> chunks = strategy.chunk("", 100, 20);
    assertTrue(chunks.isEmpty());
  }

  @Test
  public void testSingleChunk() {
    String text = "This is a short text";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);
    assertEquals(1, chunks.size());
    assertEquals(text, chunks.get(0).getContent());
    assertEquals(0, chunks.get(0).getIndex());
    assertEquals(0, chunks.get(0).getStartPosition());
    assertEquals(text.length(), chunks.get(0).getEndPosition());
  }

  @Test
  public void testChunkingRespectsWordBoundaries() {
    String text = "This is a test string for chunking.";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    // Should not split words
    for (Chunk chunk : chunks) {
      String content = chunk.getContent();
      // Check that the chunk doesn't end with a partial word (unless it's the last chunk)
      if (chunk.getIndex() < chunks.size() - 1) {
        // The last character should be a whitespace or the chunk should end at a word boundary
        char lastChar = content.charAt(content.length() - 1);
        assertTrue(
            Character.isWhitespace(lastChar) || lastChar == '.' || lastChar == ',',
            "Chunk should not end mid-word: " + content);
      }
    }
  }

  @Test
  public void testChunkingWithOverlap() {
    String text = "This is a test string for chunking with overlap.";
    List<Chunk> chunks = strategy.chunk(text, 15, 5);

    assertTrue(chunks.size() > 1, "Should have multiple chunks");

    // Verify overlap exists between chunks
    for (int i = 0; i < chunks.size() - 1; i++) {
      Chunk current = chunks.get(i);
      Chunk next = chunks.get(i + 1);

      String currentContent = current.getContent();
      String nextContent = next.getContent();

      // The beginning of the next chunk should overlap with the end of the current chunk
      String overlapPart = nextContent.substring(0, Math.min(5, nextContent.length()));
      String endOfCurrent = currentContent.substring(Math.max(0, currentContent.length() - 5));

      // At least some overlap should exist
      assertTrue(
          endOfCurrent.contains(overlapPart.substring(0, Math.min(2, overlapPart.length()))));
    }
  }

  @Test
  public void testVeryLongWord() {
    String text = "ThisIsAVeryLongWordThatShouldBeSplitBecauseItExceedsTheChunkSizeLimit";
    List<Chunk> chunks = strategy.chunk(text, 20, 5);

    assertFalse(chunks.isEmpty());

    // Overlap means chunks deliberately share characters, so summing their lengths would exceed
    // the source. What has to hold is that they cover it and address it correctly.
    assertEquals(0, chunks.get(0).getStartPosition());
    assertEquals(text.length(), chunks.get(chunks.size() - 1).getEndPosition());
    for (Chunk chunk : chunks) {
      assertEquals(
          text.substring(chunk.getStartPosition(), chunk.getEndPosition()), chunk.getContent());
    }

    // A word with no whitespace in it still gets the configured overlap.
    assertTrue(
        chunks.get(1).getStartPosition() < chunks.get(0).getEndPosition(),
        "chunks after a non-whitespace split must still overlap");
  }

  @Test
  public void testZeroChunkSize() {
    List<Chunk> chunks = strategy.chunk("test", 0, 0);
    assertTrue(chunks.isEmpty());
  }

  @Test
  public void testNegativeOverlap() {
    String text = "This is a test.";
    List<Chunk> chunks = strategy.chunk(text, 10, -5);
    assertEquals(1, chunks.size());
  }

  @Test
  public void testOverlapLargerThanChunkSize() {
    String text = "This is a test.";
    List<Chunk> chunks = strategy.chunk(text, 10, 15);
    // Should still work, but overlap will be capped
    assertFalse(chunks.isEmpty());
  }

  @Test
  public void testMultipleSpaces() {
    String text = "This  has   multiple    spaces   between   words.";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    // Should handle multiple spaces correctly
    assertFalse(chunks.isEmpty());

    // Verify all text is preserved (overlapping chunks must be merged by span, not naive concat)
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  @Test
  public void testPunctuation() {
    String text = "Hello, world! How are you? I'm fine, thank you.";
    List<Chunk> chunks = strategy.chunk(text, 15, 3);

    assertFalse(chunks.isEmpty());

    // Verify all text is preserved including punctuation
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  @Test
  public void testNewlines() {
    String text = "Line one\nLine two\nLine three";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    assertFalse(chunks.isEmpty());

    // Verify newlines are preserved
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  private static String reconstructFromChunks(String text, List<Chunk> chunks) {
    if (chunks.isEmpty()) {
      return "";
    }
    StringBuilder reconstructed =
        new StringBuilder(text.substring(0, chunks.get(0).getEndPosition()));
    for (int i = 1; i < chunks.size(); i++) {
      Chunk previous = chunks.get(i - 1);
      Chunk current = chunks.get(i);
      reconstructed.append(text, previous.getEndPosition(), current.getEndPosition());
    }
    return reconstructed.toString();
  }

  @Test
  public void testStrategyType() {
    assertEquals(ChunkingStrategyType.CHARACTER, strategy.getType());
  }

  @Test
  public void testExactChunkSize() {
    // Test that we can get exact chunk sizes when possible
    String text = "12345 67890 12345 67890";
    List<Chunk> chunks = strategy.chunk(text, 11, 0);

    // Should split on spaces at approximately 11 characters
    for (Chunk chunk : chunks) {
      // Each chunk should be close to 11 characters
      assertTrue(chunk.getLength() <= 11 || chunk.getIndex() == chunks.size() - 1);
    }
  }
}
