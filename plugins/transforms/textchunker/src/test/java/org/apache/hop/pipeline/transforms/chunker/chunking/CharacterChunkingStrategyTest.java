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

import java.util.Collections;
import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.Chunk;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for CharacterChunkingStrategy. */
public class CharacterChunkingStrategyTest {

  private CharacterChunkingStrategy strategy;

  @BeforeEach
  void setUp() {
    strategy = new CharacterChunkingStrategy();
  }

  @Test
  void testNullText() {
    List<Chunk> chunks = strategy.chunk(null, 100, 20);
    assertTrue(chunks.isEmpty());
  }

  @Test
  void testEmptyText() {
    List<Chunk> chunks = strategy.chunk("", 100, 20);
    assertTrue(chunks.isEmpty());
  }

  @Test
  void testSingleChunk() {
    String text = "This is a short text";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);
    assertEquals(1, chunks.size());
    assertEquals(text, chunks.get(0).getContent());
    assertEquals(0, chunks.get(0).getIndex());
    assertEquals(0, chunks.get(0).getStartPosition());
    assertEquals(text.length(), chunks.get(0).getEndPosition());
  }

  @Test
  void testChunkingRespectsWordBoundaries() {
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
  void testChunkingWithOverlap() {
    String text = "This is a test string for chunking with overlap.";
    List<Chunk> chunks = strategy.chunk(text, 15, 5);

    // "test " is the only whole word within the last 5 characters of the first chunk. No word
    // starts
    // within the last 5 characters of the second ("ring ") or third ("king ") one, so nothing is
    // carried there.
    assertEquals(
        List.of("This is a test ", "test string ", "for chunking ", "with overlap."),
        contents(chunks));
    assertAddressesTheSource(text, chunks);
  }

  /** #9283: the overlap must start at a word, not in the middle of one. */
  @Test
  void testOverlapStartsAtAWordBoundary() {
    List<Chunk> chunks = strategy.chunk(LOREM, 100, 20);

    assertTrue(chunks.size() > 3);
    assertAddressesTheSource(LOREM, chunks);
    for (int i = 1; i < chunks.size(); i++) {
      Chunk previous = chunks.get(i - 1);
      Chunk current = chunks.get(i);
      int start = current.getStartPosition();
      assertTrue(
          Character.isWhitespace(LOREM.charAt(start - 1)),
          "chunk " + i + " starts in the middle of a word: " + current.getContent());
      assertTrue(
          previous.getEndPosition() - start <= 20,
          "chunk " + i + " overlaps more than the configured 20 characters");
    }
    // The overlap window of the first chunk starts inside "incididunt", which used to be carried
    // over as "ncididunt". Now the next whole word is.
    assertTrue(chunks.get(1).getContent().startsWith("ut labore "), chunks.get(1).getContent());
  }

  /**
   * #9283: when a chunk backs off to a space and ends up shorter than the overlap, the next chunk
   * used to move forward by one character: 228 chunks for 419 characters.
   */
  @Test
  void testOverlapDoesNotProduceAChunkPerCharacter() {
    String text = String.join(" ", Collections.nCopies(20, "supercalifragilistic"));
    List<Chunk> chunks = strategy.chunk(text, 25, 15);

    assertEquals(20, chunks.size());
    assertAddressesTheSource(text, chunks);
    for (int i = 1; i < chunks.size(); i++) {
      assertTrue(chunks.get(i).getEndPosition() > chunks.get(i - 1).getEndPosition());
      assertTrue(chunks.get(i).getContent().startsWith("supercalifragilistic"));
    }
  }

  /** Every chunk has to add text the previous one did not hold. */
  @Test
  void testNoChunkIsContainedInThePreviousOne() {
    String text = "aaaa bbbb cccc dddd eeee " + "x".repeat(30) + " ffff gggg";
    List<Chunk> chunks = strategy.chunk(text, 25, 10);

    assertAddressesTheSource(text, chunks);
    for (int i = 1; i < chunks.size(); i++) {
      assertTrue(
          chunks.get(i).getEndPosition() > chunks.get(i - 1).getEndPosition(),
          "chunk " + i + " only repeats the previous chunk: " + chunks.get(i).getContent());
    }
  }

  /** #9283: a run of whitespace longer than the chunk size gave chunks of only spaces. */
  @Test
  void testWhitespaceOnlyChunksAreDropped() {
    String text = "a" + " ".repeat(30) + "b";
    List<Chunk> chunks = strategy.chunk(text, 10, 0);

    assertAddressesTheSource(text, chunks);
    for (int i = 0; i < chunks.size(); i++) {
      assertFalse(chunks.get(i).getContent().isBlank(), "chunk " + i + " is whitespace only");
      assertEquals(i, chunks.get(i).getIndex());
    }
    assertEquals(2, chunks.size());
    assertTrue(chunks.get(0).getContent().startsWith("a"));
    assertTrue(chunks.get(1).getContent().endsWith("b"));
  }

  @Test
  void testWhitespaceOnlyTextYieldsNoChunks() {
    assertTrue(strategy.chunk(" \n\t ", 10, 2).isEmpty());
    assertTrue(strategy.chunk(" \n\t ", 10, -1).isEmpty());
  }

  /** #9283: the split was on char positions, so an emoji was cut into two lone surrogates. */
  @Test
  void testSurrogatePairIsNotCut() {
    String text = "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02";
    List<Chunk> chunks = strategy.chunk(text, 5, 0);

    assertEquals(List.of("\uD83D\uDE00\uD83D\uDE01", "\uD83D\uDE02"), contents(chunks));
    assertAddressesTheSource(text, chunks);
  }

  @Test
  void testSurrogatePairIsNotCutWhenItIsTheOnlyCharacter() {
    String text = "\uD83D\uDE00\uD83D\uDE01";
    List<Chunk> chunks = strategy.chunk(text, 1, 0);

    assertEquals(List.of("\uD83D\uDE00", "\uD83D\uDE01"), contents(chunks));
  }

  @Test
  void testOverlapInsideALongWordDoesNotCutASurrogatePair() {
    String text = "\uD83D\uDE00".repeat(20);
    List<Chunk> chunks = strategy.chunk(text, 9, 3);

    assertAddressesTheSource(text, chunks);
    for (Chunk chunk : chunks) {
      assertFalse(Character.isLowSurrogate(chunk.getContent().charAt(0)), chunk.toString());
      assertFalse(
          Character.isHighSurrogate(chunk.getContent().charAt(chunk.getLength() - 1)),
          chunk.toString());
    }
  }

  @Test
  void testVeryLongWord() {
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
  void testZeroChunkSize() {
    List<Chunk> chunks = strategy.chunk("test", 0, 0);
    assertTrue(chunks.isEmpty());
  }

  @Test
  void testNegativeOverlap() {
    String text = "This is a test.";
    List<Chunk> chunks = strategy.chunk(text, 10, -5);
    assertEquals(1, chunks.size());
  }

  @Test
  void testOverlapLargerThanChunkSize() {
    String text = "This is a test.";
    List<Chunk> chunks = strategy.chunk(text, 10, 15);
    // Should still work, but overlap will be capped
    assertFalse(chunks.isEmpty());
  }

  @Test
  void testMultipleSpaces() {
    String text = "This  has   multiple    spaces   between   words.";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    // Should handle multiple spaces correctly
    assertFalse(chunks.isEmpty());

    // Verify all text is preserved (overlapping chunks must be merged by span, not naive concat)
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  @Test
  void testPunctuation() {
    String text = "Hello, world! How are you? I'm fine, thank you.";
    List<Chunk> chunks = strategy.chunk(text, 15, 3);

    assertFalse(chunks.isEmpty());

    // Verify all text is preserved including punctuation
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  @Test
  void testNewlines() {
    String text = "Line one\nLine two\nLine three";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    assertFalse(chunks.isEmpty());

    // Verify newlines are preserved
    assertEquals(text, reconstructFromChunks(text, chunks));
  }

  private static final String LOREM =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt"
          + " ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation"
          + " ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in"
          + " reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.";

  private static List<String> contents(List<Chunk> chunks) {
    return chunks.stream().map(Chunk::getContent).toList();
  }

  /** Each chunk is the slice of the source text its positions name, and the chunks cover it. */
  private static void assertAddressesTheSource(String text, List<Chunk> chunks) {
    for (Chunk chunk : chunks) {
      assertEquals(
          text.substring(chunk.getStartPosition(), chunk.getEndPosition()), chunk.getContent());
    }
    assertEquals(text.strip(), reconstructFromChunks(text, chunks).strip());
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
  void testStrategyType() {
    assertEquals(ChunkingStrategyType.CHARACTER, strategy.getType());
  }

  @Test
  void testExactChunkSize() {
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
