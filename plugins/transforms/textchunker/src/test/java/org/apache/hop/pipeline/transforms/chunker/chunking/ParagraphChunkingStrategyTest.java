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

/** Unit tests for ParagraphChunkingStrategy. */
public class ParagraphChunkingStrategyTest {

  private ParagraphChunkingStrategy strategy;

  @BeforeEach
  public void setUp() {
    strategy = new ParagraphChunkingStrategy();
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
  public void testSingleParagraph() {
    String text = "This is a single paragraph with no breaks.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);
    assertEquals(1, chunks.size());
    assertEquals(text, chunks.get(0).getContent());
  }

  @Test
  public void testMultipleParagraphs() {
    String text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    // Should split on double newlines
    assertEquals(3, chunks.size());
    assertEquals("First paragraph.", chunks.get(0).getContent());
    assertEquals("Second paragraph.", chunks.get(1).getContent());
    assertEquals("Third paragraph.", chunks.get(2).getContent());
  }

  @Test
  public void testParagraphsAreKeptWholeWhenTheyFit() {
    // Within maxSize, a paragraph is never split.
    String text =
        "This is paragraph one.\n\nThis is paragraph two with more text that is quite long.";
    List<Chunk> chunks = strategy.chunk(text, 200, 2);

    assertEquals(2, chunks.size());
    assertEquals("This is paragraph one.", chunks.get(0).getContent());
    assertEquals(
        "This is paragraph two with more text that is quite long.", chunks.get(1).getContent());
  }

  @Test
  public void testOversizedParagraphIsSplitOnCharacters() {
    // A paragraph larger than maxSize has to be split: an embedding model would reject a chunk
    // over its token limit, so emitting it whole is not an option.
    String text =
        "This is paragraph one.\n\nThis is paragraph two with more text that is quite long.";
    List<Chunk> chunks = strategy.chunk(text, 10, 2);

    assertTrue(chunks.size() > 2, "oversized paragraphs should be split further");
    for (Chunk chunk : chunks) {
      assertTrue(
          chunk.getContent().length() <= 10, "chunk exceeds maxSize: '" + chunk.getContent() + "'");
    }
  }

  @Test
  public void testChunkPositionsPointIntoTheOriginalText() {
    String text = "First paragraph.\r\n\r\nSecond paragraph.";
    List<Chunk> chunks = strategy.chunk(text, 100, 0);

    for (Chunk chunk : chunks) {
      assertEquals(
          chunk.getContent(),
          text.substring(chunk.getStartPosition(), chunk.getEndPosition()),
          "start/end positions must address the chunk in the source text");
    }
  }

  @Test
  public void testWindowsLineEndings() {
    String text = "First paragraph.\r\n\r\nSecond paragraph.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(2, chunks.size());
    assertEquals("First paragraph.", chunks.get(0).getContent().replace("\r", ""));
    assertEquals("Second paragraph.", chunks.get(1).getContent().replace("\r", ""));
  }

  @Test
  public void testMixedLineEndings() {
    String text = "First paragraph.\n\r\nSecond paragraph.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(2, chunks.size());
  }

  @Test
  public void testSingleNewline() {
    // Single newline should not create a paragraph break
    String text = "Line one\nLine two";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(1, chunks.size());
    assertEquals(text, chunks.get(0).getContent());
  }

  @Test
  public void testEmptyParagraphs() {
    String text = "First\n\n\n\nSecond";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    // Should handle empty paragraphs gracefully
    assertTrue(chunks.size() >= 1);
    assertTrue(chunks.size() <= 2);
  }

  @Test
  public void testTrailingNewlines() {
    String text = "First paragraph.\n\nSecond paragraph.\n\n";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(2, chunks.size());
  }

  @Test
  public void testLeadingNewlines() {
    String text = "\n\nFirst paragraph.\n\nSecond paragraph.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(2, chunks.size());
  }

  @Test
  public void testGroupingBySize() {
    // Test that paragraphs are grouped when they fit within maxSize
    String text = "Short.\n\nShort.\n\nVery long paragraph that exceeds the chunk size limit.";
    List<Chunk> chunks = strategy.chunk(text, 50, 10);

    // First two paragraphs should be grouped together
    assertTrue(chunks.size() >= 2);

    // First chunk should contain both short paragraphs
    String firstChunk = chunks.get(0).getContent();
    assertTrue(firstChunk.contains("Short.") && firstChunk.contains("Short."));
  }

  @Test
  public void testAllTextPreserved() {
    String text = "Paragraph one.\n\nParagraph two.\n\nParagraph three.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    // Reconstruct the original text (without normalizing line endings)
    StringBuilder reconstructed = new StringBuilder();
    for (int i = 0; i < chunks.size(); i++) {
      if (i > 0) {
        reconstructed.append("\n\n");
      }
      reconstructed.append(chunks.get(i).getContent());
    }

    assertEquals(text, reconstructed.toString());
  }

  @Test
  public void testStrategyType() {
    assertEquals(ChunkingStrategyType.PARAGRAPH, strategy.getType());
  }

  @Test
  public void testTextWithTabsAndSpaces() {
    String text = "Paragraph one.\n\n\tParagraph two with tab.\n\n  Paragraph three with spaces.";
    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(3, chunks.size());
  }

  @Test
  public void testVeryLongParagraphs() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      if (i > 0) {
        sb.append("\n\n");
      }
      sb.append("This is paragraph number ").append(i).append(" with some text.");
    }
    String text = sb.toString();

    List<Chunk> chunks = strategy.chunk(text, 20, 5);

    // Should split on paragraph boundaries
    assertTrue(chunks.size() > 1);

    // Each chunk should contain complete paragraphs
    for (Chunk chunk : chunks) {
      assertFalse(chunk.getContent().contains("\n\n"));
    }
  }

  @Test
  public void testRealWorldExample() {
    String text =
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n\n"
            + "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n\n"
            + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.";

    List<Chunk> chunks = strategy.chunk(text, 100, 20);

    assertEquals(3, chunks.size());
    assertTrue(chunks.get(0).getContent().contains("Lorem ipsum"));
    assertTrue(chunks.get(1).getContent().contains("Sed do eiusmod"));
    assertTrue(chunks.get(2).getContent().contains("Ut enim ad minim"));
  }
}
