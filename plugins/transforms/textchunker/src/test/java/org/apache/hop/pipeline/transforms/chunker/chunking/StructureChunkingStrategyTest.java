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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.Chunk;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StructureChunkingStrategyTest {

  private StructureChunkingStrategy strategy;

  @BeforeEach
  void setUp() {
    strategy = new StructureChunkingStrategy();
  }

  @Test
  void splitsAsciiDocByHeadingsWithBreadcrumb() {
    String text =
        "= Working with git\n\n"
            + "Version control is important.\n\n"
            + "== File Explorer Toolbar\n\n"
            + "If git was found, buttons are enabled.\n\n"
            + "=== Git Configuration options\n\n"
            + "Global configuration options are available.";

    List<Chunk> chunks = strategy.chunk(text, 500, 50, ContentType.ASCIIDOC);

    assertEquals(3, chunks.size());
    assertTrue(chunks.get(0).getContent().contains("Working with git"));
    assertTrue(chunks.get(1).getContent().contains("File Explorer Toolbar"));
    assertTrue(chunks.get(1).getContent().contains("buttons are enabled"));
    assertTrue(chunks.get(2).getContent().contains("Git Configuration options"));
  }

  @Test
  void fallsBackToCharacterSplitForLargeSection() {
    StringBuilder body = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      body.append("word").append(i).append(' ');
    }
    String text = "= Section\n\n" + body;

    List<Chunk> chunks = strategy.chunk(text.toString(), 120, 20, ContentType.ASCIIDOC);

    assertTrue(chunks.size() > 1);
    assertTrue(chunks.get(0).getContent().contains("Section"));
    assertTrue(chunks.stream().allMatch(c -> c.getContent().contains("Section")));
  }

  @Test
  void plainContentTypeKeepsSingleSection() {
    String text = "Title line\n\nBody paragraph one.\n\nBody paragraph two.";
    List<Chunk> chunks = strategy.chunk(text, 500, 0, ContentType.PLAIN);
    assertEquals(1, chunks.size());
    assertEquals(text, chunks.get(0).getContent());
  }

  @Test
  void whitespaceOnlyTextYieldsNoChunks() {
    assertTrue(strategy.chunk("   \n\n\t  \n", 5, 0, ContentType.PLAIN).isEmpty());
    assertTrue(strategy.chunk("   \n\n\t  \n", 5, 0, ContentType.MARKDOWN).isEmpty());
  }

  @Test
  void dropsWhitespaceOnlyPartsOfLargeSectionAndRenumbers() {
    // A preamble without heading has no breadcrumb, so its character-split parts are emitted as-is.
    String text = "alpha" + " ".repeat(30) + "beta";

    List<Chunk> chunks = strategy.chunk(text, 10, 0, ContentType.PLAIN);

    assertTrue(chunks.stream().noneMatch(c -> c.getContent().isBlank()));
    for (int i = 0; i < chunks.size(); i++) {
      assertEquals(i, chunks.get(i).getIndex());
    }
    assertTrue(chunks.get(0).getContent().contains("alpha"));
    assertTrue(chunks.get(chunks.size() - 1).getContent().contains("beta"));
  }

  /**
   * #9283: the chunks a long section is split into reported the heading position plus their offset
   * in the stripped body, so every chunk after the first was off by the length of the heading.
   */
  @Test
  void positionsOfASplitSectionPointAtTheirTextInTheSource() {
    StringBuilder body = new StringBuilder();
    for (int i = 1; i <= 40; i++) {
      body.append(String.format("word%03d ", i));
    }
    String text = "# Title\n\n" + body.toString().strip();

    List<Chunk> chunks = strategy.chunk(text, 100, 0, ContentType.MARKDOWN);

    assertTrue(chunks.size() > 2);
    assertEquals(0, chunks.get(0).getStartPosition(), "the first chunk starts at the heading");
    for (int i = 1; i < chunks.size(); i++) {
      Chunk chunk = chunks.get(i);
      String partText = chunk.getContent().substring("Title\n\n".length());
      assertEquals(
          partText,
          text.substring(chunk.getStartPosition(), chunk.getEndPosition()),
          "chunk " + i + " does not address its text in the source");
    }
    assertEquals(text.indexOf("word012"), chunks.get(1).getStartPosition());
  }

  @Test
  void positionsOfASplitSectionSkipBlankLinesAfterTheHeading() {
    String text = "## A\n\n\n   " + "alpha beta gamma delta ".repeat(10).strip();

    List<Chunk> chunks = strategy.chunk(text, 40, 0, ContentType.MARKDOWN);

    assertTrue(chunks.size() > 2);
    for (int i = 1; i < chunks.size(); i++) {
      Chunk chunk = chunks.get(i);
      assertEquals(
          chunk.getContent().substring("A\n\n".length()),
          text.substring(chunk.getStartPosition(), chunk.getEndPosition()));
    }
  }

  /** An unsplit section keeps starting at its heading. */
  @Test
  void anUnsplitSectionStartsAtItsHeading() {
    String text = "# Title\n\nIntro line.\n\n## Section A\n\nBody A.";

    List<Chunk> chunks = strategy.chunk(text, 200, 0, ContentType.MARKDOWN);

    assertEquals(2, chunks.size());
    assertEquals(0, chunks.get(0).getStartPosition());
    assertEquals(text.indexOf("## Section A"), chunks.get(1).getStartPosition());
    assertEquals(text.length(), chunks.get(1).getEndPosition());
  }

  /** The overlap inside a split section starts at a word, the same as for Character. */
  @Test
  void overlapInsideASplitSectionStartsAtAWord() {
    StringBuilder body = new StringBuilder();
    for (int i = 1; i <= 40; i++) {
      body.append(String.format("word%03d ", i));
    }
    String text = "# Title\n\n" + body.toString().strip();

    List<Chunk> chunks = strategy.chunk(text, 100, 20, ContentType.MARKDOWN);

    assertTrue(chunks.size() > 2);
    for (int i = 1; i < chunks.size(); i++) {
      String partText = chunks.get(i).getContent().substring("Title\n\n".length());
      assertTrue(partText.startsWith("word"), "chunk " + i + " starts mid-word: " + partText);
      assertTrue(
          chunks.get(i).getStartPosition() < chunks.get(i - 1).getEndPosition(),
          "chunk " + i + " does not overlap the previous one");
    }
  }

  @Test
  void strategyTypeIsStructure() {
    assertEquals(ChunkingStrategyType.STRUCTURE, strategy.getType());
  }

  @Test
  void chunksHopPipelineXmlByTransform() {
    String xml =
        """
                <pipeline>
                  <info><name>demo</name></info>
                  <order><hop><from>A</from><to>B</to><enabled>Y</enabled></hop></order>
                  <transform><name>A</name><type>RowGenerator</type></transform>
                  <transform><name>B</name><type>FilterRows</type><compare/></transform>
                </pipeline>
                """;

    List<Chunk> chunks = strategy.chunk(xml, 800, 0, ContentType.PIPELINE);

    assertTrue(chunks.size() >= 3);
    assertTrue(chunks.stream().anyMatch(c -> c.getContent().contains("FilterRows")));
    assertTrue(chunks.stream().anyMatch(c -> c.getContent().contains("Data flow")));
  }
}
