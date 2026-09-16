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
