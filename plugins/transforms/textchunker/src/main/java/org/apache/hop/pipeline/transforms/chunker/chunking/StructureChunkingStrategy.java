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
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentParser;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentParserRegistry;

/**
 * Structure-aware chunking: parse document into sections, prefix each chunk with a breadcrumb path,
 * and fall back to {@link CharacterChunkingStrategy} when a section exceeds the size limit.
 */
public class StructureChunkingStrategy implements ChunkingStrategy {

  private final CharacterChunkingStrategy fallback = new CharacterChunkingStrategy();
  private ContentType contentType = ContentType.AUTO;

  public void setContentType(ContentType contentType) {
    this.contentType = contentType != null ? contentType : ContentType.AUTO;
  }

  @Override
  public List<Chunk> chunk(String text, int maxSize, int overlap) {
    return chunk(text, maxSize, overlap, contentType);
  }

  public List<Chunk> chunk(String text, int maxSize, int overlap, ContentType type) {
    if (text == null || text.isEmpty() || maxSize <= 0) {
      return List.of();
    }

    DocumentParser parser = DocumentParserRegistry.parserFor(type, text);
    DocumentNode root = parser.parse(text);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    if (sections.isEmpty()) {
      return fallback.chunk(text, maxSize, overlap);
    }

    List<Chunk> chunks = new ArrayList<>();
    int chunkIndex = 0;

    for (DocumentNode.DocumentSection section : sections) {
      String sectionText = formatSection(section);
      if (sectionText.isEmpty()) {
        continue;
      }

      if (sectionText.length() <= maxSize) {
        chunks.add(
            new Chunk(
                sectionText,
                chunkIndex++,
                section.getStartPosition(),
                section.getStartPosition() + sectionText.length()));
      } else {
        String breadcrumb = section.breadcrumb();
        String prefix = breadcrumb.isEmpty() ? "" : breadcrumb + "\n\n";
        String body = section.getBody();
        int bodyMax = prefix.isEmpty() ? maxSize : Math.max(1, maxSize - prefix.length());
        List<Chunk> split = fallback.chunk(body, bodyMax, overlap);
        for (Chunk part : split) {
          String content = prefix.isEmpty() ? part.getContent() : prefix + part.getContent();
          chunks.add(
              new Chunk(
                  content,
                  chunkIndex++,
                  section.getStartPosition() + part.getStartPosition(),
                  section.getStartPosition() + part.getEndPosition()));
        }
      }
    }

    if (chunks.isEmpty()) {
      return fallback.chunk(text, maxSize, overlap);
    }

    return chunks;
  }

  private static String formatSection(DocumentNode.DocumentSection section) {
    String breadcrumb = section.breadcrumb();
    String body = section.getBody();
    if (breadcrumb.isEmpty()) {
      return body;
    }
    if (body.isEmpty()) {
      return breadcrumb;
    }
    return breadcrumb + "\n\n" + body;
  }

  @Override
  public ChunkingStrategyType getType() {
    return ChunkingStrategyType.STRUCTURE;
  }
}
