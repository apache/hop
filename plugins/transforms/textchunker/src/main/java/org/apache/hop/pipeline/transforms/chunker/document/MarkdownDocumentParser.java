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
package org.apache.hop.pipeline.transforms.chunker.document;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.pipeline.transforms.chunker.document.HeadingParserSupport.Heading;
import org.apache.hop.pipeline.transforms.chunker.document.HeadingParserSupport.Line;

/**
 * Splits Markdown on {@code #} headings and builds a section tree. Lines inside a fenced code block
 * ({@code ```} or {@code ~~~}) are never headings, so a shell comment in a code sample stays in the
 * section body.
 */
public final class MarkdownDocumentParser implements DocumentParser {

  /** An ATX heading: 1-6 {@code #}, then a space or tab and the title, on a single line. */
  private static final Pattern HEADING = Pattern.compile("^(#{1,6})[ \\t]+(\\S.*)$");

  /** The optional closing sequence of an ATX heading ({@code ## Install ##}). */
  private static final Pattern CLOSING_SEQUENCE = Pattern.compile("(?:^|[ \\t]+)#+[ \\t]*$");

  /** A code fence: up to 3 spaces of indentation, then 3 or more backticks or tildes. */
  private static final Pattern FENCE = Pattern.compile("^ {0,3}(`{3,}|~{3,})(.*)$");

  @Override
  public ContentType getContentType() {
    return ContentType.MARKDOWN;
  }

  @Override
  public DocumentNode parse(String text) {
    if (text == null || text.isEmpty()) {
      return DocumentNode.root("");
    }
    return HeadingParserSupport.parse(text, scan(text).headings());
  }

  /**
   * The headings of a Markdown text and the {@code [start, end)} offsets of its fenced code blocks.
   * An unclosed fence runs to the end of the text.
   */
  record Scan(List<Heading> headings, List<int[]> fencedBlocks) {}

  static Scan scan(String text) {
    List<Heading> headings = new ArrayList<>();
    List<int[]> fencedBlocks = new ArrayList<>();
    String openFence = null;
    int fenceStart = -1;
    for (Line line : HeadingParserSupport.lines(text)) {
      Matcher fence = FENCE.matcher(line.text());
      if (openFence != null) {
        // A fence closes on the same character, at least as long, with nothing after it.
        if (fence.matches()
            && fence.group(1).charAt(0) == openFence.charAt(0)
            && fence.group(1).length() >= openFence.length()
            && fence.group(2).isBlank()) {
          openFence = null;
          fencedBlocks.add(new int[] {fenceStart, line.end()});
        }
        continue;
      }
      if (fence.matches() && !(fence.group(1).charAt(0) == '`' && fence.group(2).contains("`"))) {
        openFence = fence.group(1);
        fenceStart = line.start();
        continue;
      }
      Heading heading = heading(line);
      if (heading != null) {
        headings.add(heading);
      }
    }
    if (openFence != null) {
      fencedBlocks.add(new int[] {fenceStart, text.length()});
    }
    return new Scan(headings, fencedBlocks);
  }

  private static Heading heading(Line line) {
    Matcher matcher = HEADING.matcher(line.text());
    if (!matcher.matches()) {
      return null;
    }
    String text = matcher.group(2).strip();
    String title = CLOSING_SEQUENCE.matcher(text).replaceFirst("").strip();
    // "## #" would otherwise get an empty title, so the breadcrumb ends in "Guide > ".
    return new Heading(
        matcher.group(1).length(), title.isEmpty() ? text : title, line.start(), line.end());
  }
}
