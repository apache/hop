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

/** Builds a {@link DocumentNode} tree from the headings found in Markdown or AsciiDoc. */
final class HeadingParserSupport {

  /** Deepest heading level kept apart; deeper headings are nested at this level. */
  private static final int MAX_LEVEL = 6;

  private HeadingParserSupport() {}

  /** A heading line: its level (1 = top), title and the offsets of the line in the text. */
  record Heading(int level, String title, int start, int end) {}

  /** One line of the text without its line terminator, with its offsets in the text. */
  record Line(String text, int start, int end) {}

  /** Splits text into lines on {@code \n}; a trailing {@code \r} is not part of the line. */
  static List<Line> lines(String text) {
    List<Line> lines = new ArrayList<>();
    int start = 0;
    while (start <= text.length()) {
      int newline = text.indexOf('\n', start);
      int end = newline < 0 ? text.length() : newline;
      int contentEnd = end > start && text.charAt(end - 1) == '\r' ? end - 1 : end;
      lines.add(new Line(text.substring(start, contentEnd), start, contentEnd));
      if (newline < 0) {
        break;
      }
      start = newline + 1;
    }
    return lines;
  }

  static DocumentNode parse(String text, List<Heading> headings) {
    if (text == null || text.isEmpty()) {
      return DocumentNode.root("");
    }
    if (headings.isEmpty()) {
      return DocumentNode.root(text);
    }

    String preamble = text.substring(0, headings.get(0).start()).strip();
    MutableNode root = new MutableNode("", preamble, 0);
    MutableNode[] stack = new MutableNode[MAX_LEVEL + 1];
    stack[0] = root;

    for (int i = 0; i < headings.size(); i++) {
      Heading heading = headings.get(i);
      int bodyStart = heading.end();
      int bodyEnd = (i + 1 < headings.size()) ? headings.get(i + 1).start() : text.length();
      String body = text.substring(bodyStart, bodyEnd).strip();

      int level = Math.min(Math.max(heading.level(), 1), MAX_LEVEL);
      MutableNode node = new MutableNode(heading.title(), body, heading.start());
      // The parent is the nearest open heading above this level, so a skipped level
      // (# Guide followed by ### Detail) still nests under Guide.
      MutableNode parent = root;
      for (int j = level - 1; j > 0; j--) {
        if (stack[j] != null) {
          parent = stack[j];
          break;
        }
      }
      parent.children.add(node);
      stack[level] = node;
      for (int j = level + 1; j < stack.length; j++) {
        stack[j] = null;
      }
    }

    return root.toImmutable();
  }

  private static final class MutableNode {
    private final String title;
    private final String body;
    private final int startPosition;
    private final List<MutableNode> children = new ArrayList<>();

    private MutableNode(String title, String body, int startPosition) {
      this.title = title;
      this.body = body;
      this.startPosition = startPosition;
    }

    private DocumentNode toImmutable() {
      List<DocumentNode> childNodes = new ArrayList<>();
      for (MutableNode child : children) {
        childNodes.add(child.toImmutable());
      }
      return new DocumentNode(title, body, startPosition, childNodes);
    }
  }
}
