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
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Builds a {@link DocumentNode} tree from heading markers in Markdown or AsciiDoc. */
final class HeadingParserSupport {

  private HeadingParserSupport() {}

  static DocumentNode parse(String text, Pattern headingPattern, ToIntFunction<String> levelFn) {
    if (text == null || text.isEmpty()) {
      return DocumentNode.root("");
    }

    Matcher matcher = headingPattern.matcher(text);
    List<HeadingMatch> headings = new ArrayList<>();
    while (matcher.find()) {
      headings.add(
          new HeadingMatch(
              levelFn.applyAsInt(matcher.group(1)),
              matcher.group(2).trim(),
              matcher.start(),
              matcher.end()));
    }

    if (headings.isEmpty()) {
      return DocumentNode.root(text);
    }

    String preamble = text.substring(0, headings.get(0).start).strip();
    MutableNode root = new MutableNode("", preamble, 0);
    MutableNode[] stack = new MutableNode[7];
    stack[0] = root;

    for (int i = 0; i < headings.size(); i++) {
      HeadingMatch heading = headings.get(i);
      int bodyStart = heading.end;
      int bodyEnd = (i + 1 < headings.size()) ? headings.get(i + 1).start : text.length();
      String body = text.substring(bodyStart, bodyEnd).strip();

      int level = Math.min(Math.max(heading.level, 1), stack.length - 1);
      MutableNode node = new MutableNode(heading.title, body, heading.start);
      stack[level] = node;
      for (int j = level + 1; j < stack.length; j++) {
        stack[j] = null;
      }
      MutableNode parent = stack[level - 1];
      if (parent == null) {
        parent = stack[0];
      }
      parent.children.add(node);
      stack[level] = node;
    }

    return root.toImmutable();
  }

  private static final class HeadingMatch {
    private final int level;
    private final String title;
    private final int start;
    private final int end;

    private HeadingMatch(int level, String title, int start, int end) {
      this.level = level;
      this.title = title;
      this.start = start;
      this.end = end;
    }
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
