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

package org.apache.hop.ai.engine;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.markdown.CommonMarkConfig;
import org.apache.hop.core.util.Utils;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.BulletList;
import org.commonmark.node.Code;
import org.commonmark.node.Emphasis;
import org.commonmark.node.FencedCodeBlock;
import org.commonmark.node.HardLineBreak;
import org.commonmark.node.Heading;
import org.commonmark.node.IndentedCodeBlock;
import org.commonmark.node.Link;
import org.commonmark.node.ListItem;
import org.commonmark.node.OrderedList;
import org.commonmark.node.Paragraph;
import org.commonmark.node.SoftLineBreak;
import org.commonmark.node.StrongEmphasis;
import org.commonmark.node.Text;
import org.commonmark.node.ThematicBreak;

/**
 * Flattens CommonMark to plain text plus style spans so the AI transcript can show headings, bold
 * and code without raw {@code **} / {@code ##} markers.
 */
public final class AiAdvisorMarkdown {

  public enum Kind {
    HEADING,
    BOLD,
    EMPHASIS,
    CODE
  }

  public record Span(int start, int length, Kind kind) {}

  public record Document(String text, List<Span> spans) {}

  private AiAdvisorMarkdown() {}

  public static Document render(String markdown) {
    if (Utils.isEmpty(markdown)) {
      return new Document("", List.of());
    }
    Flatten flatten = new Flatten();
    CommonMarkConfig.parse(markdown).accept(flatten);
    return flatten.build();
  }

  private static final class Flatten extends AbstractVisitor {
    private final StringBuilder text = new StringBuilder();
    private final List<Span> spans = new ArrayList<>();
    private int orderedItem;

    Document build() {
      while (!text.isEmpty() && Character.isWhitespace(text.charAt(text.length() - 1))) {
        text.setLength(text.length() - 1);
      }
      return new Document(text.toString(), List.copyOf(spans));
    }

    private void mark(int start, Kind kind) {
      int length = text.length() - start;
      if (length > 0) {
        spans.add(new Span(start, length, kind));
      }
    }

    private void ensureNewline() {
      if (text.isEmpty()) {
        return;
      }
      if (text.charAt(text.length() - 1) != '\n') {
        text.append('\n');
      }
    }

    private void ensureBlankLine() {
      ensureNewline();
      if (text.isEmpty()) {
        return;
      }
      if (text.length() < 2 || text.charAt(text.length() - 2) != '\n') {
        text.append('\n');
      }
    }

    @Override
    public void visit(Text textNode) {
      text.append(textNode.getLiteral());
    }

    @Override
    public void visit(SoftLineBreak softLineBreak) {
      text.append(' ');
    }

    @Override
    public void visit(HardLineBreak hardLineBreak) {
      text.append('\n');
    }

    @Override
    public void visit(Paragraph paragraph) {
      if (paragraph.getParent() instanceof ListItem) {
        visitChildren(paragraph);
        return;
      }
      visitChildren(paragraph);
      ensureBlankLine();
    }

    @Override
    public void visit(Heading heading) {
      int start = text.length();
      visitChildren(heading);
      mark(start, Kind.HEADING);
      ensureBlankLine();
    }

    @Override
    public void visit(StrongEmphasis strongEmphasis) {
      int start = text.length();
      visitChildren(strongEmphasis);
      mark(start, Kind.BOLD);
    }

    @Override
    public void visit(Emphasis emphasis) {
      int start = text.length();
      visitChildren(emphasis);
      mark(start, Kind.EMPHASIS);
    }

    @Override
    public void visit(Code code) {
      int start = text.length();
      text.append(code.getLiteral());
      mark(start, Kind.CODE);
    }

    @Override
    public void visit(FencedCodeBlock fencedCodeBlock) {
      ensureNewline();
      int start = text.length();
      String literal = fencedCodeBlock.getLiteral();
      text.append(literal != null ? literal : "");
      mark(start, Kind.CODE);
      ensureBlankLine();
    }

    @Override
    public void visit(IndentedCodeBlock indentedCodeBlock) {
      ensureNewline();
      int start = text.length();
      String literal = indentedCodeBlock.getLiteral();
      text.append(literal != null ? literal : "");
      mark(start, Kind.CODE);
      ensureBlankLine();
    }

    @Override
    public void visit(BulletList bulletList) {
      int previous = orderedItem;
      orderedItem = 0;
      visitChildren(bulletList);
      orderedItem = previous;
      ensureBlankLine();
    }

    @Override
    public void visit(OrderedList orderedList) {
      int previous = orderedItem;
      orderedItem = Math.max(1, orderedList.getStartNumber());
      visitChildren(orderedList);
      orderedItem = previous;
      ensureBlankLine();
    }

    @Override
    public void visit(ListItem listItem) {
      ensureNewline();
      if (orderedItem > 0) {
        text.append(orderedItem++).append(". ");
      } else {
        text.append("• ");
      }
      visitChildren(listItem);
      ensureNewline();
    }

    @Override
    public void visit(Link link) {
      visitChildren(link);
    }

    @Override
    public void visit(ThematicBreak thematicBreak) {
      ensureNewline();
      text.append("───");
      ensureBlankLine();
    }
  }
}
