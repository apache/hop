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

package org.apache.hop.core.gui.markdown;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Inserts CommonMark snippets for the markdown editing toolbar. Canvas notes and {@code .md} files
 * share these transforms. Bold and italic use underscores, which CommonMark renders the same way as
 * asterisks.
 */
public final class MarkdownEditing {

  private static final Pattern ATX = Pattern.compile("^#{1,6}(?:[ \\t]+|$)");
  private static final Pattern FENCE = Pattern.compile("(?s)^```[^\\n]*\\n(.*)\\n```\\n?$");

  private MarkdownEditing() {}

  /**
   * A replacement of {@code [start, end)} in the editor text. {@code caret} is the offset after the
   * replacement has been applied.
   */
  public record Edit(int start, int end, String replacement, int caret) {}

  /** Wrap the selection in {@code __}, or remove that wrap when it is already there. */
  public static Edit toggleBold(String text, int start, int end) {
    return toggleWrap(text, start, end, "__");
  }

  /**
   * Wrap the selection in a single {@code _}, or remove that wrap. A selection already wrapped in
   * {@code __} is not treated as italic.
   */
  public static Edit toggleItalic(String text, int start, int end) {
    return toggleWrap(text, start, end, "_");
  }

  /**
   * Insert a fenced code block, or remove the fences when the selection is already one block. With
   * no selection the caret is placed on the blank line between the fences.
   */
  public static Edit codeBlock(String text, int start, int end) {
    int from = lower(text, start, end);
    int to = upper(text, start, end);
    if (from == to) {
      String insertion = "```\n\n```\n";
      return new Edit(from, to, insertion, from + 4);
    }
    String selection = safe(text).substring(from, to);
    Matcher fenced = FENCE.matcher(selection);
    if (fenced.matches()) {
      String inner = fenced.group(1);
      return new Edit(from, to, inner, from + inner.length());
    }
    StringBuilder wrapped = new StringBuilder("```\n");
    wrapped.append(selection);
    if (!selection.endsWith("\n")) {
      wrapped.append('\n');
    }
    wrapped.append("```\n");
    String replacement = wrapped.toString();
    return new Edit(from, to, replacement, from + replacement.length());
  }

  /**
   * Apply an ATX heading to every line touched by the selection. {@code level} 1–4 sets that
   * heading. {@code level} 0 removes a leading heading marker. An existing {@code #} … {@code
   * ######} prefix is replaced, not stacked.
   */
  public static Edit applyHeader(String text, int start, int end, int level) {
    String source = safe(text);
    int from = lower(source, start, end);
    int to = upper(source, start, end);
    int lineStart = from;
    while (lineStart > 0 && source.charAt(lineStart - 1) != '\n') {
      lineStart--;
    }
    int lineEnd = to;
    if (from == to) {
      while (lineEnd < source.length() && source.charAt(lineEnd) != '\n') {
        lineEnd++;
      }
    } else if (!(lineEnd > lineStart && source.charAt(lineEnd - 1) == '\n')) {
      while (lineEnd < source.length() && source.charAt(lineEnd) != '\n') {
        lineEnd++;
      }
    }
    String transformed = transformLines(source.substring(lineStart, lineEnd), level);
    return new Edit(lineStart, lineEnd, transformed, lineStart + transformed.length());
  }

  /** {@code [label](destination)}. An empty label uses the destination. */
  public static String link(String label, String destination) {
    String visible = Utils.isEmpty(label) ? destinationOrEmpty(destination) : label;
    return "[" + escapeLabel(visible) + "](" + escapeDestination(destination) + ")";
  }

  /** {@code ![alt](destination)}. An empty alt uses the file name. */
  public static String image(String alt, String destination) {
    String visible = Utils.isEmpty(alt) ? fileName(destination) : alt;
    if (Utils.isEmpty(visible)) {
      visible = "image";
    }
    return "![" + escapeLabel(visible) + "](" + escapeDestination(destination) + ")";
  }

  /**
   * GFM pipe table. {@code rowsIncludingHeader} counts the header row and not the separator line.
   * Values below the minimum are raised to 1 column and 2 rows.
   */
  public static String table(
      int columns, int rowsIncludingHeader, MarkdownTableAlignment alignment) {
    int cols = Math.max(1, columns);
    int rows = Math.max(2, rowsIncludingHeader);
    MarkdownTableAlignment align = alignment == null ? MarkdownTableAlignment.DEFAULT : alignment;
    StringBuilder sb = new StringBuilder();
    appendRow(sb, cols, "");
    appendRow(sb, cols, align.separator());
    for (int row = 1; row < rows; row++) {
      appendRow(sb, cols, "");
    }
    return sb.toString();
  }

  /**
   * Path to store in Markdown. When {@code baseFilename} is set, the result is relative to that
   * file's parent and uses {@code /} separators. Otherwise {@code selected} is returned unchanged.
   */
  public static String toMarkdownPath(IVariables variables, String baseFilename, String selected) {
    if (Utils.isEmpty(selected) || Utils.isEmpty(baseFilename)) {
      return selected;
    }
    try {
      String base = variables != null ? variables.resolve(baseFilename) : baseFilename;
      String picked = variables != null ? variables.resolve(selected) : selected;
      try (FileObject baseFile = HopVfs.getFileObject(base, variables)) {
        FileObject parent = baseFile.getParent();
        if (parent == null) {
          return selected;
        }
        try (FileObject pickedFile = HopVfs.getFileObject(picked, variables)) {
          String relative = parent.getName().getRelativeName(pickedFile.getName());
          if (Utils.isEmpty(relative) || relative.contains("://")) {
            return selected;
          }
          return UriParser.decode(relative).replace('\\', '/');
        }
      }
    } catch (Exception e) {
      return selected;
    }
  }

  /** Last path segment, for a link or image label. */
  public static String fileName(String path) {
    if (Utils.isEmpty(path)) {
      return "";
    }
    String normalized = path.replace('\\', '/');
    int slash = normalized.lastIndexOf('/');
    String name = slash >= 0 ? normalized.substring(slash + 1) : normalized;
    int query = name.indexOf('?');
    if (query >= 0) {
      name = name.substring(0, query);
    }
    if (name.endsWith(">")) {
      name = name.substring(0, name.length() - 1);
    }
    return name;
  }

  private static Edit toggleWrap(String text, int start, int end, String marker) {
    int from = lower(text, start, end);
    int to = upper(text, start, end);
    String selection = safe(text).substring(from, to);
    String replacement;
    if (wrapped(selection, marker)) {
      replacement = selection.substring(marker.length(), selection.length() - marker.length());
    } else {
      replacement = marker + selection + marker;
    }
    return new Edit(from, to, replacement, from + replacement.length());
  }

  private static boolean wrapped(String selection, String marker) {
    if ("_".equals(marker)) {
      return selection.length() >= 2
          && selection.startsWith("_")
          && selection.endsWith("_")
          && !selection.startsWith("__")
          && !selection.endsWith("__");
    }
    return selection.length() >= marker.length() * 2
        && selection.startsWith(marker)
        && selection.endsWith(marker);
  }

  private static String transformLines(String block, int level) {
    if (block.isEmpty()) {
      return level <= 0 ? "" : "#".repeat(clampLevel(level)) + " ";
    }
    String[] lines = block.split("\\n", -1);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < lines.length; i++) {
      if (i > 0) {
        sb.append('\n');
      }
      if (i == lines.length - 1 && lines[i].isEmpty() && block.endsWith("\n")) {
        continue;
      }
      sb.append(transformLine(lines[i], level));
    }
    return sb.toString();
  }

  private static String transformLine(String line, int level) {
    String ending = "";
    String body = line;
    if (body.endsWith("\r")) {
      ending = "\r";
      body = body.substring(0, body.length() - 1);
    }
    String stripped = ATX.matcher(body).replaceFirst("");
    if (level <= 0) {
      return stripped + ending;
    }
    return "#".repeat(clampLevel(level)) + " " + stripped + ending;
  }

  private static int clampLevel(int level) {
    return Math.min(6, Math.max(1, level));
  }

  private static void appendRow(StringBuilder sb, int columns, String cell) {
    sb.append('|');
    for (int column = 0; column < columns; column++) {
      sb.append(' ').append(cell).append(" |");
    }
    sb.append('\n');
  }

  private static String escapeLabel(String label) {
    if (label == null) {
      return "";
    }
    return label.replace("\\", "\\\\").replace("]", "\\]");
  }

  private static String escapeDestination(String destination) {
    if (destination == null) {
      return "";
    }
    String escaped = destination.replace("\\", "\\\\");
    boolean angles =
        escaped.indexOf(' ') >= 0 || escaped.indexOf('(') >= 0 || escaped.indexOf(')') >= 0;
    if (!angles) {
      return escaped;
    }
    return "<" + escaped.replace(">", "\\>") + ">";
  }

  private static String destinationOrEmpty(String destination) {
    return destination == null ? "" : destination;
  }

  private static String safe(String text) {
    return text == null ? "" : text;
  }

  private static int lower(String text, int start, int end) {
    int length = safe(text).length();
    int from = Math.min(start, end);
    if (from < 0) {
      return 0;
    }
    return Math.min(from, length);
  }

  private static int upper(String text, int start, int end) {
    int length = safe(text).length();
    int to = Math.max(start, end);
    if (to < 0) {
      return 0;
    }
    return Math.min(to, length);
  }
}
