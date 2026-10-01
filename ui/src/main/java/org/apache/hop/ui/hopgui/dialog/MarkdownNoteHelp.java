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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.eclipse.swt.widgets.Shell;

/**
 * In-app help for Markdown notes on the pipeline/workflow canvas: syntax examples and what Hop
 * supports when rendering notes.
 */
public final class MarkdownNoteHelp {

  private static final Class<?> PKG = NotePadDialog.class;

  private MarkdownNoteHelp() {}

  public static void show(Shell shell) {
    boolean dark = PropsUi.getInstance().isDarkMode();
    ShowBrowserDialog dialog =
        new ShowBrowserDialog(
            shell,
            BaseMessages.getString(PKG, "NotePadDialog.Markdown.Help.Title"),
            buildHtml(dark));
    dialog.open();
  }

  static String buildHtml(boolean darkMode) {
    StringBuilder html = new StringBuilder();
    html.append("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"UTF-8\">\n<style>\n");
    html.append("body { font-family: system-ui, sans-serif; line-height: 1.55; margin: 28px; }\n");
    html.append("h1 { font-size: 1.6rem; margin-top: 0; }\n");
    html.append("h2 { font-size: 1.2rem; margin-top: 1.4em; }\n");
    html.append("h3 { font-size: 1.05rem; }\n");
    html.append(
        "code, pre { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 0.9em; }\n");
    html.append("code { padding: 0.12em 0.35em; border-radius: 4px; }\n");
    html.append("pre { padding: 12px 14px; overflow-x: auto; border-radius: 6px; }\n");
    html.append("table { border-collapse: collapse; margin: 0.8em 0; width: 100%; }\n");
    html.append(
        "th, td { border: 1px solid; padding: 0.4em 0.65em; text-align: left; vertical-align: top; }\n");
    html.append("th { font-weight: 600; }\n");
    html.append("ul { padding-left: 1.3em; }\n");
    html.append("li { margin: 0.25em 0; }\n");
    if (darkMode) {
      html.append("body { background: #0b0f19; color: #e2e8f0; }\n");
      html.append("code, pre { background: #1e293b; }\n");
      html.append("th, td { border-color: #334155; }\n");
      html.append("th { background: #1e293b; }\n");
      html.append("a { color: #7dd3fc; }\n");
    } else {
      html.append("body { background: #ffffff; color: #0f172a; }\n");
      html.append("code, pre { background: #f1f5f9; }\n");
      html.append("th, td { border-color: #e2e8f0; }\n");
      html.append("th { background: #f8fafc; }\n");
      html.append("a { color: #0369a1; }\n");
    }
    html.append("</style>\n</head>\n<body>\n");

    html.append("<h1>Markdown notes in Hop</h1>\n");
    html.append(
        "<p>Canvas notes on pipelines and workflows can use <strong>CommonMark</strong> with "
            + "<strong>GFM tables</strong> and <strong>task lists</strong>. "
            + "Enable <em>Render as Markdown</em> in the note editor, pick a <em>Note type</em> "
            + "for colors, and write source using the fixed-width editor.</p>\n");

    html.append("<h2>Getting started</h2>\n");
    html.append("<ul>\n");
    html.append(
        "<li>Create a note from the canvas context menu (empty area or note actions).</li>\n");
    html.append("<li>Check <strong>Render as Markdown</strong>.</li>\n");
    html.append(
        "<li>Choose a type: <strong>General</strong>, <strong>Information</strong>, "
            + "<strong>Important</strong>, or <strong>Warning</strong> "
            + "(system colors; font tab is ignored in Markdown mode).</li>\n");
    html.append("<li>Use <strong>Preview</strong> for an HTML preview of the source.</li>\n");
    html.append(
        "<li><strong>Ctrl+click</strong> (Windows/Linux) or <strong>⌘+click</strong> (macOS) "
            + "a note on the canvas to edit it.</li>\n");
    html.append("</ul>\n");

    html.append("<h2>Simple examples</h2>\n");
    html.append("<pre>");
    html.append("# Title\n");
    html.append("## Section\n\n");
    html.append("Normal paragraph with **bold**, *italic*, and `inline code`.\n\n");
    html.append("- Bullet one\n");
    html.append("- Bullet two\n\n");
    html.append("1. First step\n");
    html.append("2. Second step\n\n");
    html.append("- [ ] Todo item\n");
    html.append("- [x] Done item\n\n");
    html.append(
        "See the [Hop site](https://hop.apache.org) or open [another pipeline](other.hpl).\n\n");
    html.append("![Diagram](docs/diagram.png)\n\n");
    html.append("| Column A | Column B |\n");
    html.append("| -------- | -------- |\n");
    html.append("| value 1  | value 2  |\n");
    html.append("</pre>\n");

    html.append("<h2>What is supported on the canvas</h2>\n");
    html.append("<table>\n");
    html.append("<tr><th>Feature</th><th>Markdown</th><th>Notes</th></tr>\n");
    html.append(
        "<tr><td>Headings</td><td><code>#</code> … <code>######</code></td>"
            + "<td>Sized relative to the graph font</td></tr>\n");
    html.append(
        "<tr><td>Emphasis</td><td><code>**bold**</code>, <code>*italic*</code></td>"
            + "<td></td></tr>\n");
    html.append(
        "<tr><td>Code</td><td>Inline <code>`code`</code> and fenced blocks</td>"
            + "<td>Monospace; fenced blocks get a shaded background</td></tr>\n");
    html.append(
        "<tr><td>Lists</td><td><code>-</code> / <code>*</code> bullets, numbered lists</td>"
            + "<td>Task lists: <code>- [ ]</code> / <code>- [x]</code></td></tr>\n");
    html.append(
        "<tr><td>Links</td><td><code>[label](target)</code></td>"
            + "<td><code>http(s)</code> opens a browser; relative "
            + "<code>.hpl</code> / <code>.hwf</code> (and other registered types) open in Hop. "
            + "Click the link; Ctrl/⌘+click edits the note.</td></tr>\n");
    html.append(
        "<tr><td>Images</td><td><code>![alt](path)</code></td>"
            + "<td>Local/VFS paths only (relative to this pipeline or workflow). "
            + "PNG, JPEG, GIF, SVG. No remote <code>http(s)</code> images.</td></tr>\n");
    html.append(
        "<tr><td>Tables</td><td>GFM pipe tables</td>" + "<td>Header row emphasized</td></tr>\n");
    html.append(
        "<tr><td>Horizontal rule</td><td><code>---</code> on its own line</td>"
            + "<td></td></tr>\n");
    html.append("</table>\n");

    html.append("<h2>What is not supported</h2>\n");
    html.append("<ul>\n");
    html.append("<li>Raw HTML passthrough inside the note body</li>\n");
    html.append("<li>Remote images (<code>http://</code> / <code>https://</code>)</li>\n");
    html.append("<li>MultiMarkdown extras (citations, math, bibliographies)</li>\n");
    html.append("<li>Custom fonts/colors while Markdown is on (use note type instead)</li>\n");
    html.append("</ul>\n");

    html.append("<h2>Tips</h2>\n");
    html.append("<ul>\n");
    html.append(
        "<li>Resize a note by its edges; text reflows. Default new-note width is readable for "
            + "Markdown wrapping.</li>\n");
    html.append(
        "<li>Put images next to the pipeline/workflow (or under a subfolder) and use relative "
            + "paths like <code>images/overview.png</code>.</li>\n");
    html.append(
        "<li>Dark mode uses note-type palettes designed for contrast (same idea as model notes "
            + "elsewhere in Hop).</li>\n");
    html.append("</ul>\n");

    html.append(
        "<p><em>Full documentation: Hop User Manual → Hop Gui → Notes on pipelines and "
            + "workflows.</em></p>\n");

    html.append("</body>\n</html>");
    return html.toString();
  }
}
