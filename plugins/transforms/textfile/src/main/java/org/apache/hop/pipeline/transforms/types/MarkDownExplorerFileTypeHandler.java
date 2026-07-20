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
 *
 */

package org.apache.hop.pipeline.transforms.types;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.commonmark.Extension;
import org.commonmark.ext.footnotes.FootnotesExtension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.task.list.items.TaskListItemsExtension;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.eclipse.swt.widgets.Composite;

/** How do we handle a markdown file in the file explorer perspective? */
@GuiPlugin(name = "Markdown file type handler")
public class MarkDownExplorerFileTypeHandler extends BaseTextExplorerFileTypeHandler {

  private static final Class<?> PKG = MarkDownExplorerFileType.class;

  protected static final String LANGUAGE = "markdown";

  public static final String TOOLBAR_ITEM_PREVIEW =
      "MarkDownExplorerFileTypeHandler-ToolBar-Preview";

  private static final List<Extension> MARKDOWN_EXTENSIONS =
      List.of(
          TablesExtension.create(), TaskListItemsExtension.create(), FootnotesExtension.create());

  public MarkDownExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  protected String getLanguageId() {
    return LANGUAGE;
  }

  @Override
  public void renderFile(Composite composite) {

    // Shared content-editor toolbar (incl. Markdown preview) is built inside the editor widget.
    editorWidget = ContentEditorFacade.createContentEditor(composite, getLanguageId());
    editorWidget.getControl().setLayoutData(FormDataBuilder.builder().fullSize().build());

    // If it's a new file, there's no need to reload it
    if (this.getFilename() != null) {
      reload();
    }

    reloadListener = true;
    editorWidget.addModifyListener(
        e -> {
          if (reloadListener) {
            this.setChanged();
            perspective.updateGui();
          }
        });
  }

  @GuiToolbarElementFilter(parentId = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID)
  public static boolean showForMarkdownFileType(String itemId, Object guiPluginInstance) {
    if (TOOLBAR_ITEM_PREVIEW.equals(itemId)
        && guiPluginInstance instanceof IContentEditorWidget editor) {
      return LANGUAGE.equals(editor.getLanguage());
    }
    return true;
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PREVIEW,
      toolTip = "i18n::MarkDownFileTypeHandler.Preview.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/preview.svg",
      separator = true)
  public static void previewMarkdown(IContentEditorWidget editorWidget) {
    try {
      String markdown = editorWidget.getText();

      // Parse markdown to HTML body content
      Parser parser = Parser.builder().extensions(MARKDOWN_EXTENSIONS).build();
      HtmlRenderer renderer = HtmlRenderer.builder().extensions(MARKDOWN_EXTENSIONS).build();
      Node document = parser.parse(markdown);
      String htmlContent = renderer.render(document);

      // Wrap the content with styled CSS, supporting dark mode if configured
      StringBuilder html = new StringBuilder();
      html.append("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"UTF-8\">\n<style>\n");
      html.append("body {\n");
      html.append(
          "  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;\n");
      html.append("  line-height: 1.625;\n");
      html.append("  margin: 40px auto;\n");
      html.append("  max-width: 800px;\n");
      html.append("  padding: 0 20px;\n");
      html.append("}\n");
      html.append("h1, h2, h3, h4, h5, h6 {\n");
      html.append("  font-weight: 600;\n");
      html.append("}\n");
      html.append("h1 {\n");
      html.append("  font-size: 2.25rem;\n");
      html.append("  padding-bottom: 0.3em;\n");
      html.append("  border-bottom: 1px solid;\n");
      html.append("}\n");
      html.append("h2 {\n");
      html.append("  font-size: 1.5rem;\n");
      html.append("  padding-bottom: 0.3em;\n");
      html.append("  border-bottom: 1px solid;\n");
      html.append("}\n");
      html.append("a {\n");
      html.append("  text-decoration: none;\n");
      html.append("  font-weight: 500;\n");
      html.append("}\n");
      html.append("a:hover {\n");
      html.append("  text-decoration: underline;\n");
      html.append("}\n");
      html.append("pre, code {\n");
      html.append(
          "  font-family: SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace;\n");
      html.append("  font-size: 0.9em;\n");
      html.append("  border-radius: 6px;\n");
      html.append("}\n");
      html.append("code {\n");
      html.append("  padding: 0.2em 0.4em;\n");
      html.append("}\n");
      html.append("pre {\n");
      html.append("  padding: 16px;\n");
      html.append("  overflow-x: auto;\n");
      html.append("}\n");
      html.append("pre code {\n");
      html.append("  padding: 0;\n");
      html.append("  background-color: transparent;\n");
      html.append("  border-radius: 0;\n");
      html.append("  border: none;\n");
      html.append("}\n");
      html.append("blockquote {\n");
      html.append("  margin: 1.5em 0;\n");
      html.append("  padding: 0.5em 1em;\n");
      html.append("  border-left-width: 4px;\n");
      html.append("  border-left-style: solid;\n");
      html.append("}\n");
      html.append("table {\n");
      html.append("  width: 100%;\n");
      html.append("  margin: 1.5em 0;\n");
      html.append("  border-collapse: collapse;\n");
      html.append("  border-radius: 6px;\n");
      html.append("  overflow: hidden;\n");
      html.append("  font-size: 0.95em;\n");
      html.append("}\n");
      html.append("th, td {\n");
      html.append("  padding: 0.6em 0.85em;\n");
      html.append("  border: 1px solid;\n");
      html.append("  text-align: left;\n");
      html.append("}\n");
      html.append("th {\n");
      html.append("  font-weight: 600;\n");
      html.append("}\n");

      if (PropsUi.getInstance().isDarkMode()) {
        html.append("body {\n");
        html.append("  background-color: #0b0f19;\n");
        html.append("  color: #94a3b8;\n");
        html.append("}\n");
        html.append("h1, h2, h3, h4, h5, h6 {\n");
        html.append("  color: #f8fafc;\n");
        html.append("}\n");
        html.append("h1, h2 {\n");
        html.append("  border-bottom-color: #1e293b;\n");
        html.append("}\n");
        html.append("a {\n");
        html.append("  color: #38bdf8;\n");
        html.append("}\n");
        html.append("pre, code {\n");
        html.append("  background-color: #1e293b;\n");
        html.append("  border: 1px solid #334155;\n");
        html.append("  color: #e2e8f0;\n");
        html.append("}\n");
        html.append("blockquote {\n");
        html.append("  border-left-color: #475569;\n");
        html.append("  color: #94a3b8;\n");
        html.append("  background-color: #0f172a;\n");
        html.append("}\n");
        html.append("table {\n");
        html.append("  background-color: #0f172a;\n");
        html.append("}\n");
        html.append("th, td {\n");
        html.append("  border-color: #334155;\n");
        html.append("}\n");
        html.append("th {\n");
        html.append("  background-color: #1e293b;\n");
        html.append("  color: #f8fafc;\n");
        html.append("}\n");
        html.append("tbody tr:nth-child(even) {\n");
        html.append("  background-color: #111827;\n");
        html.append("}\n");
      } else {
        html.append("body {\n");
        html.append("  background-color: #f8fafc;\n");
        html.append("  color: #334155;\n");
        html.append("}\n");
        html.append("h1, h2, h3, h4, h5, h6 {\n");
        html.append("  color: #0f172a;\n");
        html.append("}\n");
        html.append("h1, h2 {\n");
        html.append("  border-bottom-color: #e2e8f0;\n");
        html.append("}\n");
        html.append("a {\n");
        html.append("  color: #2563eb;\n");
        html.append("}\n");
        html.append("pre, code {\n");
        html.append("  background-color: #f1f5f9;\n");
        html.append("  border: 1px solid #e2e8f0;\n");
        html.append("  color: #334155;\n");
        html.append("}\n");
        html.append("blockquote {\n");
        html.append("  border-left-color: #cbd5e1;\n");
        html.append("  color: #64748b;\n");
        html.append("  background-color: #f8fafc;\n");
        html.append("}\n");
        html.append("table {\n");
        html.append("  background-color: #ffffff;\n");
        html.append("}\n");
        html.append("th, td {\n");
        html.append("  border-color: #e2e8f0;\n");
        html.append("}\n");
        html.append("th {\n");
        html.append("  background-color: #f1f5f9;\n");
        html.append("  color: #0f172a;\n");
        html.append("}\n");
        html.append("tbody tr:nth-child(even) {\n");
        html.append("  background-color: #f8fafc;\n");
        html.append("}\n");
      }
      html.append("</style>\n</head>\n<body>\n");
      html.append(htmlContent);
      html.append("\n</body>\n</html>");

      String fullHtml = html.toString();

      // Hop Web: openUrl(file://...) points at the server temp path, which the client browser
      // cannot read. Show the rendered HTML in an in-app Browser dialog instead.
      // Desktop: keep writing a temp file and open it in the system browser.
      if (EnvironmentUtils.getInstance().isWeb()) {
        ShowBrowserDialog dialog =
            new ShowBrowserDialog(
                HopGui.getInstance().getActiveShell(), "Markdown preview", fullHtml);
        dialog.open();
      } else {
        File tempFile = File.createTempFile("markdown_preview_", ".html");
        tempFile.deleteOnExit();
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
          outputStream.write(fullHtml.getBytes(StandardCharsets.UTF_8));
        }
        EnvironmentUtils.getInstance().openUrl(tempFile.toURI().toString());
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getActiveShell(),
          "Error",
          "Error generating or displaying Markdown preview",
          e);
    }
  }
}
