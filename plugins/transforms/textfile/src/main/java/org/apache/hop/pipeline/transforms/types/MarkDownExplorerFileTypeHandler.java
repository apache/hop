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

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.commonmark.Extension;
import org.commonmark.ext.footnotes.FootnotesExtension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.task.list.items.TaskListItemsExtension;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

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

  /** Key under which the editor control points back at the handler that created it. */
  private static final String EDITOR_DATA_HANDLER = "MarkDownExplorerFileTypeHandler";

  /** How long typing has to pause before the open preview is re-rendered. */
  private static final int PREVIEW_REFRESH_DELAY_MS = 400;

  /** The tab showing the rendered preview of this file, as long as it is open. */
  private MarkDownPreviewExplorerFileTypeHandler previewHandler;

  /** The tab item we're watching for the close that ends this file's preview. */
  private CTabItem trackedTabItem;

  private Runnable pendingPreviewRefresh;

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

    // The toolbar hands the preview action nothing but the editor widget, so leave a way back
    // to this handler on the control itself.
    editorWidget.getControl().setData(EDITOR_DATA_HANDLER, this);

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
          schedulePreviewRefresh();
        });
  }

  /**
   * Re-renders the preview once typing pauses, so the tab keeps up with the editor without
   * re-rendering on every keystroke. Does nothing when no preview tab is open.
   */
  private void schedulePreviewRefresh() {
    if (previewHandler == null || !previewHandler.isOpen()) {
      return;
    }
    Display display = hopGui.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    if (pendingPreviewRefresh != null) {
      display.timerExec(-1, pendingPreviewRefresh);
    }
    pendingPreviewRefresh =
        () -> {
          pendingPreviewRefresh = null;
          refreshPreview();
        };
    display.timerExec(PREVIEW_REFRESH_DELAY_MS, pendingPreviewRefresh);
  }

  /** Renders the current editor content into the preview tab, leaving the focus where it is. */
  private void refreshPreview() {
    if (previewHandler == null || !previewHandler.isOpen() || isEditorGone()) {
      return;
    }
    previewHandler.setHtml(renderHtml(editorWidget.getText()));
    trackTabClose();
  }

  private boolean isEditorGone() {
    return editorWidget == null
        || editorWidget.getControl() == null
        || editorWidget.getControl().isDisposed();
  }

  /**
   * Watches this file's tab item for the dispose that means "closed", so the preview can go with
   * it. Closing a tab disposes its {@link CTabItem} but not the control it shows, so the item is
   * the only thing that tells us. A tab moved to another pane is disposed as well, but there the
   * perspective still lists this handler, which is how the two are told apart.
   */
  private void trackTabClose() {
    CTabItem tabItem = findTabItem();
    if (tabItem == null || tabItem == trackedTabItem) {
      return;
    }
    trackedTabItem = tabItem;
    tabItem.addDisposeListener(
        e -> {
          if (!hasOpenTab()) {
            closePreview();
          }
        });
  }

  /**
   * True while the perspective still has a tab for this file. It stops being true just before the
   * tab item is disposed on close, while a tab moved to another pane stays listed throughout.
   */
  boolean hasOpenTab() {
    return MarkDownPreviewExplorerFileTypeHandler.isTabOpen(perspective, this);
  }

  /** This file's tab item, or null when the perspective no longer has a tab for it. */
  private CTabItem findTabItem() {
    for (TabItemHandler item : perspective.getItems()) {
      if (item.getTypeHandler() == this) {
        return item.getTabItem();
      }
    }
    return null;
  }

  /** Closes the preview tab of this file, if one is open. */
  private void closePreview() {
    MarkDownPreviewExplorerFileTypeHandler preview = previewHandler;
    previewHandler = null;
    if (preview == null) {
      return;
    }
    if (!preview.isOpen()) {
      preview.disposeWidgets();
      return;
    }
    // We're inside the dispose of our own tab item: let that finish before removing another tab.
    Display display = hopGui.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (preview.isOpen()) {
            perspective.remove(preview);
          }
          preview.disposeWidgets();
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
      String html = renderHtml(editorWidget.getText());

      MarkDownExplorerFileTypeHandler handler = findHandlerOf(editorWidget);
      if (handler != null) {
        handler.showPreview(html);
      } else {
        // Editor without a Markdown tab behind it: still show the render, just not tied to a file
        openPreviewTab(html, BaseMessages.getString(PKG, "MarkDownFileTypeHandler.Preview.Tab"));
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getActiveShell(),
          "Error",
          "Error generating or displaying Markdown preview",
          e);
    }
  }

  /**
   * The handler whose editor this is. The toolbar action is static and gets only the widget that
   * was clicked, and the widget knows which handler built it, which beats guessing from whichever
   * tab is currently active.
   */
  private static MarkDownExplorerFileTypeHandler findHandlerOf(IContentEditorWidget editorWidget) {
    if (editorWidget == null
        || editorWidget.getControl() == null
        || editorWidget.getControl().isDisposed()) {
      return null;
    }
    Object handler = editorWidget.getControl().getData(EDITOR_DATA_HANDLER);
    return handler instanceof MarkDownExplorerFileTypeHandler markdownHandler
        ? markdownHandler
        : null;
  }

  /**
   * Opens the preview of this file in its own tab, or refreshes and selects the tab that is already
   * showing it.
   */
  private void showPreview(String html) {
    if (previewHandler != null && !previewHandler.isOpen()) {
      // The preview tab was closed in the meantime. Closing a tab leaves its widgets behind, so
      // clean those up before opening a new one.
      previewHandler.disposeWidgets();
      previewHandler = null;
    }
    if (previewHandler == null) {
      previewHandler =
          openPreviewTab(
              html,
              BaseMessages.getString(
                  PKG, "MarkDownFileTypeHandler.Preview.TabForFile", Const.NVL(getName(), "")));
    } else {
      // Already open: refresh it and bring it up front
      previewHandler.setHtml(html);
      perspective.setActiveFileTypeHandler(previewHandler);
    }
    trackTabClose();
  }

  private static MarkDownPreviewExplorerFileTypeHandler openPreviewTab(
      String html, String tabName) {
    ExplorerPerspective explorerPerspective = ExplorerPerspective.getInstance();

    // No filename: the preview isn't a file on disk, it's a rendering of the editor content.
    ExplorerFile previewFile =
        new ExplorerFile(tabName, null, new MarkDownPreviewExplorerFileType());
    MarkDownPreviewExplorerFileTypeHandler handler =
        new MarkDownPreviewExplorerFileTypeHandler(
            HopGui.getInstance(), explorerPerspective, previewFile);
    handler.setHtml(html);

    explorerPerspective.addFile(handler);
    return handler;
  }

  /** Renders Markdown to a full HTML page, styled to match the Hop GUI light or dark theme. */
  private static String renderHtml(String markdown) {
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
    html.append("  font-family: SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace;\n");
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

    return html.toString();
  }
}
