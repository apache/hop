/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use it except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;

/**
 * Shared content-editor toolbar actions for Hop GUI (desktop) and Hop Web.
 *
 * <p>Toolbar items must live on a class present on both classpaths. The RCP JFace editor and RAP
 * Monaco/fallback widgets implement {@link IContentEditorWidget}; listeners are static methods
 * taking that interface so {@link org.apache.hop.ui.core.gui.BaseGuiWidgets} can invoke them with
 * the registered editor instance.
 *
 * <p>Context menu and keyboard shortcuts remain on the RCP {@code ContentEditorWidget} (desktop
 * only).
 */
@GuiPlugin(name = "Content editor")
public class ContentEditorActions {

  public static final String ID_TOOLBAR_UNDO = "ContentEditor-Toolbar-10000-undo";
  public static final String ID_TOOLBAR_REDO = "ContentEditor-Toolbar-10010-redo";
  public static final String ID_TOOLBAR_SELECT_ALL = "ContentEditor-Toolbar-20000-select-all";
  public static final String ID_TOOLBAR_UNSELECT_ALL = "ContentEditor-Toolbar-20010-unselect-all";
  public static final String ID_TOOLBAR_COPY = "ContentEditor-Toolbar-30000-copy";
  public static final String ID_TOOLBAR_PASTE = "ContentEditor-Toolbar-30010-paste";
  public static final String ID_TOOLBAR_CUT = "ContentEditor-Toolbar-30020-cut";

  private ContentEditorActions() {}

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_UNDO,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/undo.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.Undo.Tooltip")
  public static void undo(IContentEditorWidget editor) {
    editor.undo();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_REDO,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/redo.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.Redo.Tooltip")
  public static void redo(IContentEditorWidget editor) {
    editor.redo();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_SELECT_ALL,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/select-all.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.SelectAll.Tooltip",
      separator = true)
  public static void selectAll(IContentEditorWidget editor) {
    editor.selectAll();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_UNSELECT_ALL,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/unselect-all.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.UnselectAll.Tooltip")
  public static void unselectAll(IContentEditorWidget editor) {
    editor.unselectAll();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_COPY,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/copy.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.Copy.Tooltip",
      separator = true)
  public static void copy(IContentEditorWidget editor) {
    editor.copy();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_CUT,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/cut.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.Cut.Tooltip")
  public static void cut(IContentEditorWidget editor) {
    editor.cut();
  }

  @GuiToolbarElement(
      root = IContentEditorWidget.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_PASTE,
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/paste.svg",
      toolTip = "i18n::ContentEditorWidget.ToolBar.Paste.Tooltip")
  public static void paste(IContentEditorWidget editor) {
    editor.paste();
  }
}
