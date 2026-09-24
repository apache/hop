/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.vfs.explorer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.vfs.IVfsBrowseLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.terminal.HopGuiBottomDock;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/** Tools menu, bottom-dock tab, and the Explore action on a VFS connection editor. */
@GuiPlugin
public class VfsFileExplorerViews {

  public static final Class<?> PKG = VfsFileExplorer.class;

  public static final String DOCK_TOOL_ID = "vfs-file-explorer";

  public static final String ID_MAIN_MENU_TOOLS_VFS_EXPLORER = "40030-menu-tools-vfs-explorer";

  public static final String CONTEXT_MENU_OPEN_LOCATION =
      "ExplorerPerspective-ContextMenu-10103-OpenInVfsExplorer";

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_VFS_EXPLORER,
      label = "i18n::VfsFileExplorer.Menu.Tools",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "ui/images/folder.svg")
  public void menuToolsVfsExplorer() {
    HopGui hopGui;
    try {
      hopGui = HopGui.peekInstance();
    } catch (Throwable e) {
      return;
    }
    if (hopGui != null) {
      VfsFileExplorerDialog.open(hopGui);
    }
  }

  public static Button[] exploreButton(Composite parent, MetadataEditor<?> editor) {
    Button button = new Button(parent, SWT.PUSH);
    button.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Explore.Button"));
    button.addListener(SWT.Selection, e -> explore(editor));
    return new Button[] {button};
  }

  /**
   * Save the editor when it has unsaved changes, then open the floating explorer at the connection
   * root. VFS resolves the saved connection, not the unsaved form.
   */
  public static void explore(MetadataEditor<?> editor) {
    if (editor == null || editor.getHopGui() == null) {
      return;
    }
    try {
      copyWidgets(editor);
      Object metadata = editor.getMetadata();
      if (!(metadata instanceof IVfsBrowseLocation location)) {
        return;
      }
      if (editor.hasChanged()) {
        editor.save();
      }
      String root = location.getBrowseRoot(editor.getHopGui().getVariables());
      if (StringUtils.isBlank(root)) {
        MessageBox box = new MessageBox(editor.getShell(), SWT.OK | SWT.ICON_INFORMATION);
        box.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Explore.NoName.Title"));
        box.setMessage(BaseMessages.getString(PKG, "VfsFileExplorer.Explore.NoName.Message"));
        box.open();
        return;
      }
      VfsFileExplorerDialog.openAt(editor.getHopGui(), root);
    } catch (Exception e) {
      new ErrorDialog(
          editor.getShell(),
          BaseMessages.getString(PKG, "VfsFileExplorer.Explore.Error.Title"),
          BaseMessages.getString(PKG, "VfsFileExplorer.Explore.Error.Message"),
          e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void copyWidgets(MetadataEditor editor) {
    editor.getWidgetsContent(editor.getMetadata());
  }

  public static void openDock(HopGui hopGui, String location) {
    if (hopGui == null) {
      return;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return;
    }
    String title = BaseMessages.getString(PKG, "VfsFileExplorer.Dock.Title");
    Control control =
        dock.focusOrOpenToolTab(
            DOCK_TOOL_ID,
            title,
            GuiResource.getInstance().getImageFolder(),
            true,
            container -> createDockedExplorer(container, hopGui, location));
    if (control instanceof VfsFileExplorer explorer && !explorer.isDisposed()) {
      explorer.activate();
      if (StringUtils.isNotEmpty(location)) {
        explorer.openLocation(location);
      }
    }
  }

  private static Control createDockedExplorer(Composite container, HopGui hopGui, String location) {
    VfsFileExplorer explorer = new VfsFileExplorer(container, hopGui);
    explorer.setLayoutData(new FormDataBuilder().fullSize().result());
    explorer.openLocation(location);
    explorer.activate();
    return explorer;
  }

  /**
   * Open {@code location} in the bottom panel when that panel is already showing the explorer, and
   * in the floating window otherwise.
   */
  public static void openAtPreferredHost(HopGui hopGui, String location) {
    if (hopGui == null || StringUtils.isBlank(location)) {
      return;
    }
    if (isExplorerDockVisible(hopGui)) {
      openDock(hopGui, location);
    } else {
      VfsFileExplorerDialog.openAt(hopGui, location);
    }
  }

  /** True when the explorer tab exists and the bottom panel is on screen. */
  public static boolean isExplorerDockVisible(HopGui hopGui) {
    if (!isDockOpen(hopGui)) {
      return false;
    }
    return hopGui.getTerminalPanel().isTerminalVisible();
  }

  @GuiMenuElement(
      root = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = ExplorerPerspective.GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_OPEN_LOCATION,
      label = "i18n::VfsFileExplorer.Menu.OpenLocation",
      image = "ui/images/folder.svg")
  public void openSelectionInVfsExplorer() {
    // The menu caller looks up a public no-argument method. A parameter here is never invoked.
    ExplorerPerspective perspective;
    HopGui hopGui;
    try {
      perspective = ExplorerPerspective.getInstance();
      hopGui = HopGui.getInstance();
    } catch (Throwable e) {
      return;
    }
    if (perspective == null || hopGui == null) {
      return;
    }
    ExplorerFile selected = perspective.getSelectedFile();
    if (selected == null || StringUtils.isBlank(selected.getFilename())) {
      return;
    }
    String path = selected.getFilename();
    if (hopGui.getVariables() != null) {
      path = hopGui.getVariables().resolve(path);
    }
    boolean folder = selected.getFileType() instanceof FolderFileType;
    String location = VfsLocations.folderToBrowse(path, folder);
    if (StringUtils.isBlank(location)) {
      return;
    }
    openAtPreferredHost(hopGui, location);
  }

  public static boolean isDockOpen(HopGui hopGui) {
    if (hopGui == null) {
      return false;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return false;
    }
    CTabItem item = dock.findToolTab(DOCK_TOOL_ID);
    return item != null && !item.isDisposed();
  }
}
