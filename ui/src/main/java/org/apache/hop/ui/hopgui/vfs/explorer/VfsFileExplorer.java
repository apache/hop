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

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/**
 * VFS file explorer shared by the floating window and the bottom dock. Location tabs belong to this
 * host; the window and the dock do not share them.
 */
@GuiPlugin
public class VfsFileExplorer extends Composite {

  public static final Class<?> PKG = VfsFileExplorer.class;

  public static final String HOST_TOOLBAR_PARENT_ID = "VfsFileExplorer-HostToolbar";
  private static final String HOST_NEW_TAB = "VfsFileExplorer-Host-0010-NewTab";
  private static final String HOST_DOCK = "VfsFileExplorer-Host-0100-Dock";
  private static final String HOST_UNDOCK = "VfsFileExplorer-Host-0110-Undock";

  @Getter private final HopGui hopGui;
  @Getter private final IVariables variables;
  @Getter private final VfsExplorerViewState viewState;
  private final Map<String, String> bookmarks = new HashMap<>();
  private final CTabFolder tabFolder;
  private final SashForm sash;
  private final VfsExplorerOperationsPanel operationsPanel;
  private final GuiToolbarWidgets hostToolbar;
  private final Control hostToolbarControl;

  public VfsFileExplorer(Composite parent, HopGui hopGui) {
    super(parent, SWT.NONE);
    this.hopGui = hopGui;
    IVariables hopVariables = hopGui == null ? null : hopGui.getVariables();
    this.variables = hopVariables == null ? new Variables() : hopVariables;
    this.viewState = VfsExplorerViewState.load();
    this.bookmarks.putAll(VfsBookmarks.load());
    PropsUi.setLook(this);
    setLayout(new FormLayout());

    IToolbarContainer hostBar =
        ToolbarFacade.createToolbarContainer(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    hostToolbarControl = hostBar.getControl();
    hostToolbarControl.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(hostToolbarControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    hostToolbar = new GuiToolbarWidgets();
    hostToolbar.registerGuiPluginObject(this);
    hostToolbar.createToolbarWidgets(hostBar, HOST_TOOLBAR_PARENT_ID);
    hostToolbarControl.pack();

    sash = new SashForm(this, SWT.VERTICAL);
    tabFolder = new CTabFolder(sash, SWT.MULTI | SWT.BORDER);
    PropsUi.setLook(tabFolder, PropsUi.WIDGET_STYLE_TAB);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            if (tabFolder.getItemCount() <= 1) {
              event.doit = false;
            }
          }
        });
    tabFolder.addListener(SWT.Selection, e -> activate());

    operationsPanel = new VfsExplorerOperationsPanel(sash, this, this);
    operationsPanel.setExpandedListener(this::layoutOperations);
    sash.setWeights(80, 20);
    layoutOperations(false);

    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);
    addDisposeListener(
        e -> {
          cancelListings();
          HopGuiKeyHandler.getInstance().removeParentObjectToHandle(this);
          hostToolbar.dispose();
          viewState.save();
        });
  }

  /** Open {@code location}, or the user home folder when it is blank. Focus an existing tab. */
  public void openLocation(String location) {
    String target =
        StringUtils.isBlank(location) ? System.getProperty("user.home") : location.trim();
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData() instanceof VfsFileExplorerLocation tab
          && sameLocation(tab.getLocationText(), target)) {
        tabFolder.setSelection(item);
        return;
      }
    }
    newTab(target);
  }

  private void newTab(String target) {
    CTabItem item = new CTabItem(tabFolder, SWT.CLOSE);
    VfsFileExplorerLocation tab = new VfsFileExplorerLocation(tabFolder, this);
    item.setControl(tab);
    item.setData(tab);
    tab.setTabItem(item);
    item.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Tab.Home"));
    tabFolder.setSelection(item);
    tab.navigateTo(target, true);
  }

  public void activate() {
    VfsFileExplorers.activate(this);
  }

  public Map<String, String> bookmarks() {
    return bookmarks;
  }

  public VfsExplorerOperation beginOperation(String description, String location) {
    return operationsPanel.addOperation(description, location);
  }

  public void refreshOperations() {
    if (!operationsPanel.isDisposed()) {
      operationsPanel.refresh();
    }
  }

  public void cancelListings() {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData() instanceof VfsFileExplorerLocation location) {
        location.invalidate();
      }
    }
    operationsPanel.cancelRunning();
  }

  public void putBookmark(String name, String uri) {
    bookmarks.put(name, uri);
    storeBookmarks();
    refreshBookmarkLists();
  }

  public void removeBookmark(String name) {
    bookmarks.remove(name);
    storeBookmarks();
    refreshBookmarkLists();
  }

  @GuiToolbarElement(
      root = HOST_TOOLBAR_PARENT_ID,
      id = HOST_NEW_TAB,
      toolTip = "i18n::VfsFileExplorer.Toolbar.NewTab.Tooltip",
      image = "ui/images/add.svg")
  public void newTab() {
    newTab(System.getProperty("user.home"));
  }

  @GuiToolbarElement(
      root = HOST_TOOLBAR_PARENT_ID,
      id = HOST_DOCK,
      toolTip = "i18n::VfsFileExplorer.Toolbar.Dock.Tooltip",
      image = "ui/images/dock-panel.svg",
      separator = true)
  public void openInBottomDock() {
    if (hopGui == null) {
      return;
    }
    VfsFileExplorerViews.openDock(hopGui, activeLocationText());
  }

  @GuiToolbarElement(
      root = HOST_TOOLBAR_PARENT_ID,
      id = HOST_UNDOCK,
      toolTip = "i18n::VfsFileExplorer.Toolbar.Undock.Tooltip",
      image = "ui/images/detach-panel.svg")
  public void openFloatingWindow() {
    if (hopGui == null) {
      return;
    }
    VfsFileExplorerDialog.openAt(hopGui, activeLocationText());
  }

  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refreshActive() {
    VfsFileExplorerLocation location = activeLocation();
    if (location != null) {
      location.refreshFolder();
    }
  }

  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void renameActive() {
    VfsFileExplorerLocation location = activeLocation();
    if (location != null) {
      location.renameSelected();
    }
  }

  private void layoutOperations(boolean expanded) {
    if (sash.isDisposed()) {
      return;
    }
    Composite statusBar = operationsPanel.getStatusBar();
    if (expanded) {
      statusBar.setVisible(false);
      statusBar.setLayoutData(new FormDataBuilder().left().bottom().height(0).width(0).result());
      sash.setLayoutData(
          new FormDataBuilder().top(hostToolbarControl, 0).bottom().fullWidth().result());
      sash.setMaximizedControl(null);
    } else {
      statusBar.setVisible(true);
      statusBar.setLayoutData(new FormDataBuilder().bottom().fullWidth().result());
      sash.setLayoutData(
          new FormDataBuilder()
              .top(hostToolbarControl, 0)
              .bottom(statusBar, 0)
              .fullWidth()
              .result());
      sash.setMaximizedControl(tabFolder);
    }
    layout(true, true);
  }

  private VfsFileExplorerLocation activeLocation() {
    if (tabFolder.isDisposed()) {
      return null;
    }
    CTabItem item = tabFolder.getSelection();
    if (item == null || item.isDisposed()) {
      return null;
    }
    Object data = item.getData();
    return data instanceof VfsFileExplorerLocation location ? location : null;
  }

  private String activeLocationText() {
    VfsFileExplorerLocation location = activeLocation();
    return location == null ? null : location.getLocationText();
  }

  private void refreshBookmarkLists() {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData() instanceof VfsFileExplorerLocation location) {
        location.refreshBookmarks();
      }
    }
  }

  private void storeBookmarks() {
    try {
      VfsBookmarks.save(bookmarks);
    } catch (HopException e) {
      LogChannel.UI.logError("Error saving VFS bookmarks", e);
      VfsExplorerOperation operation =
          beginOperation(BaseMessages.getString(PKG, "VfsFileExplorer.Error.Bookmark"), "");
      operation.fail(Const.NVL(e.getMessage(), e.toString()), Const.getClassicStackTrace(e));
      refreshOperations();
    }
  }

  static boolean sameLocation(String left, String right) {
    return stripTrailingSlash(left).equals(stripTrailingSlash(right));
  }

  private static String stripTrailingSlash(String value) {
    if (value == null) {
      return "";
    }
    String stripped = value;
    while (stripped.length() > 1 && (stripped.endsWith("/") || stripped.endsWith("\\"))) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }
}
