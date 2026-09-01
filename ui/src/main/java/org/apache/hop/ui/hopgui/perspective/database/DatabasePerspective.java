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

package org.apache.hop.ui.hopgui.perspective.database;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * Thin {@link IHopPerspective} host around {@link DatabaseWorkbench}. Later hosts (floating dialog,
 * dock tab) implement {@link IDatabaseWorkbenchHost} the same way.
 */
@HopPerspectivePlugin(
    id = "170-HopDatabasePerspective",
    name = "i18n::DatabasePerspective.Name",
    description = "i18n::DatabasePerspective.Description",
    image = "ui/images/database.svg",
    documentationUrl = "/hop-gui/perspective-database.html")
@GuiPlugin(
    name = "i18n::DatabasePerspective.Name",
    description = "i18n::DatabasePerspective.GuiPlugin.Description")
public class DatabasePerspective implements IHopPerspective, TabClosable, IDatabaseWorkbenchHost {

  public static final Class<?> PKG = DatabasePerspective.class;

  private static DatabasePerspective instance;

  private HopGui hopGui;
  private DatabaseWorkbench workbench;

  public DatabasePerspective() {
    instance = this;
  }

  public static DatabasePerspective getInstance() {
    try {
      DatabasePerspective fromGui = HopGui.findSessionPerspective(DatabasePerspective.class);
      if (fromGui != null) {
        return fromGui;
      }
    } catch (Throwable e) {
      // No HopGui in unit tests
    }
    return instance;
  }

  private boolean isInitialized() {
    return hopGui != null && workbench != null && !workbench.isDisposed();
  }

  @Override
  public String getId() {
    return "database-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'd', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'd', global = true)
  @Override
  public void activate() {
    if (!isInitialized()) {
      return;
    }
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    if (!isInitialized()) {
      return;
    }
    updateGui(workbench.getActiveFileTypeHandler());
  }

  @Override
  public boolean isActive() {
    return isInitialized() && hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;
    workbench = new DatabaseWorkbench(parent, this);
    workbench.setLayoutData(new FormDataBuilder().fullSize().result());

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    keyHandler.addParentObjectToHandle(workbench);
    hopGui.replaceKeyboardShortcutListeners(workbench, keyHandler);
  }

  @Override
  public Control getControl() {
    return workbench;
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    if (!isInitialized()) {
      return List.of();
    }
    return List.of(workbench.getSqlFileType());
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    if (!isInitialized()) {
      return new EmptyHopFileTypeHandler();
    }
    return workbench.getActiveFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    if (isInitialized()) {
      workbench.setActiveFileTypeHandler(fileTypeHandler);
    }
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (!isInitialized()) {
      return false;
    }
    return workbench.remove(typeHandler);
  }

  @Override
  public List<TabItemHandler> getItems() {
    if (!isInitialized()) {
      return List.of();
    }
    return workbench.getItems();
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    if (isInitialized()) {
      workbench.closeTab(event, tabItem);
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return isInitialized() ? workbench.getTabFolder() : null;
  }

  @Override
  public void clearSearchFilters() {
    if (isInitialized()) {
      workbench.clearSearchFilter();
    }
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  public void openSqlFile(String filename, DatabaseMeta connection, String buffer, boolean dirty) {
    if (!isInitialized()) {
      return;
    }
    activate();
    workbench.openSqlFile(filename, connection, buffer, dirty);
  }

  public List<String> connectionNames() {
    if (!isInitialized()) {
      return List.of();
    }
    return workbench.connectionNames();
  }

  public DatabaseMeta findConnection(String name) {
    if (!isInitialized()) {
      return null;
    }
    return workbench.findConnection(name);
  }

  @Override
  public HopGui getHopGui() {
    return hopGui;
  }

  @Override
  public Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public Display getDisplay() {
    return hopGui.getDisplay();
  }

  @Override
  public IVariables getVariables() {
    return hopGui.getVariables();
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return hopGui.getMetadataProvider();
  }

  @Override
  public ILoggingObject getLoggingObject() {
    return hopGui.getLoggingObject();
  }

  @Override
  public void asyncExec(Runnable runnable) {
    Display display = hopGui.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (workbench == null || workbench.isDisposed()) {
            return;
          }
          runnable.run();
        });
  }

  @Override
  public void updateGui(IHopFileTypeHandler handler) {
    if (hopGui == null || handler == null) {
      return;
    }
    hopGui.handleFileCapabilities(
        handler.getFileType(), handler, handler.hasChanged(), false, false);
  }
}
