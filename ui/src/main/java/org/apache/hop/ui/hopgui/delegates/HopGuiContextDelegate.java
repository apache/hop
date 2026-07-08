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

package org.apache.hop.ui.hopgui.delegates;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

public class HopGuiContextDelegate {

  public static final Class<?> PKG = HopGuiContextDelegate.class;
  private HopGui hopGui;

  public HopGuiContextDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  /**
   * Create a new file or metadata object. This pops up a regular drop-down menu, anchored below the
   * "New" toolbar button. The global file types (pipeline, workflow, markdown, ...) are listed at
   * the top; below a separator come all metadata types, grouped and ordered exactly like the "new"
   * button in the metadata perspective so both menus stay in sync.
   */
  public void fileNew() {
    Shell shell = hopGui.getShell();
    Menu menu = new Menu(shell, SWT.POP_UP);

    // 1) Global file types at the top (pipeline, workflow, markdown, ...).
    //
    List<GuiAction> fileActions = new ArrayList<>();
    for (IHopFileType fileType : HopFileTypeRegistry.getInstance().getFileTypes()) {
      for (IGuiContextHandler handler : fileType.getContextHandlers()) {
        fileActions.addAll(
            GuiContextUtil.getInstance()
                .filterActions(handler.getSupportedActions(), GuiActionType.Create));
      }
    }
    fileActions.sort(
        Comparator.comparing((GuiAction a) -> Const.NVL(a.getCategoryOrder(), "9999"))
            .thenComparing(a -> Const.NVL(a.getName(), a.getId())));
    for (GuiAction action : fileActions) {
      addActionMenuItem(menu, action, shell);
    }

    // 2) All metadata types, grouped/ordered exactly like the metadata perspective's "new" button.
    //
    boolean hasFileItems = menu.getItemCount() > 0;
    if (hasFileItems) {
      new MenuItem(menu, SWT.SEPARATOR);
    }
    MetadataPerspective perspective = HopGui.getMetadataPerspective();
    int metadataItems = perspective != null ? perspective.addNewMetadataTypeMenuItems(menu) : 0;
    // Drop a dangling separator when there were no metadata types to add.
    if (metadataItems == 0 && hasFileItems) {
      menu.getItem(menu.getItemCount() - 1).dispose();
    }

    if (menu.getItemCount() == 0) {
      menu.dispose();
      return;
    }

    // Position the menu just below the "New" toolbar button. Fall back to the mouse pointer when
    // the
    // toolbar item can't be located (e.g. triggered via keyboard shortcut before the toolbar
    // built).
    //
    ToolItem toolItem = null;
    if (hopGui.getMainToolbarWidgets() != null) {
      toolItem = hopGui.getMainToolbarWidgets().findToolItem(HopGui.ID_MAIN_TOOLBAR_NEW);
    }
    if (toolItem != null && !toolItem.isDisposed()) {
      ToolBar toolBar = toolItem.getParent();
      Rectangle bounds = toolItem.getBounds();
      menu.setLocation(toolBar.toDisplay(bounds.x, bounds.y + bounds.height));
    } else {
      menu.setLocation(shell.getDisplay().getCursorLocation());
    }

    // Dispose the menu once it is hidden to avoid leaking widgets.
    //
    menu.addListener(SWT.Hide, event -> menu.getDisplay().asyncExec(menu::dispose));

    menu.setVisible(true);
  }

  /** Adds a single push menu item for a {@link GuiAction} (icon, label and its action lambda). */
  private void addActionMenuItem(Menu menu, GuiAction action, Shell shell) {
    MenuItem menuItem = new MenuItem(menu, SWT.PUSH);
    menuItem.setText(Const.NVL(action.getName(), action.getId()));

    // Load the action image (SVG) when there is one.
    //
    if (StringUtils.isNotEmpty(action.getImage())) {
      try {
        ClassLoader classLoader = action.getClassLoader();
        if (classLoader == null) {
          classLoader = getClass().getClassLoader();
        }
        Image image =
            GuiResource.getInstance()
                .getImage(
                    action.getImage(),
                    classLoader,
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);
        menuItem.setImage(image);
      } catch (Exception e) {
        // Ignore image loading errors, the menu item text is enough.
      }
    }

    menuItem.addListener(
        SWT.Selection,
        event -> {
          boolean shiftClicked = (event.stateMask & SWT.SHIFT) != 0;
          boolean ctrlClicked = (event.stateMask & SWT.CONTROL) != 0;
          // Defer execution until the menu is fully closed.
          //
          hopGui
              .getDisplay()
              .asyncExec(
                  () -> {
                    try {
                      IGuiActionLambda<?> actionLambda = action.getActionLambda();
                      actionLambda.executeAction(shiftClicked, ctrlClicked);
                    } catch (Exception e) {
                      new ErrorDialog(shell, "Error", "An error occurred executing action", e);
                    }
                  });
        });
  }

  /** Edit a metadata object... */
  public void fileMetadataEdit() {

    GuiContextUtil.getInstance()
        .handleActionSelection(
            hopGui.getShell(),
            BaseMessages.getString(
                PKG, "HopGuiContextDelegate.SelectElementTypeEdit.Dialog.Header"),
            new Point(0, 0),
            hopGui,
            GuiActionType.Modify,
            "FileMetadataEdit",
            true);
  }

  /** Delete a metadata object... */
  public void fileMetadataDelete() {

    GuiContextUtil.getInstance()
        .handleActionSelection(
            hopGui.getShell(),
            BaseMessages.getString(
                PKG, "HopGuiContextDelegate.SelectElementTypeDelete.Dialog.Header"),
            new Point(0, 0),
            hopGui,
            GuiActionType.Delete,
            "FileMetadataDelete",
            true);
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }
}
