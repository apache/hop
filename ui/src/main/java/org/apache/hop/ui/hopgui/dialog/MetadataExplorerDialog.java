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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

import java.util.Collections;
import java.util.List;

@GuiPlugin
public class MetadataExplorerDialog {
  private static final Class<?> PKG = MetadataExplorerDialog.class; // For Translator

  private static final String METADATA_EXPLORER_DIALOG_TREE = "Metadata explorer dialog tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataExplorerDialog-Toolbar";
  public static final String TOOLBAR_ITEM_NEW = "MetadataExplorerDialog-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_EDIT = "MetadataExplorerDialog-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE =
      "MetadataExplorerDialog-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataExplorerDialog-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataExplorerDialog-Toolbar-10100-Refresh";

  private static ILogChannel log = LogChannel.GENERAL;

  private Shell parent;

  private Shell shell;

  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;

  private Tree tree;

  private PropsUi props;

  private Button closeButton;

  private static MetadataExplorerDialog activeInstance;

  public MetadataExplorerDialog(Shell parent) {
    this.parent = parent;
    props = PropsUi.getInstance();
  }

  public void open() {
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MetadataExplorerDialog.Dialog.Title"));

    int margin = props.getMargin();

    // Create a toolbar at the top of the main composite...
    //
    toolBar = new ToolBar(shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(0, 0);
    layoutData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(layoutData);
    toolBar.pack();

    closeButton = new Button(shell, SWT.PUSH);
    closeButton.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    BaseTransformDialog.positionBottomButtons(
        shell,
        new Button[] {
          closeButton,
        },
        margin,
        null);

    // Add listeners
    closeButton.addListener(SWT.Selection, e -> close());

    tree = new Tree(shell, SWT.SINGLE | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(tree);
    tree.setHeaderVisible(true);
    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment(0, 0); // To the right of the label
    treeFormData.top = new FormAttachment(toolBar, 0);
    treeFormData.right = new FormAttachment(100, 0);
    treeFormData.bottom = new FormAttachment(closeButton, -margin * 2);
    tree.setLayoutData(treeFormData);

    TreeColumn keyColumn = new TreeColumn(tree, SWT.LEFT);
    keyColumn.setText("Object type key (folder)");
    keyColumn.setWidth(400);

    TreeColumn valueColumn = new TreeColumn(tree, SWT.LEFT);
    valueColumn.setText("Description or value");
    valueColumn.setWidth(500);

    tree.addListener(SWT.Selection, e -> getSelectedState());
    tree.addListener(SWT.DefaultSelection, e -> doubleClickAction());
    tree.addListener(SWT.MenuDetect, e -> showMenu());

    // refresh automatically when the metadata changes
    //
    HopGui.getInstance()
        .getEventsHandler()
        .addEventListener(
            getClass().getName(), e -> refreshTree(), HopGuiEvents.MetadataChanged.name());

    TreeMemory.addTreeListener(tree, METADATA_EXPLORER_DIALOG_TREE);

    try {
      refreshTree();

      for (TreeItem item : tree.getItems()) {
        TreeMemory.getInstance().storeExpanded(METADATA_EXPLORER_DIALOG_TREE, item, true);
      }
      TreeMemory.setExpandedFromMemory(tree, METADATA_EXPLORER_DIALOG_TREE);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Unexpected error displaying metadata information", e);
    }

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            close();
          }
        });

    BaseTransformDialog.setSize(shell);

    getSelectedState();
    tree.setFocus();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private void showMenu() {
    try {
      getSelectedState();
      if (activeObjectKey == null) {
        return;
      }

      // Show the menu
      //
      Menu menu = new Menu(tree);

      MenuItem newItem = new MenuItem(menu, SWT.POP_UP);
      newItem.setText("New");
      newItem.addListener(SWT.Selection, e -> newMetadata());

      if (StringUtils.isNotEmpty(activeObjectName)) {

        MenuItem editItem = new MenuItem(menu, SWT.POP_UP);
        editItem.setText("Edit");
        editItem.addListener(SWT.Selection, e -> editMetadata());

        MenuItem duplicateItem = new MenuItem(menu, SWT.POP_UP);
        duplicateItem.setText("Duplicate");
        duplicateItem.addListener(SWT.Selection, e -> duplicateMetadata());

        new MenuItem(menu, SWT.SEPARATOR);

        MenuItem deleteItem = new MenuItem(menu, SWT.POP_UP);
        deleteItem.setText("Delete");
        deleteItem.addListener(SWT.Selection, e -> deleteMetadata());
      }

      tree.setMenu(menu);
      menu.setVisible(true);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error handling metadata object", e);
    }
  }

  private void doubleClickAction() {
    getSelectedState();
    if (StringUtils.isEmpty(activeObjectKey)) {
      return;
    }
    try {
      if (StringUtils.isEmpty(activeObjectName)) {
        newMetadata();
      } else {
        editMetadata();
      }
    } catch (Exception ex) {
      new ErrorDialog(shell, "Error", "Error handling double-click selection event", ex);
    }
  }

  private String activeObjectKey = null;
  private String activeObjectName = null;

  private void getSelectedState() {

    activeObjectKey = null;
    activeObjectName = null;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];

      if (selectedItem != null) {
        if (selectedItem.getParentItem() == null) {
          activeObjectKey = selectedItem.getText();
          activeObjectName = null;
        } else {
          activeObjectKey = selectedItem.getParentItem().getText();
          activeObjectName = selectedItem.getText(1);
        }
      }
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_NEW, StringUtils.isNotEmpty(activeObjectKey));
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, StringUtils.isNotEmpty(activeObjectName));
    toolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_DUPLICATE, StringUtils.isNotEmpty(activeObjectName));
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, StringUtils.isNotEmpty(activeObjectName));
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_REFRESH, true);
  }

  private MetadataManager<IHopMetadata> getActiveMetadataManger() {
    try {
      IHopMetadataProvider metadataProvider = HopGui.getInstance().getMetadataProvider();
      Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(activeObjectKey);
      MetadataManager<IHopMetadata> manager =
          new MetadataManager<>(
              HopGui.getInstance().getVariables(), metadataProvider, metadataClass);
      return manager;
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          "Error",
          "Unexpected error getting the metadata class for key '" + activeObjectKey + "'",
          e);
      return null;
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "New",
      image = "ui/images/new.svg")
  public void newMetadata() {
    MetadataManager<IHopMetadata> manager = getActiveMetadataManger();
    if (manager != null && manager.newMetadata() != null) {
      refreshTree();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT,
      toolTip = "Edit",
      image = "ui/images/edit.svg")
  public void editMetadata() {
    MetadataManager<IHopMetadata> manager = getActiveMetadataManger();
    if (manager != null && manager.editMetadata(activeObjectName)) {
      refreshTree();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "Delete",
      image = "ui/images/delete.svg")
  public void deleteMetadata() {
    MetadataManager<IHopMetadata> manager = getActiveMetadataManger();
    if (manager != null && manager.deleteMetadata(activeObjectName)) {
      refreshTree();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DUPLICATE,
      toolTip = "Create a copy",
      image = "ui/images/copy.svg")
  public void duplicateMetadata() {
    MetadataManager<IHopMetadata> manager = getActiveMetadataManger();
    if (manager != null && activeObjectName != null) {
      try {
        IHopMetadata metadata = manager.loadElement(activeObjectName);

        int copyNr = 2;
        while (true) {
          String newName = activeObjectName + " " + copyNr;
          if (!manager.getSerializer().exists(newName)) {
            metadata.setName(newName);
            manager.getSerializer().save(metadata);
            refreshTree();
            manager.editMetadata(newName);
            break;
          } else {
            copyNr++;
          }
        }
        refreshTree();
      } catch (Exception e) {
        new ErrorDialog(shell, "Error", "Error duplicating metadata", e);
      }
    }
  }

  /**
   * Gets activeInstance
   *
   * @return value of activeInstance
   */
  public static MetadataExplorerDialog getInstance() {
    return activeInstance;
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();

    // Get rid of the listener we registered...
    //
    HopGui.getInstance().getEventsHandler().removeEventListeners(getClass().getName());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "Refresh",
      image = "ui/images/refresh.svg")
  public void refreshTree() {
    try {
      tree.removeAll();

      IHopMetadataProvider metadataProvider = HopGui.getInstance().getMetadataProvider();

      // top level: object key
      //
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      for (Class<IHopMetadata> metadataClass : metadataClasses) {
        HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
        Image image =
            SwtSvgImageUtil.getImage(
                shell.getDisplay(),
                metadataClass.getClassLoader(),
                hopMetadata.image(),
                ConstUi.ICON_SIZE,
                ConstUi.ICON_SIZE);

        TreeItem elementTypeItem = new TreeItem(tree, SWT.NONE);
        elementTypeItem.setImage(image);

        elementTypeItem.setText(0, Const.NVL(hopMetadata.key(), ""));
        elementTypeItem.setText(1, Const.NVL(hopMetadata.name(), ""));

        // level 1: object names
        //
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = serializer.listObjectNames();
        Collections.sort(names);

        for (final String name : names) {
          TreeItem elementItem = new TreeItem(elementTypeItem, SWT.NONE);
          elementItem.setText(1, Const.NVL(name, ""));
          elementItem.addListener(SWT.Selection, event -> log.logBasic("Selected : " + name));
          elementItem.setFont(GuiResource.getInstance().getFontBold());
        }
      }

      TreeUtil.setOptimalWidthOnColumns(tree);
      TreeMemory.setExpandedFromMemory(tree, METADATA_EXPLORER_DIALOG_TREE);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error refreshing metadata tree", e);
    }
  }
}
