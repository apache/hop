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

package org.apache.hop.ui.hopgui.perspective.metadata;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.FileSystemNode;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataFileType;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@HopPerspectivePlugin(
    id = "200-HopMetadataPerspective",
    name = "i18n::MetadataPerspective.Name",
    description = "i18n::MetadataPerspective.Description",
    image = "ui/images/metadata.svg")
@GuiPlugin(description = "i18n::MetadataPerspective.GuiPlugin.Description")
public class MetadataPerspective implements IHopPerspective, TabClosable {

  public static final Class<?> PKG = MetadataPerspective.class; // i18n
  private static final String METADATA_PERSPECTIVE_TREE = "Metadata perspective tree";

  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "MetadataPerspective-Toolbar";
  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_NEW = "MetadataPerspective-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_CREATE_FOLDER =
      "MetadataPerspective-Toolbar-10010-Create-Folder";
  public static final String TOOLBAR_ITEM_EDIT = "MetadataPerspective-Toolbar-10020-Edit";
  public static final String TOOLBAR_ITEM_RENAME = "MetadataPerspective-Toolbar-10030-Rename";
  public static final String TOOLBAR_ITEM_DUPLICATE = "MetadataPerspective-Toolbar-10040-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataPerspective-Toolbar-10050-Delete";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataPerspective-Toolbar-10100-Refresh";

  public static final String KEY_HELP = "Help";
  private static final String SUBFOLDERS_ENABLED = "SubFoldersEnabled";

  private static final String CONTEXT_MENU_NEW = "MetadataPerspective-ContextMenu-10000-New";
  private static final String CONTEXT_MENU_CREATE_FOLDER =
      "MetadataPerspective-ContextMenu-10010-Create-Folder";
  private static final String CONTEXT_MENU_EDIT = "MetadataPerspective-ContextMenu-10020-Edit";
  public static final String CONTEXT_MENU_RENAME = "MetadataPerspective-ContextMenu-10030-Rename";
  private static final String CONTEXT_MENU_DUPLICATE =
      "MetadataPerspective-ContextMenu-10040-Duplicate";
  private static final String CONTEXT_MENU_DELETE = "MetadataPerspective-ContextMenu-10050-Delete";
  private static final String CONTEXT_MENU_HELP = "MetadataPerspective-ContextMenu-10050-Help";
  public static final String PATH = "path";
  public static final String CONST_FOLDER = "FOLDER";
  public static final String CONST_ERROR = "Error";

  private static MetadataPerspective instance;

  public static MetadataPerspective getInstance() {
    return instance;
  }

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;
  private GuiMenuWidgets menuWidgets;

  private List<MetadataEditor<?>> editors = new ArrayList<>();

  private final MetadataFileType metadataFileType;

  public MetadataPerspective() {
    instance = this;

    this.metadataFileType = new MetadataFileType();
  }

  @Override
  public String getId() {
    return "metadata-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'm')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'm')
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    this.refresh();
    this.updateSelection();
    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return Arrays.asList(metadataFileType);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    // Split tree and editor
    //
    sash = new SashForm(parent, SWT.HORIZONTAL);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(0, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    sash.setLayoutData(fdSash);

    createTree(sash);
    createTabFolder(sash);

    sash.setWeights(20, 80);

    this.refresh();
    this.updateSelection();

    // Set the top level items in the tree to be expanded
    //
    for (TreeItem item : tree.getItems()) {
      TreeMemory.getInstance().storeExpanded(METADATA_PERSPECTIVE_TREE, item, true);
    }

    // refresh the metadata when it changes.
    //
    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName(), e -> refresh(), HopGuiEvents.MetadataChanged.name());

    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);
  }

  protected MetadataManager<IHopMetadata> getMetadataManager(String objectKey) throws HopException {
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
    return new MetadataManager<>(
        HopGui.getInstance().getVariables(), metadataProvider, metadataClass, hopGui.getShell());
  }

  protected void createTree(Composite parent) {
    // Create composite
    //
    Composite composite = new Composite(parent, SWT.BORDER);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    composite.setLayout(layout);

    // Create toolbar
    //
    toolBar = new ToolBar(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(0, 0);
    layoutData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(layoutData);
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    tree = new Tree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    tree.addListener(SWT.Selection, event -> this.updateSelection());
    tree.addListener(
        SWT.KeyUp,
        event -> {
          if (event.keyCode == SWT.DEL) {
            onDeleteMetadata();
          }
        });
    tree.addListener(
        SWT.DefaultSelection,
        event -> {
          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null) {
            if (treeItem.getParentItem() == null) {
              onNewMetadata();
            } else {
              onEditMetadata();
            }
          }
        });
    PropsUi.setLook(tree);
    Menu menu = new Menu(tree);
    menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, getShell(), menu);

    tree.setMenu(menu);

    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment(0, 0);
    treeFormData.top = new FormAttachment(toolBar, 0);
    treeFormData.right = new FormAttachment(100, 0);
    treeFormData.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(treeFormData);

    // Create Tree editor for rename
    treeEditor = new TreeEditor(tree);
    treeEditor.horizontalAlignment = SWT.LEFT;
    treeEditor.grabHorizontal = true;

    // Remember tree node expanded/Collapsed
    TreeMemory.addTreeListener(tree, METADATA_PERSPECTIVE_TREE);
  }

  protected void createTabFolder(Composite parent) {

    tabFolder = new CTabFolder(parent, SWT.MULTI | SWT.BORDER);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            onTabClose(event);
          }
        });
    tabFolder.addListener(SWT.Selection, event -> updateGui());
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    // Show/Hide tree
    //
    ToolBar tabToolBar = new ToolBar(tabFolder, SWT.FLAT);
    final ToolItem item = new ToolItem(tabToolBar, SWT.PUSH);
    item.setImage(GuiResource.getInstance().getImageMinimizePanel());
    item.addListener(
        SWT.Selection,
        e -> {
          if (sash.getMaximizedControl() == null) {
            sash.setMaximizedControl(tabFolder);
            item.setImage(GuiResource.getInstance().getImageMaximizePanel());
          } else {
            sash.setMaximizedControl(null);
            item.setImage(GuiResource.getInstance().getImageMinimizePanel());
          }
        });
    tabFolder.setTopRight(tabToolBar, SWT.RIGHT);

    new TabCloseHandler(this);

    // Support reorder tab item
    //
    new TabFolderReorder(tabFolder);
  }

  public void addEditor(MetadataEditor<?> editor) {

    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(editor.getTitle());
    tabItem.setImage(editor.getTitleImage());
    tabItem.setToolTipText(editor.getTitleToolTip());

    // Create composite for editor and buttons
    //
    Composite composite = new Composite(tabFolder, SWT.NONE);
    FormLayout layoutComposite = new FormLayout();
    layoutComposite.marginWidth = PropsUi.getFormMargin();
    layoutComposite.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layoutComposite);
    PropsUi.setLook(composite);

    // Create buttons
    Button[] buttons = editor.createButtonsForButtonBar(composite);
    if (buttons != null) {
      BaseTransformDialog.positionBottomButtons(composite, buttons, PropsUi.getMargin(), null);
    }

    // Create editor content area
    //
    Composite area = new Composite(composite, SWT.NONE);
    FormLayout layoutArea = new FormLayout();
    layoutArea.marginWidth = 0;
    layoutArea.marginHeight = 0;
    area.setLayout(layoutArea);
    FormData fdArea = new FormData();
    fdArea.left = new FormAttachment(0, 0);
    fdArea.top = new FormAttachment(0, 0);
    fdArea.right = new FormAttachment(100, 0);
    if (buttons != null) {
      fdArea.bottom = new FormAttachment(buttons[0], -PropsUi.getMargin());
    } else {
      fdArea.bottom = new FormAttachment(100, -PropsUi.getMargin());
    }

    area.setLayoutData(fdArea);
    PropsUi.setLook(area);

    // Create editor controls
    //
    editor.createControl(area);

    tabItem.setControl(composite);
    tabItem.setData(editor);

    editors.add(editor);

    // Add listeners
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    HopGui.getInstance().replaceKeyboardShortcutListeners(this.getShell(), keyHandler);

    // Activate perspective
    //
    this.activate();

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    editor.setFocus();
  }

  /**
   * Find a metadata editor
   *
   * @param objectKey the metadata annotation key
   * @param name the name of the metadata
   * @return the metadata editor or null if not found
   */
  public MetadataEditor<?> findEditor(String objectKey, String name) {
    if (objectKey == null || name == null) return null;

    for (MetadataEditor<?> editor : editors) {
      IHopMetadata metadata = editor.getMetadata();
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadata.getClass());
      if (annotation != null
          && annotation.key().equals(objectKey)
          && name.equals(metadata.getName())) {
        return editor;
      }
    }
    return null;
  }

  public void setActiveEditor(MetadataEditor<?> editor) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(editor)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        editor.setFocus();

        HopGui.getInstance()
            .handleFileCapabilities(metadataFileType, editor.hasChanged(), false, false);
      }
    }
  }

  public MetadataEditor<?> getActiveEditor() {
    if (tabFolder.getSelectionIndex() < 0) {
      return null;
    }

    return (MetadataEditor<?>) tabFolder.getSelection().getData();
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    MetadataEditor<?> editor = getActiveEditor();
    if (editor != null) {
      return editor;
    }

    // If all editor are closed
    //
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler instanceof MetadataEditor) {
      this.setActiveEditor((MetadataEditor<?>) fileTypeHandler);
    }
  }

  protected void onTabClose(CTabFolderEvent event) {
    CTabItem tabItem = (CTabItem) event.item;
    closeTab(event, tabItem);
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_NEW,
      label = "i18n::MetadataPerspective.Menu.New.Label",
      toolTip = "i18n::MetadataPerspective.Menu.New.Tooltip",
      image = "ui/images/new.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.New.Tooltip",
      image = "ui/images/new.svg")
  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void onNewMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null) {
      String objectKey;
      if (treeItem.getParentItem() == null) {
        objectKey = (String) treeItem.getData();
      } else {
        objectKey = getMetadataClassKey(treeItem.getParentItem());
      }

      try {
        IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
        Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
        MetadataManager<IHopMetadata> manager =
            new MetadataManager<>(
                HopGui.getInstance().getVariables(),
                metadataProvider,
                metadataClass,
                hopGui.getShell());

        manager.newMetadataWithEditor(getMetadataObjectPath(treeItem));

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_CREATE_FOLDER,
      label = "i18n::MetadataPerspective.Menu.NewFolder.Label",
      toolTip = "i18n::MetadataPerspective.Menu.NewFolder.Tooltip",
      image = "ui/images/folder-add.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CREATE_FOLDER,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.NewFolder.Tooltip",
      image = "ui/images/folder-add.svg")
  @GuiKeyboardShortcut(key = SWT.F3)
  @GuiOsxKeyboardShortcut(key = SWT.F3)
  public void onNewFolder() {
    String rootFolder = hopGui.getVariables().getVariable("HOP_METADATA_FOLDER");
    if (!rootFolder.endsWith(File.separator) && !rootFolder.endsWith("\\")) {
      rootFolder += File.separator;
    }
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];
    if (Boolean.FALSE.equals(Boolean.valueOf(item.getData(SUBFOLDERS_ENABLED).toString()))) {
      new ErrorDialog(
          getShell(),
          CONST_ERROR,
          "Could not create subfolder for this metadata category",
          new HopException("Could not create subfolder for this metadata category"));
      return;
    }
    String tif = getMetadataObjectPath(item, false);
    if (tif == null) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Message", tif));
    String folder = dialog.open();
    if (folder != null) {
      String newPath = tif;
      if (!newPath.endsWith("/") && !newPath.endsWith("\\")) {
        newPath += "/";
      }
      newPath += folder;
      try {
        FileObject newFolder = HopVfs.getFileObject(newPath);
        newFolder.createFolder();

        refresh();
      } catch (Throwable e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "ExplorerPerspective.Error.CreateFolder.Header"),
            BaseMessages.getString(PKG, "ExplorerPerspective.Error.CreateFolder.Message", newPath),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_EDIT,
      label = "i18n::MetadataPerspective.Menu.Edit.Label",
      toolTip = "i18n::MetadataPerspective.Menu.Edit.Tooltip",
      image = "ui/images/edit.svg",
      separator = true)
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Edit.Tooltip",
      image = "ui/images/edit.svg")
  public void onEditMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getParentItem() != null) {
      String objectKey = getMetadataClassKey(treeItem.getParentItem());
      String objectName = getMetadataObjectName(treeItem);

      MetadataEditor<?> editor = this.findEditor(objectKey, objectName);
      if (editor != null) {
        this.setActiveEditor(editor);
      } else {
        try {
          MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
          manager.editWithEditor(objectName);

          hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
        } catch (Exception e) {
          new ErrorDialog(getShell(), CONST_ERROR, "Error editing metadata", e);
        }
      }

      this.updateSelection();
    }
  }

  private static String getMetadataClassKey(TreeItem treeItem) {
    while (treeItem.getParentItem() != null) {
      return getMetadataClassKey(treeItem.getParentItem());
    }
    return (String) treeItem.getData();
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_RENAME,
      label = "i18n::MetadataPerspective.Menu.Rename.Label",
      toolTip = "i18n::MetadataPerspective.Menu.Rename.Tooltip",
      image = "ui/images/rename.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RENAME,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Edit.Tooltip",
      image = "ui/images/rename.svg")
  public void onRenameMetadata() {

    if (tree.getSelectionCount() < 1) {
      return;
    }

    // Identify the selected item
    TreeItem item = tree.getSelection()[0];
    if (item != null) {
      if (item.getParentItem() == null) return;
      String objectKey = (String) item.getParentItem().getData();

      // The control that will be the editor must be a child of the Tree
      Text text = new Text(tree, SWT.BORDER);
      text.setText(item.getText());
      text.addListener(SWT.FocusOut, event -> text.dispose());
      text.addListener(
          SWT.KeyUp,
          event -> {
            switch (event.keyCode) {
              case SWT.CR, SWT.KEYPAD_CR:
                // If name changed
                if (!item.getText().equals(text.getText())) {
                  try {
                    MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
                    if (manager.rename(item.getText(), text.getText())) {
                      item.setText(text.getText());
                      text.dispose();
                    }
                  } catch (Exception e) {
                    new ErrorDialog(
                        getShell(),
                        BaseMessages.getString(
                            PKG, "MetadataPerspective.EditMetadata.Error.Header"),
                        BaseMessages.getString(
                            PKG, "MetadataPerspective.EditMetadata.Error.Message"),
                        e);
                  }
                }
                break;
              case SWT.ESC:
                text.dispose();
                break;
            }
          });
      text.selectAll();
      text.setFocus();
      treeEditor.setEditor(text, item);

      try {
        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
      } catch (HopException e) {
        throw new RuntimeException("Error fire metadata changed event", e);
      }
    }
  }

  private String getMetadataObjectName(TreeItem treeItem) {
    return getMetadataObjectName(treeItem, false);
  }

  private String getMetadataObjectName(TreeItem treeItem, Boolean showMetadataClassFolder) {
    if (treeItem.getData(PATH).toString().endsWith(".json")) {
      return treeItem.getData(PATH).toString();
    }
    String path = treeItem.getData(PATH).toString();
    TreeItem parent = treeItem.getParentItem();
    if (Boolean.TRUE.equals(showMetadataClassFolder)) {
      while (parent != null) {
        if (parent.getParentItem() == null) {
          path = parent.getData() + File.separator + path;
        } else {
          path = parent.getText(0) + File.separator + path;
        }

        parent = parent.getParentItem();
      }
    } else {
      while (parent != null && parent.getParentItem() != null) {
        path = parent.getText(0) + File.separator + path;
        parent = parent.getParentItem();
      }
    }
    return path;
  }

  private String getMetadataObjectPath(TreeItem treeItem) {
    return getMetadataObjectPath(treeItem, false);
  }

  // THIS should return only a relative path
  private String getMetadataObjectPath(TreeItem treeItem, Boolean showMetadataClassFolder) {
    String path = treeItem.getData(PATH).toString();
    TreeItem parent = treeItem.getParentItem();
    if (Boolean.TRUE.equals(showMetadataClassFolder)) {
      while (parent != null) {
        if (parent.getParentItem() == null) {
          path = parent.getData() + File.separator + path;
        } else {
          path = parent.getText(0) + File.separator + path;
        }

        parent = parent.getParentItem();
      }
    } else {
      while (parent != null && parent.getParentItem() != null) {
        if (!path.startsWith(parent.getData(PATH).toString())) {
          path = parent.getData(PATH) + File.separator + path;
        }
        parent = parent.getParentItem();
      }
    }

    return path;
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DUPLICATE,
      label = "i18n::MetadataPerspective.Menu.Duplicate.Label",
      image = "ui/images/duplicate.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DUPLICATE,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.CreateCopy.Tooltip",
      image = "ui/images/duplicate.svg")
  public void duplicateMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getParentItem() != null) {
      String objectKey = getMetadataClassKey(treeItem.getParentItem());
      String objectName = getMetadataObjectName(treeItem);

      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        IHopMetadata metadata = manager.loadElement(objectName);

        int copyNr = 2;
        while (true) {
          String newName = objectName + " " + copyNr;
          if (!manager.getSerializer().exists(newName)) {
            metadata.setName(newName);
            manager.getSerializer().save(metadata);
            break;
          } else {
            copyNr++;
          }
        }
        refresh();

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "MetadataPerspective.DuplicateMetadata.Error.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.DuplicateMetadata.Error.Message"),
            e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DELETE,
      label = "i18n::MetadataPerspective.Menu.Delete.Label",
      toolTip = "i18n::MetadataPerspective.Menu.Delete.Tooltip",
      image = "ui/images/delete.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Delete.Tooltip",
      image = "ui/images/delete.svg")
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void onDeleteMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getData("type") != null) {
      String objectKey = getMetadataClassKey(treeItem.getParentItem());
      String objectName = getMetadataObjectName(treeItem);

      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        manager.deleteMetadata(objectName);

        refresh();
        updateSelection();

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataDeleted.name());
      } catch (Exception e) {
        new ErrorDialog(getShell(), CONST_ERROR, "Error delete metadata", e);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_HELP,
      label = "i18n::MetadataPerspective.Menu.Help.Label",
      toolTip = "i18n::MetadataPerspective.Menu.Help.Tooltip",
      image = "ui/images/help.svg",
      separator = true)
  public void onHelpMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }
    String objectKey = null;
    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null) {
      if (treeItem.getParentItem() != null) {
        treeItem = treeItem.getParentItem();
      }
      objectKey = (String) treeItem.getData();
    }

    if (objectKey != null) {
      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        HopMetadata annotation = manager.getManagedClass().getAnnotation(HopMetadata.class);
        IPlugin plugin =
            PluginRegistry.getInstance().getPlugin(MetadataPluginType.class, annotation.key());
        HelpUtils.openHelp(getShell(), plugin);
      } catch (Exception ex) {
        new ErrorDialog(getShell(), CONST_ERROR, "Error opening URL", ex);
      }
    }
  }

  public void updateEditor(MetadataEditor<?> editor) {

    if (editor == null) return;

    // Update TabItem
    //
    for (CTabItem item : tabFolder.getItems()) {
      if (editor.equals(item.getData())) {
        item.setText(editor.getTitle());
        if (editor.hasChanged()) item.setFont(GuiResource.getInstance().getFontBold());
        else item.setFont(tabFolder.getFont());
        break;
      }
    }

    // Update TreeItem
    //
    this.refresh();

    // Update HOP GUI menu and toolbar...
    //
    this.updateGui();
  }

  /** Update HopGui menu and toolbar... */
  public void updateGui() {
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    activeHandler.updateGui();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    try {
      tree.setRedraw(false);
      tree.removeAll();

      // top level: object key
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      // Sort by name
      Collections.sort(
          metadataClasses,
          (cl1, cl2) -> {
            HopMetadata a1 = HopMetadataUtil.getHopMetadataAnnotation(cl1);
            HopMetadata a2 = HopMetadataUtil.getHopMetadataAnnotation(cl2);
            return a1.name().compareTo(a2.name());
          });
      // Iterate over metadataClass, meaning all the elements at the top level in the metadata tree
      // (Schema Definitions, Database, Neo4j, Mongo....
      for (Class<IHopMetadata> metadataClass : metadataClasses) {
        HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
        Image image =
            GuiResource.getInstance()
                .getImage(
                    annotation.image(),
                    metadataClass.getClassLoader(),
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);

        TreeItem classItem = new TreeItem(tree, SWT.NONE);
        classItem.setText(
            0, Const.NVL(TranslateUtil.translate(annotation.name(), metadataClass), ""));
        classItem.setImage(image);
        classItem.setExpanded(true);
        classItem.setData(annotation.key());
        classItem.setData(KEY_HELP, annotation.description());
        classItem.setData("type", CONST_FOLDER);
        classItem.setData(SUBFOLDERS_ENABLED, Boolean.valueOf(annotation.subfoldersEnabled()));

        // level 1: object names: folders and definitions
        //
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        final FileSystemNode metadataItemRoot = serializer.getFileSystemTree();
        classItem.setData(PATH, metadataItemRoot.getChildren().get(0).getPath());

        appendChildren(classItem, metadataItemRoot.getChildren().get(0), annotation);
      }

      TreeUtil.setOptimalWidthOnColumns(tree);
      TreeMemory.setExpandedFromMemory(tree, METADATA_PERSPECTIVE_TREE);

      tree.setRedraw(true);

      updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Header"),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Message"),
          e);
    }
  }

  private void appendChildren(
      TreeItem parentItem, FileSystemNode rootNode, HopMetadata annotation) {
    for (final FileSystemNode node : rootNode.getChildren()) {
      TreeItem item = new TreeItem(parentItem, SWT.NONE);
      item.setText(0, Const.NVL(node.getName(), ""));
      item.setData(node.getName());
      item.setData(PATH, node.getPath());
      item.setData(SUBFOLDERS_ENABLED, item.getParentItem().getData(SUBFOLDERS_ENABLED));
      switch (node.getType()) {
        case FILE:
          item.setData("type", "FILE");
          break;
        case FOLDER:
          item.setImage(GuiResource.getInstance().getImage("ui/images/folder.svg"));
          item.setData("type", CONST_FOLDER);
          appendChildren(item, node, annotation);
          break;
        default:
          break;
      }
      MetadataEditor<?> editor = this.findEditor(annotation.key(), node.getName());
      if (editor != null && editor.hasChanged()) {
        item.setFont(GuiResource.getInstance().getFontBold());
      }
    }
  }

  private enum TreeItemType {
    FILE,
    FOLDER;

    public static boolean isFile(TreeItemType type) {
      return FILE == type;
    }

    public static boolean isFolder(TreeItemType type) {
      return FOLDER == type;
    }

    public static boolean isFile(Object type) {
      String typeStr;
      try {
        typeStr = (String) type;
      } catch (ClassCastException e) {
        return false;
      }
      TreeItemType value = TreeItemType.valueOf(typeStr);
      return isFile(value);
    }
  }

  protected void updateSelection() {

    String objectName = null;

    TreeItemType type = null;
    Boolean subFolderEnabled = false;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];
      if (selectedItem.getData("type") == null
          || selectedItem.getData("type").equals(CONST_FOLDER)) {
        objectName = null;
        type = TreeItemType.FOLDER;
      } else {
        objectName = getMetadataObjectName(selectedItem);
        type = TreeItemType.FILE;
      }

      subFolderEnabled = Boolean.valueOf(selectedItem.getData(SUBFOLDERS_ENABLED).toString());
    }

    boolean isFile = TreeItemType.isFile(type);
    boolean isFolder = TreeItemType.isFolder(type);

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_NEW, isFile);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CREATE_FOLDER, isFolder && subFolderEnabled);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, isFile);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DUPLICATE, isFile);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, isFile);

    menuWidgets.enableMenuItem(CONTEXT_MENU_NEW, isFolder);
    menuWidgets.enableMenuItem(CONTEXT_MENU_CREATE_FOLDER, isFolder && subFolderEnabled);
    menuWidgets.enableMenuItem(CONTEXT_MENU_EDIT, isFile);
    menuWidgets.enableMenuItem(CONTEXT_MENU_DUPLICATE, isFile);
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (typeHandler instanceof MetadataEditor editor && editor.isCloseable()) {
      editors.remove(editor);

      for (CTabItem item : tabFolder.getItems()) {
        if (editor.equals(item.getData())) {
          item.dispose();
        }
      }

      // Refresh tree to remove bold
      //
      this.refresh();

      // Update Gui menu and toolbar
      this.updateGui();
    }

    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
    List<TabItemHandler> items = new ArrayList<>();
    for (CTabItem tabItem : tabFolder.getItems()) {
      for (MetadataEditor<?> editor : editors) {
        if (tabItem.getData().equals(editor)) {
          // This is the editor tabItem...
          //
          items.add(new TabItemHandler(tabItem, editor));
        }
      }
    }

    return items;
  }

  @Override
  public void navigateToPreviousFile() {
    tabFolder.setSelection(tabFolder.getSelectionIndex() - 1);
    updateGui();
  }

  @Override
  public void navigateToNextFile() {
    tabFolder.setSelection(tabFolder.getSelectionIndex() + 1);
    updateGui();
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return tabFolder.getSelectionIndex() > 0;
  }

  @Override
  public boolean hasNavigationNextFile() {
    return (tabFolder.getItemCount() > 0)
        && (tabFolder.getSelectionIndex() < (tabFolder.getItemCount() - 1));
  }

  @Override
  public Control getControl() {
    return sash;
  }

  protected Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>();
  }

  @Override
  public List<ISearchable> getSearchables() {
    return new ArrayList<>();
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    MetadataEditor<?> editor = (MetadataEditor<?>) tabItem.getData();

    boolean isRemoved = remove(editor);

    if (!isRemoved && event != null) {
      event.doit = false;
      return;
    }

    // If all editor are closed
    //
    if (tabFolder.getItemCount() == 0) {
      HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  private String getKeyOfMetadataClass(Class<? extends IHopMetadata> managedClass) {
    HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);
    assert annotation != null : "Metadata classes need to be annotated with @HopMetadata";
    return annotation.key();
  }

  public void goToType(Class<? extends IHopMetadata> managedClass) {
    String key = getKeyOfMetadataClass(managedClass);
    // Look at all the top level items in the tree
    //
    for (TreeItem item : tree.getItems()) {
      String classKey = (String) item.getData();
      if (key.equals(classKey)) {
        // Found the item.
        //
        tree.setSelection(item);
        tree.showSelection();
        return;
      }
    }
  }

  public void goToElement(Class<? extends IHopMetadata> managedClass, String elementName) {
    String key = getKeyOfMetadataClass(managedClass);
    for (TreeItem item : tree.getItems()) {
      String classKey = (String) item.getData();
      if (key.equals(classKey)) {
        // Found the type.
        //
        for (TreeItem elementItem : item.getItems()) {
          if (elementName.equals(elementItem.getText())) {
            tree.setSelection(elementItem);
            tree.showSelection();
            onEditMetadata();
            return;
          }
        }
        goToType(managedClass);
      }
    }
  }

  private class TreeItemFolder {
    public TreeItem treeItem;
    public String path;
    public String name;
    public IHopFileType fileType;
    public int depth;
    public boolean folder;
    public boolean loaded;

    public TreeItemFolder(
        TreeItem treeItem,
        String path,
        String name,
        IHopFileType fileType,
        int depth,
        boolean folder,
        boolean loaded) {
      this.treeItem = treeItem;
      this.path = path;
      this.name = name;
      this.fileType = fileType;
      this.depth = depth;
      this.folder = folder;
      this.loaded = loaded;
    }
  }
}
