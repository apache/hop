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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataFileType;
import org.apache.hop.ui.core.metadata.MetadataManager;
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
import org.apache.hop.ui.hopgui.perspective.TabItemReorder;
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
import org.eclipse.swt.widgets.MenuItem;
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
    image = "ui/images/metadata.svg",
    documentationUrl = "/hop-gui/perspective-metadata.html")
@GuiPlugin(description = "i18n::MetadataPerspective.GuiPlugin.Description")
public class MetadataPerspective implements IHopPerspective, TabClosable {

  public static final Class<?> PKG = MetadataPerspective.class; // i18n
  private static final String METADATA_PERSPECTIVE_TREE = "Metadata perspective tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_NEW = "MetadataPerspective-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_EDIT = "MetadataPerspective-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE = "MetadataPerspective-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataPerspective-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_RENAME = "MetadataPerspective-Toolbar-10020-Rename";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataPerspective-Toolbar-10100-Refresh";

  private static final String KEY_HELP = "Help";
  private static final String KEY_TYPE = "type";
  public static final String FILE = "File";
  public static final String FOLDER = "Folder";
  public static final String VIRTUAL_PATH = "virtualPath";
  public static final String ERROR = "Error";

  @Getter private static MetadataPerspective instance;

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private GuiToolbarWidgets toolBarWidgets;

  private final List<MetadataEditor<?>> editors = new ArrayList<>();

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
    this.updateSelection();
    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return List.of(metadataFileType);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    // Split tree and editor
    //
    sash = new SashForm(parent, SWT.HORIZONTAL);
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    createTree(sash);
    createTabFolder(sash);

    sash.setWeights(new int[] {20, 80});

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

    // Add key listeners
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
    ToolBar toolBar = new ToolBar(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
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
        SWT.DefaultSelection,
        event -> {
          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null && treeItem.getData(KEY_TYPE).equals(FILE)) {
            if (treeItem.getParentItem() == null) {
              onNewMetadata();
            } else {
              onEditMetadata();
            }
          }
        });

    tree.addMenuDetectListener(
        event -> {
          if (tree.getSelectionCount() < 1) {
            return;
          }

          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null) {
            // Show the menu
            //
            Menu menu = new Menu(tree);
            MenuItem menuItem;

            switch ((String) treeItem.getData(KEY_TYPE)) {
              case "MetadataItem", FOLDER:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.New"));
                menuItem.addListener(SWT.Selection, e -> onNewMetadata());
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.NewFolder"));
                menuItem.addListener(SWT.Selection, e -> createNewFolder());
                new MenuItem(menu, SWT.SEPARATOR);
                break;
              case FILE:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Edit"));
                menuItem.setImage(GuiResource.getInstance().getImageEdit());
                menuItem.addListener(SWT.Selection, e -> onEditMetadata());

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Rename"));
                menuItem.setImage(GuiResource.getInstance().getImageRename());
                menuItem.addListener(SWT.Selection, e -> onRenameMetadata());

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Duplicate"));
                menuItem.setImage(GuiResource.getInstance().getImageDuplicate());
                menuItem.addListener(SWT.Selection, e -> duplicateMetadata());

                new MenuItem(menu, SWT.SEPARATOR);

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Delete"));
                menuItem.setImage(GuiResource.getInstance().getImageDelete());
                menuItem.addListener(SWT.Selection, e -> onDeleteMetadata());

                new MenuItem(menu, SWT.SEPARATOR);
                break;
              default:
                break;
            }

            menuItem = new MenuItem(menu, SWT.POP_UP);
            menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Help"));
            menuItem.setImage(GuiResource.getInstance().getImageHelp());
            menuItem.addListener(SWT.Selection, e -> onHelpMetadata());

            tree.setMenu(menu);
            menu.setVisible(true);
          }
        });
    PropsUi.setLook(tree);

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
            CTabItem tabItem = (CTabItem) event.item;
            closeTab(event, tabItem);
          }
        });
    tabFolder.addListener(SWT.Selection, event -> updateGui());
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    // Show/Hide tree
    //
    ToolBar tabToolBar = new ToolBar(tabFolder, SWT.FLAT);
    tabFolder.setTopRight(tabToolBar, SWT.RIGHT);
    PropsUi.setLook(tabToolBar);

    final ToolItem item = new ToolItem(tabToolBar, SWT.PUSH);
    item.setImage(GuiResource.getInstance().getImageMaximizePanel());
    item.addListener(
        SWT.Selection,
        e -> {
          if (sash.getMaximizedControl() == null) {
            sash.setMaximizedControl(tabFolder);
            item.setImage(GuiResource.getInstance().getImageMinimizePanel());
          } else {
            sash.setMaximizedControl(null);
            item.setImage(GuiResource.getInstance().getImageMaximizePanel());
          }
        });
    int height = tabToolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    tabFolder.setTabHeight(Math.max(height, tabFolder.getTabHeight()));

    new TabCloseHandler(this);

    // Support reorder tab item
    //
    new TabItemReorder(this, tabFolder);
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

    // If all editors are closed
    //
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler instanceof MetadataEditor) {
      this.setActiveEditor((MetadataEditor<?>) fileTypeHandler);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.New.Tooltip",
      image = "ui/images/new.svg")
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
        objectKey = (String) treeItem.getParentItem().getData();
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

        manager.newMetadataWithEditor((String) treeItem.getData(VIRTUAL_PATH));

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Message"),
            e);
      }
    }
    refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Edit.Tooltip",
      image = "ui/images/edit.svg")
  @GuiKeyboardShortcut(key = SWT.F3)
  @GuiOsxKeyboardShortcut(key = SWT.F3)
  public void onEditMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem.getData(KEY_TYPE).equals(FILE)) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      MetadataEditor<?> editor = this.findEditor(objectKey, objectName);
      if (editor != null) {
        this.setActiveEditor(editor);
      } else {
        try {
          MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
          manager.editWithEditor(objectName);

          hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
        } catch (Exception e) {
          new ErrorDialog(getShell(), ERROR, "Error editing metadata", e);
        }
      }

      this.updateSelection();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RENAME,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Rename.Tooltip",
      image = "ui/images/rename.svg")
  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void onRenameMetadata() {

    if (tree.getSelectionCount() < 1) {
      return;
    }

    // Identify the selected item
    TreeItem item = tree.getSelection()[0];
    if (item != null) {
      if (item.getParentItem() == null) return;
      String objectKey = (String) item.getParentItem().getData();
      String objectName = item.getText(0);

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
                if (!objectName.equals(text.getText())) {
                  try {
                    MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
                    if (manager.rename(objectName, text.getText())) {
                      text.dispose();
                    }
                    hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
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
              default:
                break;
            }
          });
      text.selectAll();
      text.setFocus();
      PropsUi.setLook(text);
      treeEditor.setEditor(text, item);
    }
  }

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

    // No delete on folder
    if (treeItem.getData(KEY_TYPE).equals(FILE)) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        manager.deleteMetadata(objectName);

        refresh();
        updateSelection();

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataDeleted.name());
      } catch (Exception e) {
        new ErrorDialog(getShell(), ERROR, "Error delete metadata", e);
      }
    }
  }

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
    if (treeItem.getData(KEY_TYPE).equals(FILE)) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

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
        new ErrorDialog(getShell(), ERROR, "Error opening URL", ex);
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
        classItem.setData(VIRTUAL_PATH, "");
        classItem.setData(KEY_TYPE, "MetadataItem");

        // level 1: object names
        //
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = serializer.listObjectNames();
        Collections.sort(names);

        for (final String name : names) {
          IHopMetadata hopMetadata;
          try {
            hopMetadata = serializer.load(name);
          } catch (HopException e) {
            // Ignore missing metadata items
            LogChannel.GENERAL.logError("Error loading metadata object:" + name);
            continue;
          }
          TreeItem parentItem = classItem;

          if (!Utils.isEmpty(hopMetadata.getVirtualPath())) {
            List<String> folders =
                new ArrayList<>(Arrays.asList(hopMetadata.getVirtualPath().split("/")));
            // remove empty elements
            folders.removeAll(Arrays.asList("", null));

            for (String folder : folders) {
              TreeItem alreadyExists = null;
              if (!folder.isEmpty()) {
                // check if folder already exists on this level
                alreadyExists = null;
                for (TreeItem childItem : parentItem.getItems()) {
                  if (childItem.getData(KEY_TYPE).equals(FOLDER)
                      && childItem.getText().equals(folder)) {
                    alreadyExists = childItem;
                  }
                }

                if (alreadyExists != null) {
                  parentItem = alreadyExists;
                } else {
                  TreeItem folderItem = new TreeItem(parentItem, SWT.NONE);
                  folderItem.setText(folder);
                  folderItem.setData(annotation.key());
                  folderItem.setImage(GuiResource.getInstance().getImageFolder());
                  folderItem.setData(
                      VIRTUAL_PATH,
                      folderItem.getParentItem().getData(VIRTUAL_PATH) + "/" + folder);
                  folderItem.setData(KEY_TYPE, FOLDER);
                  parentItem = folderItem;
                }
              }
            }
          }

          TreeItem item = new TreeItem(parentItem, SWT.NONE);
          item.setText(0, Const.NVL(name, ""));
          item.setData(VIRTUAL_PATH, parentItem.getData(VIRTUAL_PATH));
          item.setData(KEY_TYPE, FILE);
          MetadataEditor<?> editor = this.findEditor(annotation.key(), name);
          if (editor != null && editor.hasChanged()) {
            item.setFont(GuiResource.getInstance().getFontBold());
          }
        }
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

  protected void updateSelection() {

    boolean isMetadataSelected = false;
    if (tree.getSelectionCount() > 0) {
      TreeItem treeItem = tree.getSelection()[0];
      if (treeItem.getData(KEY_TYPE).equals(FILE)) {
        isMetadataSelected = true;
      }
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DUPLICATE, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, isMetadataSelected);
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (typeHandler instanceof MetadataEditor<?> editor && editor.isCloseable()) {

      editors.remove(editor);

      for (CTabItem item : tabFolder.getItems()) {
        if (editor.equals(item.getData())) {
          item.dispose();
        }
      }

      // Avoid refresh in a closing process (when switching project or exit)
      if (!hopGui.fileDelegate.isClosing()) {
        // Refresh tree to remove bold
        //
        this.refresh();

        // If all editors are closed
        //
        if (tabFolder.getItemCount() == 0) {
          HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
        }

        // Update Gui menu and toolbar
        this.updateGui();
      }
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
    if (hasNavigationPreviousFile()) {
      int index = tabFolder.getSelectionIndex() - 1;
      if (index < 0) {
        index = tabFolder.getItemCount() - 1;
      }
      tabFolder.setSelection(index);
      updateGui();
    }
  }

  @Override
  public void navigateToNextFile() {
    if (hasNavigationNextFile()) {
      int index = tabFolder.getSelectionIndex() + 1;
      if (index >= tabFolder.getItemCount()) {
        index = 0;
      }
      tabFolder.setSelection(index);
      updateGui();
    }
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return tabFolder.getItemCount() > 1;
  }

  @Override
  public boolean hasNavigationNextFile() {
    return tabFolder.getItemCount() > 1;
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
      // Ignore event if canceled
      event.doit = false;
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

  public void createNewFolder() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Header"),
            BaseMessages.getString(
                PKG,
                "MetadataPerspective.CreateFolder.Message",
                ((String) item.getData(VIRTUAL_PATH)).isEmpty()
                    ? item.getText()
                    : (String) item.getData(VIRTUAL_PATH)));
    String folder = dialog.open();
    if (!Utils.isEmpty(folder)) {
      for (TreeItem treeItem : item.getItems()) {
        if (folder.equals(treeItem.getText()) && treeItem.getData("type").equals(FOLDER)) {
          ShowMessageDialog msgDialog =
              new ShowMessageDialog(
                  getShell(),
                  SWT.ICON_INFORMATION | SWT.OK,
                  BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Header"),
                  BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Message"),
                  false);
          msgDialog.open();
          return;
        }
      }
      TreeItem newFolder = new TreeItem(item, SWT.NONE);
      newFolder.setText(folder);
      newFolder.setData(item.getData());
      newFolder.setImage(GuiResource.getInstance().getImageFolder());
      newFolder.setData(VIRTUAL_PATH, item.getData(VIRTUAL_PATH) + "/" + folder);
      newFolder.setData(KEY_TYPE, FOLDER);
      TreeItem emptyString = new TreeItem(newFolder, SWT.NONE);
      emptyString.setText(
          BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.EmptyFolder"));
      emptyString.setData(KEY_TYPE, "Label");
      emptyString.setForeground(tree.getDisplay().getSystemColor(SWT.COLOR_GRAY));
      newFolder.setExpanded(true);
    }
  }
}
