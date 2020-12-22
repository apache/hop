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

package org.apache.hop.ui.hopgui.perspective.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.search.ISearchable;
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
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataFileType;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeToolTipSupport;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
    name = "Metadata",
    description = "The Hop Metatada Perspective",
    image = "ui/images/metadata.svg"
)
@GuiPlugin(description="This perspective allows you to see and edit all available metadata")
public class MetadataPerspective implements IHopPerspective {

  private static final String METADATA_PERSPECTIVE_TREE = "Metadata perspective tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_EDIT = "MetadataPerspective-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE = "MetadataPerspective-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataPerspective-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataPerspective-Toolbar-10100-Refresh";

  public static final String KEY_HELP = "Help";

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

  private List<MetadataEditor<?>> editors = new ArrayList<>();

  private final EmptyFileType emptyFileType;
  private final MetadataFileType metadataFileType;

  public MetadataPerspective() {
    instance = this;

    this.emptyFileType = new EmptyFileType();
    this.metadataFileType = new MetadataFileType();
  }

  @Override
  public String getId() {
    return "metadata-perspective";
  }

  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    this.refresh();
    this.updateSelection();

    // If all editor are closed
    //
    if (tabFolder.getItemCount() == 0) {
      HopGui.getInstance().handleFileCapabilities(emptyFileType, false, false);
    } else {
      HopGui.getInstance().handleFileCapabilities(metadataFileType, false, false);
    }
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
    hopGui.getEventsHandler().addEventListener(
      getClass().getName(),
      e->refresh(),
      HopGuiEvents.MetadataChanged.name()
    );

  }

  protected MetadataManager<IHopMetadata> getMetadataManager(String objectKey) throws HopException {
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
    return new MetadataManager<>(HopGui.getInstance().getVariables(), metadataProvider, metadataClass);
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

            MenuItem menuItem = new MenuItem(menu, SWT.POP_UP);
            menuItem.setText("New");
            menuItem.addListener(SWT.Selection, e -> onNewMetadata());

            if (treeItem.getParentItem() != null) {
              new MenuItem(menu, SWT.SEPARATOR);

              menuItem = new MenuItem(menu, SWT.POP_UP);
              menuItem.setText("Edit");
              menuItem.addListener(SWT.Selection, e -> onEditMetadata());

              menuItem = new MenuItem(menu, SWT.POP_UP);
              menuItem.setText("Rename");
              menuItem.addListener(SWT.Selection, e -> onRenameMetadata());

              menuItem = new MenuItem(menu, SWT.POP_UP);
              menuItem.setText("Duplicate");
              menuItem.addListener(SWT.Selection, e -> duplicateMetadata());

              new MenuItem(menu, SWT.SEPARATOR);

              menuItem = new MenuItem(menu, SWT.POP_UP);
              menuItem.setText("Delete");
              menuItem.addListener(SWT.Selection, e -> onDeleteMetadata());
            }

            tree.setMenu(menu);
            menu.setVisible(true);
          }
        });
    PropsUi.getInstance().setLook(tree);

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

    // Add on first level tooltip with metatada description
    new TreeToolTipSupport(tree);

    // Remember tree node expanded/Collapsed
    TreeMemory.addTreeListener(tree, METADATA_PERSPECTIVE_TREE);
  }

  protected void createTabFolder(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    tabFolder = new CTabFolder(parent, SWT.MULTI | SWT.BORDER);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            onTabClose(event);
          }
        });
    props.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    // Show/Hide tree
    //
    ToolBar toolBar = new ToolBar(tabFolder, SWT.FLAT);
    final ToolItem item = new ToolItem(toolBar, SWT.PUSH);
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
    tabFolder.setTopRight(toolBar, SWT.RIGHT);

    // Support reorder tab item
    //
    new TabFolderReorder(tabFolder);
  }

  public void addEditor(MetadataEditor<?> editor) {
    PropsUi props = PropsUi.getInstance();

    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setText(editor.getTitle());
    tabItem.setImage(editor.getTitleImage());
    tabItem.setToolTipText(editor.getTitleToolTip());

    // Create composite for editor and buttons
    //
    Composite composite = new Composite(tabFolder, SWT.NONE);
    FormLayout layoutComposite = new FormLayout();
    layoutComposite.marginWidth = Const.FORM_MARGIN;
    layoutComposite.marginHeight = Const.FORM_MARGIN;
    composite.setLayout(layoutComposite);
    props.setLook(composite);

    // Create buttons
    Button[] buttons = editor.createButtonsForButtonBar(composite);
    if (buttons != null) {
      BaseTransformDialog.positionBottomButtons(composite, buttons, props.getMargin(), null);
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
      fdArea.bottom = new FormAttachment(buttons[0], -props.getMargin());
    } else {
      fdArea.bottom = new FormAttachment(100, -props.getMargin());
    }

    area.setLayoutData(fdArea);
    props.setLook(area);

    // Create editor controls
    //
    editor.createControl(area);

    tabItem.setControl(composite);
    tabItem.setData(editor);

    editors.add(editor);

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

        HopGui.getInstance().handleFileCapabilities(metadataFileType, false, false);
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
      if ( editor!=null ) {
	  return editor;
      }
      
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
    MetadataEditor<?> editor = (MetadataEditor<?>) tabItem.getData();

    if (editor.isCloseable()) {
      editors.remove(editor);
      tabItem.dispose();

      // Refresh tree to remove bold
      //
      this.refresh();

      // If all editor are closed
      //
      if (tabFolder.getItemCount() == 0) {
        HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false);
      }
    } else {
      // Ignore event if canceled
      event.doit = false;
    }
  }

  public void onNewMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null) {
      String objectKey;
      if ( treeItem.getParentItem()==null ) {
	  objectKey = (String) treeItem.getData();
      }
      else {
	  objectKey = (String) treeItem.getParentItem().getData(); 
      }
	  

      try {
        IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
        Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
        MetadataManager<IHopMetadata> manager =
            new MetadataManager<>(
                HopGui.getInstance().getVariables(), metadataProvider, metadataClass);

        manager.newMetadataWithEditor();
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error create metadata", e);
      }
    }
  }

  public void onEditMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getParentItem() != null) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      MetadataEditor<?> editor = this.findEditor(objectKey, objectName);
      if (editor != null) {
        this.setActiveEditor(editor);
      } else {
        try {
          MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
          manager.editWithEditor(objectName);
        } catch (Exception e) {
          new ErrorDialog(getShell(), "Error", "Error editing metadata", e);
        }
      }
      
      this.updateSelection();
    }
  }

  @GuiToolbarElement(
	      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
	      id = TOOLBAR_ITEM_EDIT,
	      toolTip = "Edit",
	      image = "ui/images/edit.svg")
  public void onRenameMetadata() {

    if ( tree.getSelectionCount() < 1) {
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
              case SWT.CR:
              case SWT.KEYPAD_CR:
                // If name changed
                if (!item.getText().equals(text.getText())) {
                  try {
                    MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
                    if (manager.rename(item.getText(), text.getText())) {
                      item.setText(text.getText());
                      text.dispose();
                    }
                  } catch (Exception e) {
                    new ErrorDialog(getShell(), "Error", "Error rename metadata", e);
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
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "Delete",
      image = "ui/images/delete.svg")
  public void onDeleteMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getParentItem() != null) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        manager.deleteMetadata(objectName);

        refresh();
        updateSelection();
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error delete metadata", e);
      }
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DUPLICATE,
      toolTip = "Create a copy",
      image = "ui/images/duplicate.svg")
  public void duplicateMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null && treeItem.getParentItem() != null) {
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
            // manager.editWithEditor(newName);
            break;
          } else {
            copyNr++;
          }
        }
        refresh();
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error duplicating metadata", e);
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
        if (editor.isChanged()) item.setFont(GuiResource.getInstance().getFontBold());
        else item.setFont(tabFolder.getFont());
        break;
      }
    }

    // Update TreeItem
    //
    this.refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "Refresh",
      image = "ui/images/refresh.svg")
  public void refresh() {
    try {
      tree.setRedraw(false);
      tree.removeAll();

      // top level: object key
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
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
        classItem.setText(0, Const.NVL(annotation.name(), ""));
        classItem.setImage(image);
        classItem.setExpanded(true);
        classItem.setData(annotation.key());
        classItem.setData(KEY_HELP, annotation.description());

        // level 1: object names
        //
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = serializer.listObjectNames();
        Collections.sort(names);

        for (final String name : names) {
          TreeItem item = new TreeItem(classItem, SWT.NONE);
          item.setText(0, Const.NVL(name, ""));
          MetadataEditor<?> editor = this.findEditor(annotation.key(), name);
          if (editor != null) {
            if (editor.isChanged()) {
              item.setFont(GuiResource.getInstance().getFontBold());
            }
          }
        }
      }

      TreeUtil.setOptimalWidthOnColumns(tree);
      TreeMemory.setExpandedFromMemory(tree, METADATA_PERSPECTIVE_TREE);

      tree.setRedraw(true);
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error refreshing metadata tree", e);
    }
  }

  protected void updateSelection() {

    String objectKey = null;
    String objectName = null;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];
      if (selectedItem.getParentItem() == null) {
        objectKey = selectedItem.getText();
        objectName = null;
      } else {
        objectKey = (String) selectedItem.getParentItem().getData();
        objectName = selectedItem.getText(0);
      }
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, StringUtils.isNotEmpty(objectName));
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DUPLICATE, StringUtils.isNotEmpty(objectName));
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, StringUtils.isNotEmpty(objectName));
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (typeHandler instanceof MetadataEditor) {
      MetadataEditor<?> editor = (MetadataEditor<?>) typeHandler;

      if (editor.isCloseable()) {

        editors.remove(editor);

        for (CTabItem item : tabFolder.getItems()) {
          if (editor.equals(item.getData())) {
            item.dispose();
          }
        }

        // Refresh tree to remove bold
        //
        this.refresh();

        // If all editor are closed
        //
        if (tabFolder.getItemCount() == 0) {
          HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false);
        }
      }
    }

    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
    return null;
  }

  @Override
  public void navigateToPreviousFile() {
    tabFolder.setSelection(tabFolder.getSelectionIndex() + 1);
  }

  @Override
  public void navigateToNextFile() {
    tabFolder.setSelection(tabFolder.getSelectionIndex() - 1);
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    if (tabFolder.getItemCount() == 0) {
      return false;
    }
    return tabFolder.getSelectionIndex() >= 1;
  }

  @Override
  public boolean hasNavigationNextFile() {
    if (tabFolder.getItemCount() == 0) {
      return false;
    }
    return tabFolder.getSelectionIndex() < tabFolder.getItemCount();
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
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

  @Override
  public List<ISearchable> getSearchables() {
    List<ISearchable> searchables = new ArrayList<>();
    return searchables;
  }
}
