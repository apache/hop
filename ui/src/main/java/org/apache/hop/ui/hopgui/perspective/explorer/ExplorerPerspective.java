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

package org.apache.hop.ui.hopgui.perspective.explorer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeToolTipSupport;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.ExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@HopPerspectivePlugin(
    id = "300-HopExplorerPerspective",
    name = "File Explorer",
    description = "The Hop Explorer Perspective",
    image = "ui/images/folder.svg")
@GuiPlugin(description = "A file explorer for your current project")
public class ExplorerPerspective implements IHopPerspective {

  public static final String GUI_TOOLBAR_CREATED_CALLBACK_ID =
      "ExplorerPerspective-Toolbar-Created";

  private static final String FILE_EXPLORER_TREE = "File explorer tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ExplorerPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_OPEN = "ExplorerPerspective-Toolbar-10000-Open";
  public static final String TOOLBAR_ITEM_DELETE = "ExplorerPerspective-Toolbar-10100-Delete";
  public static final String TOOLBAR_ITEM_REFRESH = "ExplorerPerspective-Toolbar-10100-Refresh";

  private static ExplorerPerspective instance;

  public static ExplorerPerspective getInstance() {
    // There can be only one
    if (instance == null) {
      new ExplorerPerspective();
    }
    return instance;
  }

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;

  private List<ExplorerFile> files = new ArrayList<>();

  private final EmptyFileType emptyFileType;
  private final ExplorerFileType explorerFileType;

  private String rootFolder;
  private String rootName;

  private class TreeItemFolder {
    public TreeItem treeItem;
    public String path;
    public String name;
    public IHopFileType fileType;

    public TreeItemFolder(TreeItem treeItem, String path, String name, IHopFileType fileType) {
      this.treeItem = treeItem;
      this.path = path;
      this.name = name;
      this.fileType = fileType;
    }
  }

  private Map<String, TreeItemFolder> treeItemFolderMap;

  private List<IExplorerFilePaintListener> filePaintListeners;

  private List<IExplorerRootChangedListener> rootChangedListeners;

  private List<IExplorerRefreshListener> refreshListeners;
  private List<IExplorerSelectionListener> selectionListeners;

  private List<IHopFileType> fileTypes;

  private Map<String, Image> typeImageMap;

  public ExplorerPerspective() {
    instance = this;

    this.emptyFileType = new EmptyFileType();
    this.explorerFileType = new ExplorerFileType();

    this.treeItemFolderMap = new HashMap<>();
    this.filePaintListeners = new ArrayList<>();
    this.rootChangedListeners = new ArrayList<>();
    this.refreshListeners = new ArrayList<>();
    this.selectionListeners = new ArrayList<>();
    this.typeImageMap = new HashMap<>();
  }

  @Override
  public String getId() {
    return "explorer-perspective";
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
      HopGui.getInstance().handleFileCapabilities(emptyFileType, false, false, false);
    } else {
      ExplorerFile activeFile = getActiveFile();
      boolean changed = activeFile!=null ? activeFile.isChanged() : false;
      HopGui.getInstance().handleFileCapabilities(explorerFileType, changed, false, false);
    }
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return Arrays.asList(explorerFileType);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    determineRootFolderName(hopGui);
    loadFileTypes();
    loadTypeImages(parent);

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

    // TODO: Refresh the root folder when it comes back into focus and when it's needed
    //

  }

  private void loadFileTypes() {
    fileTypes = new ArrayList<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(HopFileTypePluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IHopFileType fileType = (IHopFileType) registry.loadClass(plugin);
        fileTypes.add(fileType);
      } catch (Exception e) {
        hopGui.getLog().logError("Unable to load file type plugin: " + plugin.getIds()[0], e);
      }
    }
    // Keep as last in the list...
    fileTypes.add(new GenericFileType());
  }

  private void loadTypeImages(Composite parentComposite) {
    typeImageMap = new HashMap<>();
    int iconSize = (int) (PropsUi.getInstance().getZoomFactor() * 16);

    for (IHopFileType fileType : fileTypes) {
      String imageFilename = fileType.getFileTypeImage();
      if (imageFilename != null) {
        try {
          SvgCacheEntry svgCacheEntry =
              SvgCache.loadSvg(new SvgFile(imageFilename, fileType.getClass().getClassLoader()));
          SwtUniversalImageSvg imageSvg =
              new SwtUniversalImageSvg(new SvgImage(svgCacheEntry.getSvgDocument()));
          Image image = imageSvg.getAsBitmapForSize(hopGui.getDisplay(), iconSize, iconSize);
          typeImageMap.put(fileType.getName(), image);
        } catch (Exception e) {
          hopGui
              .getLog()
              .logError(
                  "Error loading image : '"
                      + imageFilename
                      + "' for type '"
                      + fileType.getName()
                      + "'",
                  e);
        }
      }
    }
    // Properly dispose images when done...
    //
    parentComposite.addListener(
        SWT.Dispose,
        e -> {
          for (Image image : typeImageMap.values()) {
            image.dispose();
          }
        });
  }

  public class DetermineRootFolderExtension {
    public HopGui hopGui;
    public String rootFolder;
    public String rootName;

    public DetermineRootFolderExtension(HopGui hopGui, String rootFolder, String rootName) {
      this.hopGui = hopGui;
      this.rootFolder = rootFolder;
      this.rootName = rootName;
    }
  }

  public void determineRootFolderName(HopGui hopGui) {

    String oldRootFolder = rootFolder;
    String oldRootName = rootName;
    rootFolder = hopGui.getVariables().getVariable("user.home");
    rootName = "Home folder";

    DetermineRootFolderExtension ext =
        new DetermineRootFolderExtension(hopGui, rootFolder, rootName);
    try {
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          hopGui.getVariables(),
          HopGuiExtensionPoint.HopGuiDetermineExplorerRoot.id,
          ext);
      rootFolder = ext.rootFolder;
      rootName = ext.rootName;
    } catch (Exception e) {
      new ErrorDialog(
          getShell(), "Error", "Error getting root folder/name of explorer perspective", e);
    }

    if (!StringUtils.equals(oldRootFolder, rootFolder)
        || !StringUtils.equals(oldRootName, rootName)) {
      // call the root changed listeners...
      //
      for (IExplorerRootChangedListener listener : rootChangedListeners) {
        listener.rootChanged(rootFolder, rootName);
      }
    }
  }

  protected void createTree(Composite parent) {
    PropsUi props = PropsUi.getInstance();

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
    props.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    tree = new Tree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    tree.addListener(SWT.Selection, event -> updateSelection());
    tree.addListener(SWT.DefaultSelection, this::openFile);
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
    TreeMemory.addTreeListener(tree, FILE_EXPLORER_TREE);

    // Inform other plugins that this toolbar is created
    // They can then add listeners to this class and so on.
    //
    GuiRegistry.getInstance().executeCallbackMethods(GUI_TOOLBAR_CREATED_CALLBACK_ID);
  }

  private void openFile(Event event) {
    if (event.item instanceof TreeItem) {
      TreeItem item = (TreeItem) event.item;
      openFile(item);
    }
  }

  private void openFile(TreeItem item) {
    try {
      TreeItemFolder tif = treeItemFolderMap.get(ConstUi.getTreePath(item, 0));
      if (tif != null && tif.fileType != null) {
        tif.fileType.openFile(hopGui, tif.path, hopGui.getVariables());
        updateGui();
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error opening file", e);
    }
  }

  private void deleteFile(TreeItem item) {
    try {
      TreeItemFolder tif = treeItemFolderMap.get(ConstUi.getTreePath(item, 0));
      if (tif != null && tif.fileType != null) {

        MessageBox box = new MessageBox( hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
        box.setText( "Delete file?" );
        box.setMessage( "Are you sure you want to delete the following file?"+Const.CR+Const.CR+tif.path );
        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          FileObject fileObject = HopVfs.getFileObject(tif.path);
          boolean deleted = fileObject.delete();
          if (deleted) {
            refresh();
            updateSelection();
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error opening file", e);
    }
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
    tabFolder.addListener(SWT.Selection, event -> handleTabSelectionEvent(event));
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

  /**
   * Also select the corresponding file in the left hand tree...
   *
   * @param event
   */
  private void handleTabSelectionEvent(Event event) {
    if (event.item instanceof CTabItem) {
      CTabItem tabItem = (CTabItem) event.item;
      ExplorerFile explorerFile = (ExplorerFile) tabItem.getData();
      selectInTree(explorerFile.getFilename());
    }
  }

  public CTabItem addFile(ExplorerFile explorerFile, IExplorerFileTypeHandler renderer) {
    PropsUi props = PropsUi.getInstance();

    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(tabFolder.getFont());
    tabItem.setText(Const.NVL(explorerFile.getName(), ""));
    if (explorerFile.getTabImage() != null) {
      tabItem.setImage(explorerFile.getTabImage());
    } else {
      tabItem.setImage(GuiResource.getInstance().getImageFile());
    }
    tabItem.setToolTipText(explorerFile.getFilename());
    tabItem.setData(explorerFile);

    // Set the tab bold if the file has changed and vice-versa
    //
    explorerFile.addContentChangedListener(
        new IContentChangedListener() {
          @Override
          public void contentChanged(Object parentObject) {
            tabItem.setFont(GuiResource.getInstance().getFontBold());
          }

          @Override
          public void contentSafe(Object parentObject) {
            tabItem.setFont(tabFolder.getFont());
          }
        });

    // Create composite for editor and buttons
    //
    Composite composite = new Composite(tabFolder, SWT.NONE);
    FormLayout layoutComposite = new FormLayout();
    layoutComposite.marginWidth = Const.FORM_MARGIN;
    layoutComposite.marginHeight = Const.FORM_MARGIN;
    composite.setLayout(layoutComposite);
    props.setLook(composite);

    // This is usually done by the file type
    //
    renderer.renderFile(composite);

    // Create file content area
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
    fdArea.bottom = new FormAttachment(100, 0);

    area.setLayoutData(fdArea);
    props.setLook(area);

    tabItem.setControl(composite);
    tabItem.setData(explorerFile);

    files.add(explorerFile);

    // Activate perspective
    //
    this.activate();

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    selectInTree(explorerFile.getFilename());

    updateGui();

    return tabItem;
  }

  private void selectInTree(String filename) {
    for (TreeItemFolder tif : treeItemFolderMap.values()) {
      if (tif.path.equals(filename)) {
        tree.setSelection(tif.treeItem);
        return;
      }
    }
  }

  /**
   * Which file is selected in the tree. It's not delivering a type or handler
   *
   * @return The selected explorer file or null if nothing is selected
   */
  public ExplorerFile getSelectedFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return null;
    }
    TreeItem item = selection[0];

    for (TreeItemFolder tif : treeItemFolderMap.values()) {
      if (tif.treeItem == item) {

        Image image = getFileTypeImage(tif.fileType);
        return new ExplorerFile(tif.name, image, tif.path, null, null);
      }
    }
    return null;
  }

  public void setActiveFile(ExplorerFile file) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(file)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        HopGui.getInstance().handleFileCapabilities(explorerFileType, file.isChanged(), false, false);
      }
    }
  }

  public void closeFile(ExplorerFile explorerFile) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(explorerFile)) {
        if (explorerFile.getFileTypeHandler().isCloseable()) {
          item.dispose();
        }
      }
    }
  }

  public ExplorerFile getActiveFile() {
    if (tabFolder.getSelectionIndex() < 0) {
      return null;
    }

    return (ExplorerFile) tabFolder.getSelection().getData();
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    ExplorerFile explorerFile = getActiveFile();
    if (explorerFile != null) {
      return explorerFile.getFileTypeHandler();
    }

    return new EmptyHopFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler instanceof ExplorerFile) {
      this.setActiveFile((ExplorerFile) fileTypeHandler);
    }
  }

  protected void onTabClose(CTabFolderEvent event) {
    CTabItem tabItem = (CTabItem) event.item;
    ExplorerFile file = (ExplorerFile) tabItem.getData();

    if (file.getFileTypeHandler().isCloseable()) {
      files.remove(file);
      tabItem.dispose();

      // Refresh tree to remove bold
      //
      this.refresh();

      // If all editor are closed
      //
      if (tabFolder.getItemCount() == 0) {
        HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
      }
      updateGui();
    } else {
      // Ignore event if canceled
      event.doit = false;
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_OPEN,
      toolTip = "Open selected file",
      image = "ui/images/arrow-right.svg")
  public void openFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    openFile(selection[0]);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "Delete selected file",
      image = "ui/images/delete.svg",
      separator = true)
  public void deleteFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    deleteFile(selection[0]);
  }

  public void onNewFile() {}

  boolean first = true;

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "Refresh",
      image = "ui/images/refresh.svg")
  public void refresh() {
    try {
      determineRootFolderName(hopGui);

      for (IExplorerRefreshListener listener : refreshListeners) {
        listener.beforeRefresh();
      }

      tree.setRedraw(false);
      tree.removeAll();
      treeItemFolderMap.clear();

      // Add the root element...
      //
      TreeItem rootItem = new TreeItem(tree, SWT.NONE);
      rootItem.setText(Const.NVL(rootName, ""));
      IHopFileType fileType = getFileType(rootFolder);
      setItemImage(rootItem, fileType);
      callPaintListeners(tree, rootItem, rootFolder, rootName, fileType);
      addToFolderMap(rootItem, rootFolder, rootName, fileType);

      refreshFolder(rootItem, rootFolder);

      tree.setRedraw(true);

      if (first) {
        first = false;
        // Set the top level items in the tree to be expanded
        //
        for (TreeItem item : tree.getItems()) {
          item.setExpanded(true);
          TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, item, true);
        }
      } else {
        TreeMemory.setExpandedFromMemory(tree, FILE_EXPLORER_TREE);
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error refreshing file explorer tree", e);
    }
    ExplorerPerspective.getInstance().updateSelection();
  }

  private void addToFolderMap(TreeItem treeItem, String path, String name, IHopFileType fileType) {
    treeItemFolderMap.put(
        ConstUi.getTreePath(treeItem, 0), new TreeItemFolder(treeItem, path, name, fileType));
  }

  private void setItemImage(TreeItem treeItem, IHopFileType fileType) {
    Image image = typeImageMap.get(fileType.getName());
    if (image != null) {
      treeItem.setImage(image);
    }
  }

  public Image getFileTypeImage(IHopFileType fileType) {
    return typeImageMap.get(fileType.getName());
  }

  public IHopFileType getFileType(String path) throws HopException {

    // TODO: get this list from the plugin registry...
    //
    for (IHopFileType hopFileType : fileTypes) {
      // Only look at the extension of the file
      //
      if (hopFileType.isHandledBy(path, false)) {
        return hopFileType;
      }
    }

    return new EmptyFileType();
  }

  private void refreshFolder(TreeItem item, String path) {

    try {
      FileObject fileObject = HopVfs.getFileObject(path);
      FileObject[] children = fileObject.getChildren();

      // Sort by full path ascending
      Arrays.sort(children, Comparator.comparing(Object::toString));

      for (boolean folder : new boolean[] {true, false}) {
        for (FileObject child : children) {
          if (child.isHidden()) {
            continue; // skip hidden files for now
          }
          if (child.isFolder() != folder) {
            continue;
          }

          String childPath = child.getName().getPath();
          String childName = child.getName().getBaseName();
          IHopFileType fileType = getFileType(childPath);
          TreeItem childItem = new TreeItem(item, SWT.NONE);
          childItem.setText(childName);
          setItemImage(childItem, fileType);
          callPaintListeners(tree, childItem, childPath, childName, fileType);
          addToFolderMap(childItem, childPath, childName, fileType);

          // Recursively add children
          //
          if (child.isFolder()) {
            refreshFolder(childItem, child.getName().getPath());
          }
        }
      }

    } catch (Exception e) {
      TreeItem treeItem = new TreeItem(item, SWT.NONE);
      treeItem.setText("!!Error refreshing folder!!");
      hopGui.getLog().logError("Error refresh folder '" + path + "'", e);
    }
  }

  private void callPaintListeners(
      Tree tree, TreeItem treeItem, String path, String name, IHopFileType fileType) {
    for (IExplorerFilePaintListener filePaintListener : filePaintListeners) {
      filePaintListener.filePainted(tree, treeItem, path, name);
    }
  }

  public void updateSelection() {

    String objectKey = null;
    TreeItemFolder tif = null;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];
      objectKey = ConstUi.getTreePath(selectedItem, 0);
      tif = treeItemFolderMap.get(objectKey);
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_OPEN, tif != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, tif != null);

    for (IExplorerSelectionListener listener : selectionListeners) {
      listener.fileSelected();
    }
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (typeHandler instanceof ExplorerFile) {
      ExplorerFile file = (ExplorerFile) typeHandler;

      if (file.getFileTypeHandler().isCloseable()) {

        files.remove(file);

        for (CTabItem item : tabFolder.getItems()) {
          if (file.equals(item.getData())) {
            item.dispose();
          }
        }

        // Refresh tree to remove bold
        //
        this.refresh();

        // If all editor are closed
        //
        if (tabFolder.getItemCount() == 0) {
          HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
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

  public void updateGui() {
    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    hopGui
        .getDisplay()
        .asyncExec(() -> hopGui.handleFileCapabilities(activeHandler.getFileType(), activeHandler.hasChanged(), false, false));
  }

  /**
   * Gets rootChangedListeners
   *
   * @return value of rootChangedListeners
   */
  public List<IExplorerRootChangedListener> getRootChangedListeners() {
    return rootChangedListeners;
  }

  /**
   * Gets toolBarWidgets
   *
   * @return value of toolBarWidgets
   */
  public GuiToolbarWidgets getToolBarWidgets() {
    return toolBarWidgets;
  }

  /**
   * Gets filePaintListeners
   *
   * @return value of filePaintListeners
   */
  public List<IExplorerFilePaintListener> getFilePaintListeners() {
    return filePaintListeners;
  }

  /**
   * Gets rootFolder
   *
   * @return value of rootFolder
   */
  public String getRootFolder() {
    return rootFolder;
  }

  /**
   * Gets rootName
   *
   * @return value of rootName
   */
  public String getRootName() {
    return rootName;
  }

  /**
   * Gets refreshListeners
   *
   * @return value of refreshListeners
   */
  public List<IExplorerRefreshListener> getRefreshListeners() {
    return refreshListeners;
  }

  /**
   * Gets selectionListeners
   *
   * @return value of selectionListeners
   */
  public List<IExplorerSelectionListener> getSelectionListeners() {
    return selectionListeners;
  }

  /**
   * Gets tree
   *
   * @return value of tree
   */
  public Tree getTree() {
    return tree;
  }
}
