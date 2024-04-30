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

package org.apache.hop.ui.hopgui.perspective.explorer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigSingleton;
import org.apache.hop.ui.hopgui.perspective.explorer.file.ExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.BusyIndicator;
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
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@HopPerspectivePlugin(
    id = "300-HopExplorerPerspective",
    name = "i18n::ExplorerPerspective.Name",
    description = "The Hop Explorer Perspective",
    image = "ui/images/folder.svg")
@GuiPlugin(description = "i18n::ExplorerPerspective.GuiPlugin.Description")
public class ExplorerPerspective implements IHopPerspective, TabClosable {

  public static final Class<?> PKG = ExplorerPerspective.class; // i18n

  public static final String GUI_TOOLBAR_CREATED_CALLBACK_ID =
      "ExplorerPerspective-Toolbar-Created";

  private static final String FILE_EXPLORER_TREE = "File explorer tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ExplorerPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_OPEN = "ExplorerPerspective-Toolbar-10000-Open";
  public static final String TOOLBAR_ITEM_CREATE_FOLDER =
      "ExplorerPerspective-Toolbar-10050-CreateFolder";
  public static final String TOOLBAR_ITEM_DELETE = "ExplorerPerspective-Toolbar-10100-Delete";
  public static final String TOOLBAR_ITEM_RENAME = "ExplorerPerspective-Toolbar-10200-Rename";
  public static final String TOOLBAR_ITEM_REFRESH = "ExplorerPerspective-Toolbar-10300-Refresh";

  private static ExplorerPerspective instance;

  private boolean treeIsFresh;

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

    this.filePaintListeners = new ArrayList<>();
    this.rootChangedListeners = new ArrayList<>();
    this.refreshListeners = new ArrayList<>();
    this.selectionListeners = new ArrayList<>();
    this.typeImageMap = new HashMap<>();

    this.treeIsFresh = false;
  }

  @Override
  public String getId() {
    return "explorer-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'e')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'e')
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    if (!treeIsFresh) {
      this.refresh();
    }

    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.singletonList(explorerFileType);
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

    sash.setWeights(20, 80);

    // refresh the file explorer when project activated or updated.
    //
    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName() + "ProjectActivated",
            e -> refresh(),
            HopGuiEvents.ProjectActivated.name());

    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName() + "ProjectUpdated",
            e -> refresh(),
            HopGuiEvents.ProjectUpdated.name());

    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);
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

  public static class DetermineRootFolderExtension {
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
          getShell(),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.RootFolder.Header"),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.RootFolder.Message"),
          e);
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
    tree.addListener(SWT.Selection, event -> updateSelection());
    tree.addListener(SWT.DefaultSelection, this::openFile);
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

    // Lazy loading...
    //
    tree.addListener(SWT.Expand, this::lazyLoadFolderOnExpand);

    // Remember tree node expanded/Collapsed
    //
    TreeMemory.addTreeListener(tree, FILE_EXPLORER_TREE);

    // Inform other plugins that this toolbar is created
    // They can then add listeners to this class and so on.
    //
    GuiRegistry.getInstance().executeCallbackMethods(GUI_TOOLBAR_CREATED_CALLBACK_ID);
  }

  /**
   * This is called when a user expands a folder. We only need to lazily load the contents of the
   * folder if it's not loaded already. To keep track of this we have a flag called "loaded" in the
   * item data.
   */
  private void lazyLoadFolderOnExpand(Event event) {
    // Which folder is being expanded?
    //
    TreeItem item = (TreeItem) event.item;
    TreeItemFolder treeItemFolder = (TreeItemFolder) item.getData();
    if (treeItemFolder != null) {
      if (!treeItemFolder.loaded) {
        BusyIndicator.showWhile(
            hopGui.getDisplay(),
            () -> {
              refreshFolder(item, treeItemFolder.path, treeItemFolder.depth + 1);
              treeItemFolder.loaded = true;
            });
      }
    }
  }

  private void openFile(Event event) {
    if (event.item instanceof TreeItem) {
      TreeItem item = (TreeItem) event.item;

      TreeItemFolder tif = (TreeItemFolder) item.getData();
      if (tif.folder) {
        if (!item.getExpanded()) {
          lazyLoadFolderOnExpand(event);
          item.setExpanded(true);
        } else {
          item.setExpanded(false);
        }
        TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, item, item.getExpanded());
      } else {
        openFile(item);
      }
    }
  }

  private void openFile(TreeItem item) {
    try {
      TreeItemFolder tif = (TreeItemFolder) item.getData();
      if (tif != null && tif.fileType != null) {
        if (tif.fileType instanceof FolderFileType) {
          // Expand the folder
          //
          boolean expanded = !item.getExpanded();
          item.setExpanded(expanded);
          TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, item, expanded);
        } else {
          IHopFileTypeHandler handler =
              tif.fileType.openFile(hopGui, tif.path, hopGui.getVariables());
          if (handler != null) {
            updateGui();
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.OpenFile.Header"),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.OpenFile.Message"),
          e);
    }
  }

  private void deleteFile(TreeItem item) {
    try {
      TreeItemFolder tif = (TreeItemFolder) item.getData();
      if (tif != null && tif.fileType != null) {
        FileObject fileObject = HopVfs.getFileObject(tif.path);

        String header =
            BaseMessages.getString(PKG, "ExplorerPerspective.DeleteFile.Confirmation.Header");
        String message =
            BaseMessages.getString(PKG, "ExplorerPerspective.DeleteFile.Confirmation.Message");
        if (fileObject.isFolder()) {
          header =
              BaseMessages.getString(PKG, "ExplorerPerspective.DeleteFolder.Confirmation.Header");
          message =
              BaseMessages.getString(PKG, "ExplorerPerspective.DeleteFolder.Confirmation.Message");
        }

        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText(header);
        box.setMessage(message + Const.CR + Const.CR + tif.path);

        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          int deleted = fileObject.deleteAll();
          if (deleted > 0) {
            refresh();
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.DeleteFile.Header"),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.DeleteFile.Message"),
          e);
    }
  }

  private void renameFile(TreeItem item) {
    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null && tif.fileType != null) {

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
                    FileObject fileObject = HopVfs.getFileObject(tif.path);
                    FileObject newObject =
                        HopVfs.getFileObject(
                            HopVfs.getFilename(fileObject.getParent()) + "/" + text.getText());
                    fileObject.moveTo(newObject);
                  } catch (Exception e) {
                    new ErrorDialog(
                        hopGui.getShell(),
                        BaseMessages.getString(PKG, "ExplorerPerspective.Error.RenameFile.Header"),
                        BaseMessages.getString(PKG, "ExplorerPerspective.Error.RenameFile.Message"),
                        e);
                  } finally {
                    text.dispose();
                    refresh();
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
      PropsUi.setLook(text);
      treeEditor.setEditor(text, item);
    }
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
    tabFolder.addListener(SWT.Selection, this::handleTabSelectionEvent);
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

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

    new TabCloseHandler(this);

    // Support reorder tab item
    //
    new TabFolderReorder(tabFolder);
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    ExplorerFile file = (ExplorerFile) tabItem.getData();

    if (file.getFileTypeHandler().isCloseable()) {
      files.remove(file);
      tabItem.dispose();

      //
      // Remove the file in refreshDelegate
      try {
        hopGui.fileRefreshDelegate.remove(
            HopVfs.getFileObject(file.getFileTypeHandler().getFilename()).getPublicURIString());
      } catch (HopFileException e) {
        hopGui.getLog().logError("Error getting VFS fileObject", e);
      }

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
      if (event != null) {
        // Ignore event if canceled
        event.doit = false;
      }
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  /**
   * Also select the corresponding file in the left hand tree...
   *
   * @param event The selection event
   */
  private void handleTabSelectionEvent(Event event) {
    if (event.item instanceof CTabItem) {
      CTabItem tabItem = (CTabItem) event.item;
      ExplorerFile explorerFile = (ExplorerFile) tabItem.getData();
      selectInTree(explorerFile.getFilename());
      updateGui();
    }
  }

  public void addFile(ExplorerFile explorerFile) {

    if (files.contains(explorerFile)) {
      return;
    }

    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
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
    layoutComposite.marginWidth = PropsUi.getFormMargin();
    layoutComposite.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layoutComposite);
    PropsUi.setLook(composite);

    IExplorerFileTypeHandler renderer = explorerFile.getFileTypeHandler();
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
    PropsUi.setLook(area);

    tabItem.setControl(composite);
    tabItem.setData(explorerFile);

    files.add(explorerFile);
    hopGui.fileRefreshDelegate.register(explorerFile.getFilename(), renderer);

    // Activate perspective
    //
    if (!isActive()) {
      this.activate();
    }

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    selectInTree(explorerFile.getFilename());

    updateGui();
  }

  public void refreshFileContent() {
    tabFolder.getChildren();
  }

  private void selectInTree(String filename) {
    // Look in the whole tree for the file...
    //
    for (TreeItem item : tree.getItems()) {
      if (selectInTree(item, filename)) {
        break;
      }
    }
  }

  private boolean selectInTree(TreeItem item, String filename) {
    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null && tif.path.equals(filename)) {
      tree.setSelection(tif.treeItem);
      return true;
    }
    for (TreeItem child : item.getItems()) {
      if (selectInTree(child, filename)) {
        return true;
      }
    }
    return false;
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

    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null) {
      Image image = getFileTypeImage(tif.fileType);
      return new ExplorerFile(tif.name, image, tif.path, null, null);
    }
    return null;
  }

  public void setActiveFile(ExplorerFile file) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(file)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        HopGui.getInstance()
            .handleFileCapabilities(explorerFileType, file.isChanged(), false, false);
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
    closeTab(event, tabItem);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_OPEN,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Open.Tooltip",
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
      id = TOOLBAR_ITEM_CREATE_FOLDER,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.CreateFolder.Tooltip",
      image = "ui/images/folder-add.svg")
  public void createFolder() {

    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];
    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif == null) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "ExplorerPerspective.CreateFolder.Header"),
            BaseMessages.getString(PKG, "ExplorerPerspective.CreateFolder.Message", tif.path));
    String folder = dialog.open();
    if (folder != null) {
      String newPath = tif.path;
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

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Delete.Tooltip",
      image = "ui/images/delete.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void deleteFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    deleteFile(selection[0]);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RENAME,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Rename.Tooltip",
      image = "ui/images/rename.svg",
      separator = false)
  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void renameFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    renameFile(selection[0]);
  }

  public void onNewFile() {}

  boolean first = true;

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    try {
      determineRootFolderName(hopGui);

      for (IExplorerRefreshListener listener : refreshListeners) {
        listener.beforeRefresh();
      }

      tree.setRedraw(false);
      tree.removeAll();

      // Add the root element...
      //
      TreeItem rootItem = new TreeItem(tree, SWT.NONE);
      rootItem.setText(Const.NVL(rootName, ""));
      IHopFileType fileType = getFileType(rootFolder);
      setItemImage(rootItem, fileType);
      callPaintListeners(tree, rootItem, rootFolder, rootName, fileType);
      setTreeItemData(rootItem, rootFolder, rootName, fileType, 0, true, true);

      // Paint the top level folder only
      //
      refreshFolder(rootItem, rootFolder, 0);

      tree.setRedraw(true);

      TreeMemory.setExpandedFromMemory(tree, FILE_EXPLORER_TREE);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.TreeRefresh.Header"),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.TreeRefresh.Message"),
          e);
    }
    updateSelection();
    treeIsFresh = true;
  }

  private void setTreeItemData(
      TreeItem treeItem,
      String path,
      String name,
      IHopFileType fileType,
      int depth,
      boolean folder,
      boolean loaded) {
    treeItem.setData(new TreeItemFolder(treeItem, path, name, fileType, depth, folder, loaded));
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

  private void refreshFolder(TreeItem item, String path, int depth) {

    try {
      // Remove any old children in the item...
      //
      for (TreeItem child : item.getItems()) {
        child.dispose();
      }

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

          String childPath = child.toString();
          String childName = child.getName().getBaseName();
          IHopFileType fileType = getFileType(childPath);
          TreeItem childItem = new TreeItem(item, SWT.NONE);
          childItem.setText(childName);
          setItemImage(childItem, fileType);
          callPaintListeners(tree, childItem, childPath, childName, fileType);
          setTreeItemData(childItem, childPath, childName, fileType, depth, folder, true);

          // Recursively add children
          //
          if (child.isFolder()) {
            // What is the maximum depth to lazily load?
            //
            String maxDepthString =
                ExplorerPerspectiveConfigSingleton.getConfig().getLazyLoadingDepth();
            int maxDepth = Const.toInt(hopGui.getVariables().resolve(maxDepthString), 0);
            if (depth + 1 <= maxDepth) {
              // Remember folder data to expand easily
              //
              childItem.setData(
                  new TreeItemFolder(
                      childItem,
                      child.getName().getURI(),
                      childName,
                      fileType,
                      depth,
                      folder,
                      true));

              // We actually load the content up to the desired depth
              //
              refreshFolder(childItem, childPath, depth + 1);
            } else {
              // Remember folder data to expand easily
              //
              childItem.setData(
                  new TreeItemFolder(
                      childItem,
                      child.getName().getURI(),
                      childName,
                      fileType,
                      depth,
                      folder,
                      false));

              // Create a new item to get the "expand" icon but without the content behind it.
              // The folder just contains an empty item to show the expand icon.
              //
              new TreeItem(childItem, SWT.NONE);
              childItem.setExpanded(false);
              TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, childItem, false);
            }
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

    TreeItemFolder tif = null;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];
      tif = (TreeItemFolder) selectedItem.getData();
      if (tif == null) {
        return;
      }
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_OPEN, tif != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, tif != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, tif != null);
    toolBarWidgets.enableToolbarItem(
        TOOLBAR_ITEM_CREATE_FOLDER, tif != null && tif.fileType instanceof FolderFileType);

    for (IExplorerSelectionListener listener : selectionListeners) {
      listener.fileSelected();
    }
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {

    if (typeHandler instanceof BaseExplorerFileTypeHandler) {
      BaseExplorerFileTypeHandler fileTypeHandler = (BaseExplorerFileTypeHandler) typeHandler;

      if (fileTypeHandler.isCloseable()) {
        ExplorerFile file = fileTypeHandler.getExplorerFile();
        files.remove(file);
        for (CTabItem item : tabFolder.getItems()) {
          if (file.equals(item.getData())) {
            item.dispose();
          }
        }

        // Refresh tree to remove bold
        //
        this.refresh();

        // Update HopGui menu and toolbar
        //
        this.updateGui();
      }
    }

    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
    List<TabItemHandler> items = new ArrayList<>();
    for (CTabItem tabItem : tabFolder.getItems()) {
      for (ExplorerFile file : files) {
        if (tabItem.getData().equals(file)) {
          // This is the editor tabItem...
          //
          items.add(new TabItemHandler(tabItem, file.getFileTypeHandler()));
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

  /** Update HOP GUI menu and toolbar... */
  public void updateGui() {
    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    activeHandler.updateGui();
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
