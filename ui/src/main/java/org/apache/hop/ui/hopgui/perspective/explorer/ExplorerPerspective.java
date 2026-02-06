/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.perspective.explorer;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemReorder;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigSingleton;
import org.apache.hop.ui.hopgui.perspective.explorer.file.ExplorerFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.apache.hop.ui.hopgui.selection.HopGuiSelectionTracker;
import org.apache.hop.ui.hopgui.shared.CanvasZoomHelper;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.BusyIndicator;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.FileTransfer;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

@HopPerspectivePlugin(
    id = "100-HopExplorerPerspective",
    name = "i18n::ExplorerPerspective.Name",
    description = "The Hop Explorer Perspective",
    image = "ui/images/folder.svg",
    documentationUrl = "/hop-gui/perspective-file-explorer.html")
@GuiPlugin(
    name = "i18n::ExplorerPerspective.Name",
    description = "i18n::ExplorerPerspective.GuiPlugin.Description")
@SuppressWarnings("java:S1104")
public class ExplorerPerspective implements IHopPerspective, TabClosable {

  public static final Class<?> PKG = ExplorerPerspective.class; // i18n

  public static final String GUI_TOOLBAR_CREATED_CALLBACK_ID =
      "ExplorerPerspective-Toolbar-Created";
  public static final String GUI_CONTEXT_MENU_CREATED_CALLBACK_ID =
      "ExplorerPerspective-ContextMenu-Created";
  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ExplorerPerspective-Toolbar";
  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "ExplorerPerspective-ContextMenu";
  public static final String TOOLBAR_ITEM_OPEN = "ExplorerPerspective-Toolbar-10000-Open";
  public static final String TOOLBAR_ITEM_CREATE_FOLDER =
      "ExplorerPerspective-Toolbar-10050-CreateFolder";
  public static final String TOOLBAR_ITEM_EXPAND_ALL =
      "ExplorerPerspective-Toolbar-10060-ExpandAll";
  public static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "ExplorerPerspective-Toolbar-10070-CollapseAll";
  public static final String TOOLBAR_ITEM_DELETE = "ExplorerPerspective-Toolbar-10100-Delete";
  public static final String TOOLBAR_ITEM_RENAME = "ExplorerPerspective-Toolbar-10200-Rename";
  public static final String TOOLBAR_ITEM_REFRESH = "ExplorerPerspective-Toolbar-10300-Refresh";
  public static final String TOOLBAR_ITEM_SHOW_HIDDEN =
      "ExplorerPerspective-Toolbar-10400-Show-hidden";
  public static final String TOOLBAR_ITEM_SELECT_OPENED_FILE =
      "ExplorerPerspective-Toolbar-10500-Select-opened-file";
  public static final String CONTEXT_MENU_CREATE_FOLDER =
      "ExplorerPerspective-ContextMenu-10050-CreateFolder";
  public static final String CONTEXT_MENU_EXPAND_ALL =
      "ExplorerPerspective-ContextMenu-10060-ExpandAll";
  public static final String CONTEXT_MENU_COLLAPSE_ALL =
      "ExplorerPerspective-ContextMenu-10070-CollapseAll";
  public static final String CONTEXT_MENU_OPEN = "ExplorerPerspective-ContextMenu-10100-Open";
  public static final String CONTEXT_MENU_RENAME = "ExplorerPerspective-ContextMenu-10300-Rename";
  public static final String CONTEXT_MENU_COPY_NAME =
      "ExplorerPerspective-ContextMenu-10400-CopyName";
  public static final String CONTEXT_MENU_COPY_PATH =
      "ExplorerPerspective-ContextMenu-10401-CopyPath";
  public static final String CONTEXT_MENU_DELETE = "ExplorerPerspective-ContextMenu-90000-Delete";
  private static final String FILE_EXPLORER_TREE = "File explorer tree";
  private static ExplorerPerspective instance;
  @Getter private GuiToolbarWidgets toolBarWidgets;

  @Getter private final ExplorerFileType explorerFileType;
  @Getter private final HopPipelineFileType<PipelineMeta> pipelineFileType;
  @Getter private final HopWorkflowFileType<WorkflowMeta> workflowFileType;

  private HopGui hopGui;
  private SashForm sash;
  @Getter private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private Control toolBar;
  @Getter private GuiMenuWidgets menuWidgets;
  private final List<TabItemHandler> items;
  private boolean showingHiddenFiles;

  private boolean fileExplorerPanelVisible = true;
  @Getter private String rootFolder;
  @Getter private String rootName;
  private String dragFile;
  private int dropOperation;
  @Getter private List<IExplorerFilePaintListener> filePaintListeners;
  @Getter private List<IExplorerRootChangedListener> rootChangedListeners;
  @Getter private List<IExplorerRefreshListener> refreshListeners;
  @Getter private List<IExplorerSelectionListener> selectionListeners;
  private List<IHopFileType> fileTypes;
  private Map<String, Image> typeImageMap;
  private Text searchText;
  private String filterText = "";
  private Map<String, Boolean> treeStateBeforeFilter = null;

  public ExplorerPerspective() {
    instance = this;

    this.explorerFileType = new ExplorerFileType();
    this.pipelineFileType = new HopPipelineFileType<>();
    this.workflowFileType = new HopWorkflowFileType<>();

    this.items = new CopyOnWriteArrayList<>();
    this.filePaintListeners = new ArrayList<>();
    this.rootChangedListeners = new ArrayList<>();
    this.refreshListeners = new ArrayList<>();
    this.selectionListeners = new ArrayList<>();
    this.typeImageMap = new HashMap<>();
    this.showingHiddenFiles = false;
  }

  public static ExplorerPerspective getInstance() {
    // There can be only one
    if (instance == null) {
      new ExplorerPerspective();
    }
    return instance;
  }

  @Override
  public String getId() {
    return "explorer-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'd')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'd')
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return List.of(explorerFileType, pipelineFileType, workflowFileType);
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
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    createTree(sash);
    createTabFolder(sash);

    sash.setWeights(20, 80);

    // Set initial file explorer panel visibility from configuration
    Boolean visibleByDefault =
        ExplorerPerspectiveConfigSingleton.getConfig().getFileExplorerVisibleByDefault();
    if (visibleByDefault != null && !visibleByDefault) {
      fileExplorerPanelVisible = false;
      sash.setMaximizedControl(tabFolder);
    } else {
      fileExplorerPanelVisible = true;
    }

    // Refresh the file explorer when a project is activated or updated.
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

    // Add key listeners
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
    Composite treeComposite;
    // Create composite
    //
    treeComposite = new Composite(parent, SWT.NONE);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    treeComposite.setLayout(layout);

    // Create search/filter text box
    //
    searchText = new Text(treeComposite, SWT.SEARCH | SWT.ICON_CANCEL | SWT.ICON_SEARCH);
    searchText.setMessage(BaseMessages.getString(PKG, "ExplorerPerspective.Search.Placeholder"));
    PropsUi.setLook(searchText);
    FormData searchFormData = new FormData();
    searchFormData.left = new FormAttachment(0, 0);
    searchFormData.top = new FormAttachment(0, 0);
    searchFormData.right = new FormAttachment(100, 0);
    searchText.setLayoutData(searchFormData);

    // Add listener to filter tree on text change
    searchText.addListener(
        SWT.Modify,
        event -> {
          String text = searchText.getText();
          boolean wasFiltering = !Utils.isEmpty(filterText);
          boolean willFilter = text != null && text.length() > 2;

          // Save tree state before filtering starts
          if (!wasFiltering && willFilter) {
            saveTreeState();
          }

          // Only filter when we have more than 2 characters, otherwise show all
          filterText = willFilter ? text.toLowerCase() : "";
          refresh();

          // Restore tree state after filtering ends
          if (wasFiltering && !willFilter) {
            restoreTreeState();
          }
        });

    // Create a composite with toolbar and tree for the border
    Composite composite = new Composite(treeComposite, SWT.BORDER);
    composite.setLayout(new FormLayout());
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(searchText, PropsUi.getMargin());
    layoutData.right = new FormAttachment(100, 0);
    layoutData.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(layoutData);

    // Create toolbar
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBar = toolBarContainer.getControl();
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData toolBarFormData = new FormData();
    toolBarFormData.left = new FormAttachment(0, 0);
    toolBarFormData.top = new FormAttachment(0, 0);
    toolBarFormData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(toolBarFormData);
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    tree = new Tree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    tree.addListener(SWT.Selection, event -> updateSelection());
    tree.addListener(SWT.DefaultSelection, this::openFile);
    PropsUi.setLook(tree);

    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment(0, 0);
    treeFormData.top = new FormAttachment(toolBar, PropsUi.getMargin());
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

    // Create context menu...
    //
    Menu menu = new Menu(tree);
    menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    tree.setMenu(menu);
    tree.addListener(
        SWT.MenuDetect,
        event -> {
          if (tree.getSelectionCount() < 1) {
            return;
          }

          TreeItem[] selection = tree.getSelection();
          menuWidgets.findMenuItem(CONTEXT_MENU_OPEN).setEnabled(selection.length == 1);
          menuWidgets.findMenuItem(CONTEXT_MENU_RENAME).setEnabled(selection.length == 1);

          // Show the menu
          //
          menu.setVisible(true);
        });

    // Create drag and drop on the tree
    //
    createTreeDragSource(tree);
    createTreeDropTarget(tree);

    // Remember tree node expanded/Collapsed
    //
    TreeMemory.addTreeListener(tree, FILE_EXPLORER_TREE);

    // Inform other plugins that toolbar and context menu are created
    // They can then add listeners to this class and so on.
    //
    GuiRegistry.getInstance().executeCallbackMethods(GUI_TOOLBAR_CREATED_CALLBACK_ID);
    GuiRegistry.getInstance().executeCallbackMethods(GUI_CONTEXT_MENU_CREATED_CALLBACK_ID);
  }

  /**
   * Creates the Drag & Drop DragSource for items being dragged from the tree.
   *
   * @return the DragSource for the tree
   */
  private DragSource createTreeDragSource(final Tree tree) {

    final FileTransfer fileTransfer = FileTransfer.getInstance();

    final DragSource dragSource = new DragSource(tree, DND.DROP_COPY | DND.DROP_MOVE);
    dragSource.setTransfer(fileTransfer);
    dragSource.addDragListener(
        new DragSourceAdapter() {
          @Override
          public void dragStart(DragSourceEvent event) {
            ExplorerFile file = getSelectedFile();
            String metadataFolder = hopGui.getVariables().getVariable(Const.HOP_METADATA_FOLDER);
            // Avoid moving root folder, metadata folder or hidden file (like .git)
            if (file == null
                || file.getFilename().equals(rootFolder)
                || file.getFilename().contains(metadataFolder)
                || file.getName().startsWith(".")) {
              event.doit = false;
              return;
            }

            // Used by dragOver
            dragFile = file.getFilename();
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            if (fileTransfer.isSupportedType(event.dataType)) {
              event.doit = true;
              event.data = new String[] {getSelectedFile().getFilename()};
            }
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            dragFile = null;
          }
        });

    return dragSource;
  }

  /**
   * Creates the Drag & Drop DropTarget for items being dropped onto the tree.
   *
   * @return the DropTarget for the tree
   */
  private DropTarget createTreeDropTarget(final Tree tree) {

    final FileTransfer fileTransfer = FileTransfer.getInstance();

    // Allow files to be copied or moved to the drop target
    DropTarget target = new DropTarget(tree, DND.DROP_COPY | DND.DROP_MOVE);
    target.setTransfer(fileTransfer);
    target.addDropListener(
        new DropTargetAdapter() {

          @Override
          public void dragEnter(final DropTargetEvent event) {
            // By default try to perform a move operation
            if (event.detail == DND.DROP_DEFAULT) {
              // Check if the drag source support the move action
              if ((event.operations & DND.DROP_MOVE) == 0) {
                event.detail = DND.DROP_COPY;
              } else {
                event.detail = DND.DROP_MOVE;
              }
            }
            dropOperation = event.detail;

            // Will accept only files dropped
            for (int i = 0; i < event.dataTypes.length; i++) {
              if (fileTransfer.isSupportedType(event.dataTypes[i])) {
                event.currentDataType = event.dataTypes[i];
                break;
              }
            }
          }

          @Override
          public void dragOperationChanged(DropTargetEvent event) {
            // By default try to perform a move operation
            if (event.detail == DND.DROP_DEFAULT) {
              // Check if the drag source support the move action
              if ((event.operations & DND.DROP_MOVE) == 0) {
                event.detail = DND.DROP_COPY;
              } else {
                event.detail = DND.DROP_MOVE;
              }
            }

            dropOperation = event.detail;
          }

          @Override
          public void dragLeave(DropTargetEvent event) {
            // Do nothing
          }

          @Override
          public void dropAccept(final DropTargetEvent event) {
            // Will accept only files dropped
            if (!FileTransfer.getInstance().isSupportedType(event.currentDataType)) {
              event.detail = DND.DROP_NONE;
            }
          }

          @Override
          public void dragOver(final DropTargetEvent event) {
            if (event.item == null) {
              return;
            }

            Object data = event.item.getData();
            if (data instanceof TreeItemFolder targetItem) {
              // Only drop to folder
              if (targetItem.folder) {
                event.detail = dropOperation;
                event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL | DND.FEEDBACK_EXPAND;

                try {
                  FileObject targetFile = HopVfs.getFileObject(targetItem.path);

                  // For internal drag check hierarchies
                  if (dragFile != null) {
                    FileObject sourceFile = HopVfs.getFileObject(dragFile);

                    // Avoid copy or move to itself, it's parent or for folder it's descendant
                    if (sourceFile.equals(targetFile)
                        || sourceFile.getParent().equals(targetFile)
                        || sourceFile.getName().isDescendent(targetFile.getName())) {
                      event.detail = DND.DROP_NONE;
                    }
                  }
                } catch (FileSystemException | HopFileException e) {
                  // Ignore
                }
              } else {
                event.detail = DND.DROP_NONE;
                event.feedback = DND.FEEDBACK_NONE;
              }
            }
          }

          @Override
          public void drop(final DropTargetEvent event) {
            if (FileTransfer.getInstance().isSupportedType(event.currentDataType)) {
              Widget item = event.item;
              if (item.getData() instanceof TreeItemFolder targetItem) {
                List<String> errors = new ArrayList<>();

                for (String path : (String[]) event.data) {
                  try {
                    FileObject sourceFile = HopVfs.getFileObject(path);
                    FileObject targetFile =
                        HopVfs.getFileObject(
                            targetItem.path
                                + Const.FILE_SEPARATOR
                                + sourceFile.getName().getBaseName());

                    if (event.detail == DND.DROP_COPY) {
                      // Copy file and folder and all its descendants
                      // No need to update tab item handler because all files are new
                      targetFile.copyFrom(sourceFile, Selectors.SELECT_ALL);
                    } else if (event.detail == DND.DROP_MOVE) {
                      // Move file or folder and all its descendants and update tab item handlers
                      moveFile(sourceFile, targetFile);
                    }
                  } catch (Exception e) {
                    errors.add(path);
                  }
                }

                // Report errors
                if (!errors.isEmpty()) {

                  String paths = String.join("\n", errors);

                  MessageBox messageBox =
                      new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_ERROR | SWT.OK);
                  messageBox.setText("Drag and drop");
                  messageBox.setMessage("Unable to copy/move file(s):\n\n" + paths);
                  messageBox.open();
                }

                refresh();
              }
            }
          }
        });

    return target;
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
    if (treeItemFolder != null && !treeItemFolder.loaded) {
      BusyIndicator.showWhile(
          hopGui.getDisplay(),
          () -> {
            refreshFolder(item, treeItemFolder.path, treeItemFolder.depth + 1);
            treeItemFolder.loaded = true;
          });
    }
  }

  private void openFile(Event event) {
    if (event.item instanceof TreeItem item) {
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
            handler.updateGui();
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

  private void deleteFile(final TreeItem treeItem) {
    try {
      TreeItemFolder tif = (TreeItemFolder) treeItem.getData();
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
        box.setMessage(message + Const.CR + Const.CR + HopVfs.getFilename(fileObject));

        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          // List files before they are deleted
          List<String> filenames = getRecursiveFilenames(fileObject, new ArrayList<>());

          // Delete file or folder
          int deleted = fileObject.deleteAll();
          if (deleted > 0) {
            treeItem.dispose();

            // Closes all impacted file type handlers that are opened
            for (String filename : filenames) {
              TabItemHandler handler = findTabItemHandler(filename);
              if (handler != null) {
                removeTabItem(handler);
              }
            }
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

  private List<String> getRecursiveFilenames(FileObject parentFile, List<String> list)
      throws FileSystemException {
    if (parentFile.isFile()) {
      list.add(HopVfs.getFilename(parentFile));
    } else {
      for (FileObject file : parentFile.getChildren()) {
        getRecursiveFilenames(file, list);
      }
    }
    return list;
  }

  private void renameFile(final TreeItem item) {
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
              case SWT.CR, SWT.KEYPAD_CR:
                // If name changed
                if (!item.getText().equals(text.getText())) {
                  try {
                    FileObject file = HopVfs.getFileObject(tif.path);
                    FileObject newFile =
                        HopVfs.getFileObject(
                            file.getParent().getName().toString()
                                + File.separator
                                + text.getText());
                    renameFile(file, newFile);
                    item.setText(text.getText());
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

  private void renameFile(FileObject sourceFile, FileObject targetFile) throws FileSystemException {

    if (sourceFile.isFolder()) {
      // List all impacted files
      List<String> filenames = getRecursiveFilenames(sourceFile, new ArrayList<>());

      // Rename the folder
      sourceFile.moveTo(targetFile);

      // Update all opened impacted file type handlers
      for (String filename : filenames) {
        TabItemHandler handler = findTabItemHandler(filename);
        if (handler != null) {
          Path oldPath = Paths.get(filename);
          Path targetPath = targetFile.getPath();
          Path relativePath = oldPath.subpath(targetPath.getNameCount(), oldPath.getNameCount());
          Path path = Paths.get(targetPath.toString(), relativePath.toString());

          changeFilename(handler.getTypeHandler(), path.toString());
          updateTabItem(handler.getTypeHandler());
          saveFileIfNameSynchronized(handler.getTypeHandler());
        }
      }
    } else {
      // Rename the file
      sourceFile.moveTo(targetFile);

      // Update opened file type handler
      TabItemHandler handler = findTabItemHandler(HopVfs.getFilename(sourceFile));
      if (handler != null) {
        changeFilename(handler.getTypeHandler(), HopVfs.getFilename(targetFile));
        updateTabItem(handler.getTypeHandler());
        saveFileIfNameSynchronized(handler.getTypeHandler());
      } else {
        // File is not open, but we still need to update the name attribute if name sync is enabled
        updateClosedFileIfNameSynchronized(targetFile);
      }
    }

    // TODO: Search and rename dependencies
  }

  private void moveFile(FileObject sourceFile, FileObject targetFile) throws FileSystemException {

    if (sourceFile.isFolder()) {
      // List all impacted files before moving the folder
      List<String> filenames = getRecursiveFilenames(sourceFile, new ArrayList<>());

      // Move file
      sourceFile.moveTo(targetFile);

      // Update all opened impacted file type handlers
      Path sourcePath = sourceFile.getPath();
      Path targetPath = targetFile.getPath();
      for (String filename : filenames) {
        TabItemHandler handler = findTabItemHandler(filename);
        if (handler != null) {
          Path originalPath = Paths.get(filename);
          Path relativePath =
              originalPath.subpath(sourcePath.getNameCount(), originalPath.getNameCount());
          Path path = Paths.get(targetPath.toString(), relativePath.toString());
          changeFilename(handler.getTypeHandler(), path.toString());
          updateTabItem(handler.getTypeHandler());
        }
      }

      // TODO: Search and move dependencies
    } else {
      // Move file
      sourceFile.moveTo(targetFile);

      // Update opened file type handler
      TabItemHandler handler = findTabItemHandler(HopVfs.getFilename(sourceFile));
      if (handler != null) {
        handler.getTypeHandler().setFilename(HopVfs.getFilename(targetFile));
        updateTabItem(handler.getTypeHandler());
      }

      // TODO: Search and move dependencies
    }
  }

  /** Change the file name of an open tab */
  protected void changeFilename(IHopFileTypeHandler fileTypeHandler, String newFilename) {
    String oldFilename = fileTypeHandler.getFilename();
    hopGui.fileRefreshDelegate.remove(oldFilename);
    fileTypeHandler.setFilename(newFilename);
    hopGui.fileRefreshDelegate.register(newFilename, fileTypeHandler);
  }

  /**
   * Save the file if it's a pipeline or workflow with name synchronization enabled. This ensures
   * that when a file is renamed, the name attribute in the XML is updated to match the new
   * filename.
   *
   * @param fileTypeHandler to specify which filetype
   */
  private void saveFileIfNameSynchronized(IHopFileTypeHandler fileTypeHandler) {
    try {
      Object subject = fileTypeHandler.getSubject();
      if (subject instanceof PipelineMeta pipelineMeta
          && pipelineMeta.isNameSynchronizedWithFilename()) {
        fileTypeHandler.save();
      } else if (subject instanceof WorkflowMeta workflowMeta
          && workflowMeta.isNameSynchronizedWithFilename()) {
        fileTypeHandler.save();
      }

    } catch (Exception e) {
      hopGui.getLog().logError("Error saving file after rename", e);
    }
  }

  /**
   * Update a closed file's name attribute if it has name synchronization enabled. This is called
   * when renaming a file that is not currently open in any tab.
   */
  private void updateClosedFileIfNameSynchronized(FileObject targetFile) {
    try {
      String filename = HopVfs.getFilename(targetFile);
      String extension = filename.substring(filename.lastIndexOf('.'));

      // Check if it's a pipeline or workflow file
      if (".hpl".equalsIgnoreCase(extension)) {
        // Load pipeline
        IVariables variables = Variables.getADefaultVariableSpace();
        PipelineMeta pipelineMeta =
            new PipelineMeta(filename, hopGui.getMetadataProvider(), variables);
        if (pipelineMeta.isNameSynchronizedWithFilename()) {
          // Save to update the name attribute
          String xml = pipelineMeta.getXml(variables);
          OutputStream out = HopVfs.getOutputStream(filename, false);
          try {
            out.write(XmlHandler.getXmlHeader(Const.XML_ENCODING).getBytes(StandardCharsets.UTF_8));
            out.write(xml.getBytes(StandardCharsets.UTF_8));
          } finally {
            out.flush();
            out.close();
          }
        }
      } else if (".hwf".equalsIgnoreCase(extension)) {
        // Load workflow
        IVariables variables = Variables.getADefaultVariableSpace();
        WorkflowMeta workflowMeta =
            new WorkflowMeta(variables, filename, hopGui.getMetadataProvider());
        if (workflowMeta.isNameSynchronizedWithFilename()) {
          // Save to update the name attribute
          String xml = workflowMeta.getXml(variables);
          OutputStream out = HopVfs.getOutputStream(filename, false);
          try {
            out.write(XmlHandler.getXmlHeader(Const.XML_ENCODING).getBytes(StandardCharsets.UTF_8));
            out.write(xml.getBytes(StandardCharsets.UTF_8));
          } finally {
            out.flush();
            out.close();
          }
        }
      }
    } catch (Exception e) {
      hopGui.getLog().logError("Error updating closed file after rename", e);
    }
  }

  protected void createTabFolder(Composite parent) {
    tabFolder = new CTabFolder(parent, SWT.MULTI | SWT.BORDER);
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          updateGui();
          // Notify zoom handler when tab is switched (for web/RAP)
          if (org.apache.hop.ui.util.EnvironmentUtils.getInstance().isWeb()) {
            notifyZoomHandlerForActiveTab();
          }
        });
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            CTabItem tabItem = (CTabItem) event.item;
            closeTab(event, tabItem);
          }
        });
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

  protected TabItemHandler findTabItemHandler(String filename) {
    if (filename != null) {
      for (TabItemHandler item : items) {
        if (filename.equals(item.getTypeHandler().getFilename())) {
          return item;
        }
      }
    }
    return null;
  }

  /**
   * Close tabs for the given filenames (e.g. after files are deleted by revert or external delete).
   * Only tabs whose handler filename exactly matches one of the given filenames are closed.
   *
   * @param filenames filenames as stored in the tab handlers (e.g. from HopVfs.getFilename)
   */
  public void closeTabsForFilenames(java.util.Collection<String> filenames) {
    if (filenames == null) {
      return;
    }
    for (String filename : filenames) {
      TabItemHandler handler = findTabItemHandler(filename);
      if (handler != null) {
        removeTabItem(handler);
      }
    }
  }

  /**
   * Reload content from disk for tabs matching the given filenames (e.g. after revert that did not
   * delete the file). Only tabs whose handler filename exactly matches one of the given filenames
   * are reloaded; handlers that support {@link IHopFileTypeHandler#reload()} will refresh content.
   *
   * @param filenames filenames as stored in the tab handlers (e.g. from HopVfs.getFilename)
   */
  public void reloadTabsForFilenames(java.util.Collection<String> filenames) {
    if (filenames == null) {
      return;
    }
    for (String filename : filenames) {
      TabItemHandler handler = findTabItemHandler(filename);
      if (handler != null) {
        try {
          handler.getTypeHandler().reload();
        } catch (Exception e) {
          hopGui.getLog().logError("Error reloading file '" + filename + "'", e);
        }
      }
    }
  }

  /**
   * Get an existing tab item handler, can be used when the IHopeFileTypeHandler has no file name.
   */
  protected TabItemHandler getTabItemHandler(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler != null) {
      for (TabItemHandler item : items) {
        if (fileTypeHandler.equals(item.getTypeHandler())) {
          return item;
        }
      }
    }
    return null;
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    IHopFileTypeHandler fileTypeHandler = (IHopFileTypeHandler) tabItem.getData();
    boolean isRemoved = remove(fileTypeHandler);
    if (!isRemoved && event != null) {
      // Ignore event if canceled
      event.doit = false;
    }
  }

  private void removeTabItem(TabItemHandler item) {
    items.remove(item);

    // Close the tab
    item.getTabItem().dispose();

    // Remove the file in refreshDelegate
    //
    IHopFileTypeHandler fileTypeHandler = item.getTypeHandler();
    if (fileTypeHandler.getFilename() != null) {
      hopGui.fileRefreshDelegate.remove(fileTypeHandler.getFilename());
    }

    // Avoid refresh in a closing process (when switching project or exit)
    if (!hopGui.fileDelegate.isClosing()) {

      // If all tab items are closed
      //
      if (tabFolder.getItemCount() == 0) {
        HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
      }

      // Update HopGui menu and toolbar
      //
      this.updateGui();
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  public void addFile(IExplorerFileTypeHandler fileTypeHandler) {

    // Select and show tab item
    //
    TabItemHandler handler = this.findTabItemHandler(fileTypeHandler.getFilename());
    if (handler != null) {
      tabFolder.setSelection(handler.getTabItem());
      tabFolder.showItem(handler.getTabItem());
      tabFolder.setFocus();
      return;
    }

    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    String displayName = getTabDisplayName(fileTypeHandler);
    tabItem.setText(Const.NVL(displayName, ""));
    tabItem.setToolTipText(Const.NVL(fileTypeHandler.getFilename(), ""));
    tabItem.setImage(getFileTypeImage(fileTypeHandler.getFileType()));
    tabItem.setData(fileTypeHandler);

    // Set the tab bold if the file has changed and vice-versa
    //
    fileTypeHandler.addContentChangedListener(
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

    // Create file content area
    //
    Composite composite = new Composite(tabFolder, SWT.NONE);
    FormLayout layoutComposite = new FormLayout();
    layoutComposite.marginWidth = PropsUi.getFormMargin();
    layoutComposite.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layoutComposite);
    composite.setLayoutData(new FormDataBuilder().fullSize().result());
    PropsUi.setLook(composite);

    // This is usually done by the file type
    //
    fileTypeHandler.renderFile(composite);

    tabItem.setControl(composite);

    items.add(new TabItemHandler(tabItem, fileTypeHandler));

    hopGui.fileRefreshDelegate.register(fileTypeHandler.getFilename(), fileTypeHandler);

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    // Add key listeners
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    HopGui.getInstance().replaceKeyboardShortcutListeners(this.getShell(), keyHandler);

    updateGui();
  }

  /**
   * Add a new pipeline tab to the tab folder...
   *
   * @param pipelineMeta the pipeline metadata to edit
   * @return The file type handler
   */
  public IHopFileTypeHandler addPipeline(PipelineMeta pipelineMeta) throws HopException {

    // Select and show the tab item (If it's a new pipeline, the file name will be null)
    //
    TabItemHandler handler = this.findTabItemHandler(pipelineMeta.getFilename());
    if (handler != null) {
      tabFolder.setSelection(handler.getTabItem());
      tabFolder.showItem(handler.getTabItem());
      tabFolder.setFocus();
      return handler.getTypeHandler();
    }

    // Create the pipeline graph
    //
    HopGuiPipelineGraph pipelineGraph =
        new HopGuiPipelineGraph(tabFolder, hopGui, this, pipelineMeta, pipelineFileType);

    // Assign the control to the tab
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setImage(GuiResource.getInstance().getImagePipeline());
    tabItem.setText(Const.NVL(pipelineGraph.getName(), "<>"));
    tabItem.setToolTipText(pipelineGraph.getFilename());
    tabItem.setControl(pipelineGraph);
    tabItem.setData(pipelineGraph);

    items.add(new TabItemHandler(tabItem, pipelineGraph));

    // If it's a new pipeline, the file name will be null. So, ignore
    //
    if (pipelineMeta.getFilename() != null) {
      hopGui.fileRefreshDelegate.register(pipelineMeta.getFilename(), pipelineGraph);
    }

    // Update the internal variables (file specific) in the pipeline graph variables
    //
    pipelineMeta.setInternalHopVariables(pipelineGraph.getVariables());

    // Update the variables using the list of parameters
    //
    hopGui.setParametersAsVariablesInUI(pipelineMeta, pipelineGraph.getVariables());

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    try {
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          pipelineGraph.getVariables(),
          HopExtensionPoint.HopGuiNewPipelineTab.id,
          pipelineGraph);
    } catch (Exception e) {
      throw new HopException(
          "Error calling extension point plugin for plugin id "
              + HopExtensionPoint.HopGuiNewPipelineTab.id
              + " trying to handle a new pipeline tab",
          e);
    }

    // Add key listeners
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    HopGui.getInstance().replaceKeyboardShortcutListeners(this.getShell(), keyHandler);

    pipelineGraph.setFocus();

    return pipelineGraph;
  }

  /**
   * Add a new workflow tab to the tab folder...
   *
   * @param workflowMeta The workflow metadata to edit
   * @return The file type handler
   */
  public IHopFileTypeHandler addWorkflow(WorkflowMeta workflowMeta) throws HopException {

    // Select and show the tab item (If it's a new workflow, the file name will be null)
    //
    TabItemHandler handler = this.findTabItemHandler(workflowMeta.getFilename());
    if (handler != null) {
      tabFolder.setSelection(handler.getTabItem());
      tabFolder.showItem(handler.getTabItem());
      tabFolder.setFocus();
      return handler.getTypeHandler();
    }

    HopGuiWorkflowGraph workflowGraph =
        new HopGuiWorkflowGraph(tabFolder, hopGui, this, workflowMeta, workflowFileType);

    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setImage(GuiResource.getInstance().getImageWorkflow());
    tabItem.setText(Const.NVL(workflowGraph.getName(), "<>"));
    tabItem.setToolTipText(workflowGraph.getFilename());
    tabItem.setControl(workflowGraph);
    tabItem.setData(workflowGraph);

    items.add(new TabItemHandler(tabItem, workflowGraph));

    // If it's a new workflow, the file name will be null
    //
    if (workflowMeta.getFilename() != null) {
      hopGui.fileRefreshDelegate.register(workflowMeta.getFilename(), workflowGraph);
    }

    // Update the internal variables (file specific) in the workflow graph variables
    //
    workflowMeta.setInternalHopVariables(workflowGraph.getVariables());

    // Update the variables using the list of parameters
    //
    hopGui.setParametersAsVariablesInUI(workflowMeta, workflowGraph.getVariables());

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    try {
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          workflowGraph.getVariables(),
          HopExtensionPoint.HopGuiNewWorkflowTab.id,
          workflowGraph);
    } catch (Exception e) {
      throw new HopException(
          "Error calling extension point plugin for plugin id "
              + HopExtensionPoint.HopGuiNewWorkflowTab.id
              + " trying to handle a new workflow tab",
          e);
    }

    // Add key listeners
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    HopGui.getInstance().replaceKeyboardShortcutListeners(this.getShell(), keyHandler);

    workflowGraph.setFocus();

    return workflowGraph;
  }

  /** Select the corresponding file in the left-hand tree */
  private void selectInTree(String filename) {

    if (Utils.isEmpty(filename)) {
      return;
    }

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
      return new ExplorerFile(tif.name, tif.path, tif.fileType);
    }
    return null;
  }

  public IHopFileTypeHandler findFileTypeHandlerByFilename(String filename) {
    if (filename != null) {
      for (TabItemHandler item : items) {
        if (filename.equals(item.getTypeHandler().getFilename())) {
          return item.getTypeHandler();
        }
      }
    }
    return null;
  }

  public HopGuiPipelineGraph findPipeline(String logChannelId) {
    // Go over all the pipeline graphs and see if there's one that has an IPipelineEngine with the
    // given ID
    //
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof HopGuiPipelineGraph pipelineGraph) {
        IPipelineEngine<PipelineMeta> pipeline = pipelineGraph.getPipeline();
        if (pipeline != null && logChannelId.equals(pipeline.getLogChannelId())) {
          return pipelineGraph;
        }
      }
    }
    return null;
  }

  public HopGuiWorkflowGraph findWorkflow(String logChannelId) {
    // Go over all the workflow graphs and see if there's one that has an IWorkflow with the given
    // ID
    //
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof HopGuiWorkflowGraph workflowGraph) {
        IWorkflowEngine<WorkflowMeta> workflow = workflowGraph.getWorkflow();
        if (workflow != null && logChannelId.equals(workflow.getLogChannelId())) {
          return workflowGraph;
        }
      }
    }
    return null;
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    if (tabFolder.getSelectionIndex() < 0) {
      return new EmptyHopFileTypeHandler();
    }
    return (IHopFileTypeHandler) tabFolder.getSelection().getData();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(fileTypeHandler)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        HopGui.getInstance()
            .handleFileCapabilities(
                fileTypeHandler.getFileType(), fileTypeHandler.hasChanged(), false, false);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_OPEN,
      label = "i18n::ExplorerPerspective.Menu.Open",
      image = "ui/images/open.svg")
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_OPEN,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Open.Tooltip",
      image = "ui/images/arrow-right.svg")
  @GuiKeyboardShortcut(key = SWT.F3)
  @GuiOsxKeyboardShortcut(key = SWT.F3)
  public void openFile() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    openFile(selection[0]);
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_CREATE_FOLDER,
      label = "i18n::ExplorerPerspective.Menu.CreateFolder",
      image = "ui/images/folder-add.svg")
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
    if (!Utils.isEmpty(folder)) {
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
      id = TOOLBAR_ITEM_SELECT_OPENED_FILE,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.SelectOpenedFile.Tooltip",
      image = "ui/images/select-target.svg")
  @GuiKeyboardShortcut(alt = true, key = SWT.F1)
  @GuiOsxKeyboardShortcut(alt = true, key = SWT.F1)
  public void selectInTree() {
    if (tabFolder.getSelectionIndex() >= 0) {
      this.selectInTree(getActiveFileTypeHandler().getFilename());
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EXPAND_ALL,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.ExpandAll.Tooltip",
      image = "ui/images/expand-all.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_EXPAND_ALL,
      label = "i18n::ExplorerPerspective.Menu.ExpandAll",
      image = "ui/images/expand-all.svg")
  public void expandAll() {
    tree.setRedraw(false); // Stop redraw until operation complete
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      expandCollapse(tree.getTopItem(), true);
    } else {
      TreeItemFolder treeItemFolder = (TreeItemFolder) selection[0].getData();
      if (treeItemFolder != null && !treeItemFolder.loaded) {
        BusyIndicator.showWhile(
            hopGui.getDisplay(),
            () -> {
              refreshFolder(selection[0], treeItemFolder.path, treeItemFolder.depth + 1);
              treeItemFolder.loaded = true;
            });
      }
      expandCollapse(selection[0], true);
    }
    tree.setRedraw(true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COLLAPSE_ALL,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.CollapseAll.Tooltip",
      image = "ui/images/collapse-all.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_COLLAPSE_ALL,
      label = "i18n::ExplorerPerspective.Menu.CollapseAll",
      image = "ui/images/collapse-all.svg")
  public void collapsedAll() {
    tree.setRedraw(false); // Stop redraw until operation complete
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      expandCollapse(tree.getTopItem(), false);
    } else {
      expandCollapse(selection[0], false);
    }
    tree.setRedraw(true);
  }

  public void expandCollapse(TreeItem item, boolean expand) {
    TreeItem[] items = item.getItems();
    item.setExpanded(expand);
    TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, item, expand);

    for (TreeItem childItem : items) {
      if (childItem.getItemCount() >= 1) {
        expandCollapse(childItem, expand);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DELETE,
      label = "i18n::ExplorerPerspective.Menu.Delete",
      image = "ui/images/delete.svg",
      separator = true)
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "i18n::ExplorerPerspective.ToolbarElement.Delete.Tooltip",
      image = "ui/images/delete.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void deleteFile() {
    // Only handle delete if a file was the last selected item
    HopGuiSelectionTracker selectionTracker = HopGuiSelectionTracker.getInstance();
    if (!selectionTracker.isLastSelection(HopGuiSelectionTracker.SelectionType.FILE_EXPLORER)) {
      return;
    }

    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }

    deleteFile(selection[0]);
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_RENAME,
      label = "i18n::ExplorerPerspective.Menu.Rename",
      image = "ui/images/rename.svg")
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

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_COPY_NAME,
      label = "i18n::ExplorerPerspective.Menu.CopyName",
      separator = true)
  public void copyFileName() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItemFolder folder = (TreeItemFolder) selection[0].getData();
    GuiResource.getInstance().toClipboard(folder.name);
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_COPY_PATH,
      label = "i18n::ExplorerPerspective.Menu.CopyPath")
  public void copyFilePath() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }

    TreeItemFolder folder = (TreeItemFolder) selection[0].getData();
    GuiResource.getInstance().toClipboard(folder.path);
  }

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
      callPaintListeners(tree, rootItem, rootFolder, rootName);
      setTreeItemData(rootItem, rootFolder, rootName, fileType, 0, true, true);

      // Paint the top level folder only
      //
      refreshFolder(rootItem, rootFolder, 0);

      // Always expand root item when filtering
      if (!Utils.isEmpty(filterText)) {
        rootItem.setExpanded(true);
        TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, rootItem, true);
      } else {
        TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, rootItem, true);

        // When not filtering, use tree memory (but don't call it here as it will be called later)
        // The TreeMemory will be applied either by restoreTreeState() or setExpandedFromMemory()
        if (treeStateBeforeFilter == null) {
          // Only restore from memory if we're not about to restore from saved state
          TreeMemory.setExpandedFromMemory(tree, FILE_EXPLORER_TREE);
        }
      }

      tree.setRedraw(true);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.TreeRefresh.Header"),
          BaseMessages.getString(PKG, "ExplorerPerspective.Error.TreeRefresh.Message"),
          e);
    }
    updateSelection();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_HIDDEN,
      toolTip = "i18n:org.apache.hop.ui.core.vfs:HopVfsFileDialog.ShowHiddenFiles.Tooltip.Message",
      image = "ui/images/hide.svg")
  public void showHideHidden() {
    showingHiddenFiles = !showingHiddenFiles;

    ToolItem toolItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_HIDDEN);
    if (toolItem != null) {
      if (showingHiddenFiles) {
        toolItem.setImage(GuiResource.getInstance().getImageShow());
      } else {
        toolItem.setImage(GuiResource.getInstance().getImageHide());
      }
    }

    refresh();
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

          String childName = child.getName().getBaseName();
          String metadataFolder = hopGui.getVariables().getVariable(Const.HOP_METADATA_FOLDER);

          // Skip hidden files or folders
          if (!showingHiddenFiles
              && (child.isHidden()
                  || childName.startsWith(".")
                  || child.toString().contains(metadataFolder))) {
            continue;
          }

          if (child.isFolder() != folder) {
            continue;
          }

          // Apply filter if search text is not empty
          if (!Utils.isEmpty(filterText)
              && !childName.toLowerCase().contains(filterText)
              && !hasMatchingDescendant(child)) {
            continue;
          }

          String childPath = HopVfs.getFilename(child);

          IHopFileType fileType = getFileType(childPath);
          TreeItem childItem = new TreeItem(item, SWT.NONE);
          childItem.setText(childName);
          setItemImage(childItem, fileType);
          callPaintListeners(tree, childItem, childPath, childName);
          setTreeItemData(childItem, childPath, childName, fileType, depth, folder, true);

          // Recursively add children
          //
          if (child.isFolder()) {
            // What is the maximum depth to lazily load?
            //
            String maxDepthString =
                ExplorerPerspectiveConfigSingleton.getConfig().getLazyLoadingDepth();
            int maxDepth = Const.toInt(hopGui.getVariables().resolve(maxDepthString), 0);

            // If filtering is active, we want to load and expand more to show matches
            boolean isFiltering = !Utils.isEmpty(filterText);

            if (depth + 1 <= maxDepth || isFiltering) {
              // Remember folder data to expand easily
              //
              childItem.setData(
                  new TreeItemFolder(
                      childItem, childPath, childName, fileType, depth, folder, true));

              // We actually load the content up to the desired depth
              //
              refreshFolder(childItem, childPath, depth + 1);

              // Auto-expand if filtering and this folder has visible children
              if (isFiltering && childItem.getItemCount() > 0) {
                childItem.setExpanded(true);
                TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, childItem, true);
              }
            } else {
              // Remember folder data to expand easily
              //
              childItem.setData(
                  new TreeItemFolder(
                      childItem, childPath, childName, fileType, depth, folder, false));

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

  private void callPaintListeners(Tree tree, TreeItem treeItem, String path, String name) {
    for (IExplorerFilePaintListener filePaintListener : filePaintListeners) {
      filePaintListener.filePainted(tree, treeItem, path, name);
    }
  }

  /**
   * Save the current expanded/collapsed state of all tree items before filtering. This allows us to
   * restore the exact state when the filter is cleared.
   */
  private void saveTreeState() {
    treeStateBeforeFilter = new HashMap<>();
    if (tree != null && !tree.isDisposed()) {
      for (TreeItem item : tree.getItems()) {
        saveTreeItemState(item);
      }
    }
  }

  /** Recursively save the expanded state of a tree item and its children. */
  private void saveTreeItemState(TreeItem item) {
    if (item == null || item.isDisposed()) {
      return;
    }

    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null && tif.path != null) {
      treeStateBeforeFilter.put(tif.path, item.getExpanded());
    }

    // Recursively save children
    for (TreeItem child : item.getItems()) {
      saveTreeItemState(child);
    }
  }

  /**
   * Restore the tree state that was saved before filtering started. This is called after the tree
   * is refreshed when the filter is cleared.
   */
  private void restoreTreeState() {
    if (treeStateBeforeFilter != null && tree != null && !tree.isDisposed()) {
      tree.setRedraw(false);
      try {
        for (TreeItem item : tree.getItems()) {
          restoreTreeItemState(item);
        }
      } finally {
        tree.setRedraw(true);
        treeStateBeforeFilter = null; // Clear the saved state
      }
    }
  }

  /** Recursively restore the expanded state of a tree item and its children. */
  private void restoreTreeItemState(TreeItem item) {
    if (item == null || item.isDisposed()) {
      return;
    }

    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null && tif.path != null && treeStateBeforeFilter.containsKey(tif.path)) {
      boolean wasExpanded = treeStateBeforeFilter.get(tif.path);

      // If it should be expanded but has a dummy child, load it first
      if (wasExpanded && !tif.loaded && item.getItemCount() == 1) {
        TreeItem firstChild = item.getItem(0);
        if (firstChild.getData() == null) {
          // This is a dummy item, load the folder contents
          refreshFolder(item, tif.path, tif.depth + 1);
          tif.loaded = true;
        }
      }

      item.setExpanded(wasExpanded);
      TreeMemory.getInstance().storeExpanded(FILE_EXPLORER_TREE, item, wasExpanded);
    }

    // Recursively restore children
    for (TreeItem child : item.getItems()) {
      restoreTreeItemState(child);
    }
  }

  /**
   * Check if a folder has any descendants (files or folders) that match the filter text. This
   * allows parent folders to be shown if any of their children match the filter.
   *
   * @param folder The folder to check
   * @return true if the folder or any of its descendants match the filter
   */
  private boolean hasMatchingDescendant(FileObject folder) {
    return hasMatchingDescendant(folder, 0, 10); // Limit search depth to 10 levels
  }

  /**
   * Check if a folder has any descendants (files or folders) that match the filter text.
   *
   * @param folder The folder to check
   * @param currentDepth The current recursion depth
   * @param maxDepth The maximum depth to search
   * @return true if the folder or any of its descendants match the filter
   */
  private boolean hasMatchingDescendant(FileObject folder, int currentDepth, int maxDepth) {
    if (Utils.isEmpty(filterText)) {
      return true;
    }

    // Limit recursion depth to avoid performance issues
    if (currentDepth >= maxDepth) {
      return false;
    }

    try {
      if (!folder.isFolder()) {
        return false;
      }

      FileObject[] children = folder.getChildren();
      for (FileObject child : children) {
        String childName = child.getName().getBaseName();

        // Skip hidden files if needed
        if (!showingHiddenFiles && (child.isHidden() || childName.startsWith("."))) {
          continue;
        }

        // Check if this child matches
        if (childName.toLowerCase().contains(filterText)) {
          return true;
        }

        // Recursively check descendants
        if (child.isFolder() && hasMatchingDescendant(child, currentDepth + 1, maxDepth)) {
          return true;
        }
      }
    } catch (Exception e) {
      // If there's an error reading the folder, assume no match
      return false;
    }

    return false;
  }

  /** Update de tab name, tooltip and set the tab bold if the file has changed and vice versa. */
  public void updateTabItem(IHopFileTypeHandler fileTypeHandler) {
    TabItemHandler tabItemHandler = this.getTabItemHandler(fileTypeHandler);
    if (tabItemHandler != null) {
      CTabItem tabItem = tabItemHandler.getTabItem();
      if (!tabItem.isDisposed()) {
        String displayName = getTabDisplayName(fileTypeHandler);
        tabItem.setText(Const.NVL(displayName, "<>"));
        tabItem.setToolTipText(Const.NVL(fileTypeHandler.getFilename(), ""));
        Font font =
            fileTypeHandler.hasChanged()
                ? GuiResource.getInstance().getFontBold()
                : tabFolder.getFont();
        tabItem.setFont(font);
      }
    }
  }

  /**
   * Get a display name for a tab, extracting a short title from URLs if needed.
   *
   * @param fileTypeHandler The file type handler
   * @return A short display name (max 30 characters for URLs)
   */
  private String getTabDisplayName(IHopFileTypeHandler fileTypeHandler) {
    String name = fileTypeHandler.getName();
    String filename = fileTypeHandler.getFilename();

    // If the filename is a URL and the name is the full URL or very long, extract a better title
    if (filename != null
        && (filename.toLowerCase().startsWith("http://")
            || filename.toLowerCase().startsWith("https://"))
        && (name == null || name.equals(filename) || name.length() > 30)) {
      return extractTitleFromUrl(filename);
    }

    return name;
  }

  /**
   * Extract a meaningful title from a URL for use as a tab name.
   *
   * @param url The URL to extract a title from
   * @return A short, meaningful title (max 30 characters)
   */
  private String extractTitleFromUrl(String url) {
    try {
      // Remove protocol and query parameters
      String path = url;
      if (path.contains("?")) {
        path = path.substring(0, path.indexOf("?"));
      }
      if (path.contains("#")) {
        path = path.substring(0, path.indexOf("#"));
      }

      // Extract the last meaningful part of the path
      // e.g., "https://hop.apache.org/manual/latest/pipelines/transforms/data-grid.html"
      // becomes "Data Grid"
      String[] parts = path.split("/");
      String lastPart = "";
      for (int i = parts.length - 1; i >= 0; i--) {
        if (!parts[i].isEmpty() && !parts[i].equals("manual") && !parts[i].equals("latest")) {
          lastPart = parts[i];
          break;
        }
      }

      // Remove file extension and decode
      if (lastPart.endsWith(".html") || lastPart.endsWith(".htm")) {
        lastPart = lastPart.substring(0, lastPart.lastIndexOf("."));
      }

      // Convert kebab-case, snake_case, or camelCase to Title Case
      // e.g., "data-grid" -> "Data Grid", "data_grid" -> "Data Grid"
      String title = lastPart.replaceAll("[-_]", " ");
      title = title.replaceAll("([a-z])([A-Z])", "$1 $2"); // camelCase

      // Capitalize words
      String[] words = title.split("\\s+");
      StringBuilder result = new StringBuilder();
      for (String word : words) {
        if (!word.isEmpty()) {
          if (!result.isEmpty()) {
            result.append(" ");
          }
          result.append(word.substring(0, 1).toUpperCase());
          if (word.length() > 1) {
            result.append(word.substring(1).toLowerCase());
          }
        }
      }
      title = result.toString();

      // If we got a meaningful title, use it (limit to 30 chars)
      if (!title.isEmpty() && title.length() <= 30) {
        return title;
      }

      // Fallback: show domain + last part (truncated to 30 chars)
      if (title.length() > 30) {
        title = title.substring(0, 27) + "...";
      }

      // If still no good title, use smart truncation of the full URL
      if (title.isEmpty() || title.length() < 5) {
        // Show domain and last path segment
        int domainEnd = path.indexOf("/", 8); // After "https://"
        if (domainEnd > 0 && domainEnd < path.length() - 1) {
          String domain = path.substring(0, domainEnd);
          String pathPart = path.substring(domainEnd);
          if (pathPart.length() > 20) {
            pathPart = "..." + pathPart.substring(pathPart.length() - 17);
          }
          title = domain + pathPart;
        } else {
          title = path;
        }
        if (title.length() > 30) {
          title = title.substring(0, 27) + "...";
        }
      }

      return title;
    } catch (Exception e) {
      // Fallback to simple truncation if anything goes wrong
      if (url.length() > 30) {
        return url.substring(0, 27) + "...";
      }
      return url;
    }
  }

  /** Update tree item */
  public void updateTreeItem(IHopFileTypeHandler fileTypeHandler) {
    // If no filename, no need to update the tree item
    String filename = fileTypeHandler.getFilename();
    if (filename != null) {

      // TODO: Check if it's really needed normalize
      // Normalize the filename to an absolute path
      try {
        filename = HopVfs.normalize(filename);
      } catch (HopFileException e) {
        hopGui.getLog().logError("Error getting VFS fileObject ''{0}''", filename);
      }

      // Look in the whole tree for the file...
      //
      TreeItem item = findTreeItem(filename);
      if (item != null) {
        for (IExplorerRefreshListener listener : refreshListeners) {
          listener.beforeRefresh();
        }
        callPaintListeners(tree, item, filename, fileTypeHandler.getName());
      }
    }
  }

  private TreeItem findTreeItem(String filename) {
    for (TreeItem item : tree.getItems()) {
      TreeItem found = findTreeItem(item, filename);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private TreeItem findTreeItem(TreeItem item, String filename) {
    TreeItemFolder tif = (TreeItemFolder) item.getData();
    if (tif != null && tif.path.equals(filename)) {
      return tif.treeItem;
    }
    for (TreeItem child : item.getItems()) {
      TreeItem found = findTreeItem(child, filename);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  public void updateSelection() {

    TreeItemFolder tif = null;

    if (tree.getSelectionCount() > 0) {
      TreeItem selectedItem = tree.getSelection()[0];
      tif = (TreeItemFolder) selectedItem.getData();
      if (tif == null) {
        return;
      }
      // Track that a file was selected
      HopGuiSelectionTracker.getInstance()
          .setLastSelectionType(HopGuiSelectionTracker.SelectionType.FILE_EXPLORER);
    }

    boolean isFolderSelected = tif != null && tif.fileType instanceof FolderFileType;

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CREATE_FOLDER, isFolderSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_OPEN, tif != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, tif != null);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, tif != null);

    menuWidgets.enableMenuItem(CONTEXT_MENU_CREATE_FOLDER, isFolderSelected);
    menuWidgets.enableMenuItem(CONTEXT_MENU_OPEN, tif != null);
    menuWidgets.enableMenuItem(CONTEXT_MENU_DELETE, tif != null);
    menuWidgets.enableMenuItem(CONTEXT_MENU_RENAME, tif != null);
    menuWidgets.enableMenuItem(CONTEXT_MENU_COPY_NAME, tif != null);
    menuWidgets.enableMenuItem(CONTEXT_MENU_COPY_PATH, tif != null);

    for (IExplorerSelectionListener listener : selectionListeners) {
      listener.fileSelected();
    }
  }

  /**
   * Remove the file type handler from this perspective, from the tab folder. This simply tries to
   * remove the item, does not
   *
   * @param fileTypeHandler The file type handler to remove
   * @return true if the handler was removed from the perspective, false if it wasn't (canceled, not
   *     possible, ...)
   */
  @Override
  public boolean remove(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler.isCloseable()) {
      TabItemHandler item = this.getTabItemHandler(fileTypeHandler);
      removeTabItem(item);
      return true;
    }
    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
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

  /** Update HOP GUI menu and toolbar... */
  public void updateGui() {
    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    activeHandler.updateGui();
  }

  /** Notify the zoom handler when tab is switched (for web/RAP) */
  private void notifyZoomHandlerForActiveTab() {
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    if (activeHandler == null) {
      return;
    }

    // Check if it's a pipeline or workflow graph and notify its zoom handler
    if (activeHandler instanceof HopGuiPipelineGraph pipelineGraph) {
      Object zoomHandler = pipelineGraph.getCanvasZoomHandler();
      if (zoomHandler != null) {
        CanvasZoomHelper.notifyCanvasReady(zoomHandler);
      }
    } else if (activeHandler instanceof HopGuiWorkflowGraph workflowGraph) {
      Object zoomHandler = workflowGraph.getCanvasZoomHandler();
      if (zoomHandler != null) {
        CanvasZoomHelper.notifyCanvasReady(zoomHandler);
      }
    }
  }

  /**
   * Toggle the visibility of the file explorer panel (tree). When hidden, the tab folder is
   * maximized. When shown, normal sash weights are restored.
   */
  public void toggleFileExplorerPanel() {
    if (sash == null || sash.isDisposed()) {
      return;
    }

    fileExplorerPanelVisible = !fileExplorerPanelVisible;

    if (fileExplorerPanelVisible) {
      // Show the file explorer panel - restore normal layout
      sash.setMaximizedControl(null);
      sash.setWeights(20, 80);
    } else {
      // Hide the file explorer panel - maximize the tab folder
      sash.setMaximizedControl(tabFolder);
    }
  }

  /**
   * Check if the file explorer panel is currently visible.
   *
   * @return true if the file explorer panel is visible, false otherwise
   */
  public boolean isFileExplorerPanelVisible() {
    return fileExplorerPanelVisible;
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

  private static class TreeItemFolder {
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
