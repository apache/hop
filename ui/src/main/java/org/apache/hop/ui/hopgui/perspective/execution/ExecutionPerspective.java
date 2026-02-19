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

package org.apache.hop.ui.hopgui.perspective.execution;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TabFolderReorder;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.w3c.dom.Node;

@HopPerspectivePlugin(
    id = "150-HopExecutionPerspective",
    name = "i18n::ExecutionPerspective.Name",
    description = "i18n::ExecutionPerspective.Description",
    image = "ui/images/execution.svg",
    documentationUrl = "/hop-gui/perspective-execution-information.html")
@GuiPlugin(
    name = "i18n::ExecutionPerspective.Name",
    description = "i18n::ExecutionPerspective.Description")
public class ExecutionPerspective implements IHopPerspective, TabClosable {

  public static final Class<?> PKG = ExecutionPerspective.class; // i18n
  private static final String EXECUTION_PERSPECTIVE_TREE = "Execution perspective tree";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ExecutionPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_EDIT = "ExecutionPerspective-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE =
      "ExecutionPerspective-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "ExecutionPerspective-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_REFRESH = "ExecutionPerspective-Toolbar-10100-Refresh";

  public static final String KEY_HELP = "Help";
  public static final String CONST_ERROR = "error";
  public static final String CONST_ERROR1 = "Error";

  private static ExecutionPerspective instance;

  public static ExecutionPerspective getInstance() {
    return instance;
  }

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private CTabFolder tabFolder;
  private Control toolBar;
  private GuiToolbarWidgets toolBarWidgets;

  private List<IExecutionViewer> viewers = new ArrayList<>();

  private Map<String, ExecutionInfoLocation> locationMap;

  public ExecutionPerspective() {
    instance = this;
  }

  @Override
  public String getId() {
    return "execution-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'i', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'i', global = true)
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    // Automatically refresh.
    //
    this.refresh();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;
    this.locationMap = new HashMap<>();

    // Split tree and tab folder
    //
    sash = new SashForm(parent, SWT.HORIZONTAL);
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    createTree(sash);
    createTabFolder(sash);

    sash.setWeights(new int[] {20, 80});

    this.refresh();

    // Set the top level items in the tree to be expanded
    //
    for (TreeItem item : tree.getItems()) {
      TreeMemory.getInstance().storeExpanded(EXECUTION_PERSPECTIVE_TREE, item, true);
    }

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
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBar = toolBarContainer.getControl();
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(0, 0);
    layoutData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(layoutData);
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    tree = new Tree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    tree.addListener(
        SWT.DefaultSelection,
        event -> {
          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null) {
            onNewViewer();
          }
        });

    PropsUi.setLook(tree);

    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment(0, 0);
    treeFormData.top = new FormAttachment(toolBar, 0);
    treeFormData.right = new FormAttachment(100, 0);
    treeFormData.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(treeFormData);

    // Remember tree node expanded/Collapsed
    TreeMemory.addTreeListener(tree, EXECUTION_PERSPECTIVE_TREE);
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
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    // Show/Hide tree
    //
    ToolBar toolBar = new ToolBar(tabFolder, SWT.FLAT);
    final ToolItem item = new ToolItem(toolBar, SWT.PUSH);
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
    tabFolder.setTopRight(toolBar, SWT.RIGHT);
    int height = toolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    tabFolder.setTabHeight(Math.max(height, tabFolder.getTabHeight()));

    new TabCloseHandler(this);

    // Support reorder tab item
    //
    new TabFolderReorder(tabFolder);
  }

  public void addViewer(IExecutionViewer viewer) {
    // Create tab item
    //
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(viewer.getName());
    tabItem.setImage(viewer.getTitleImage());
    tabItem.setToolTipText(viewer.getTitleToolTip());

    tabItem.setControl(viewer.getControl());
    tabItem.setData(viewer);

    viewers.add(viewer);

    // Activate the perspective
    //
    this.activate();

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    viewer.setFocus();

    viewer.refresh();
  }

  /**
   * Find a metadata editor
   *
   * @param logChannelId the ID of the execution (log channel)
   * @param name the name of the workflow or pipeline which is executing
   * @return the metadata editor or null if not found
   */
  public IExecutionViewer findViewer(String logChannelId, String name) {
    if (logChannelId == null || name == null) return null;

    for (IExecutionViewer viewer : viewers) {
      if (logChannelId.equals(viewer.getLogChannelId()) && name.equals(viewer.getName())) {
        return viewer;
      }
    }
    return null;
  }

  public void setActiveViewer(IExecutionViewer viewer) {
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(viewer)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        viewer.setFocus();
      }
    }
  }

  public IExecutionViewer getActiveViewer() {
    if (tabFolder.getSelectionIndex() < 0) {
      return null;
    }

    return (IExecutionViewer) tabFolder.getSelection().getData();
  }

  protected void onTabClose(CTabFolderEvent event) {
    CTabItem tabItem = (CTabItem) event.item;
    closeTab(event, tabItem);
  }

  public void onNewViewer() {

    try {
      if (tree.getSelectionCount() != 1) {
        return;
      }

      TreeItem treeItem = tree.getSelection()[0];
      if (treeItem != null) {
        if (treeItem.getData() instanceof Execution execution) {
          ExecutionInfoLocation location =
              (ExecutionInfoLocation) treeItem.getParentItem().getData();
          ExecutionState executionState =
              location.getExecutionInfoLocation().getExecutionState(execution.getId());
          createExecutionViewer(location.getName(), execution, executionState);
        } else if (treeItem.getData(CONST_ERROR) instanceof Exception exception) {
          new ErrorDialog(getShell(), CONST_ERROR1, "Location error:", exception);
        }
      }
    } catch (Exception e) {
      getShell().setCursor(null);
      new ErrorDialog(getShell(), CONST_ERROR1, "Error showing viewer for execution", e);
    }
  }

  public void createExecutionViewer(
      String locationName, Execution execution, ExecutionState executionState) throws Exception {
    Cursor busyCursor = getBusyCursor();

    try {
      if (locationName == null || execution == null) {
        return;
      }
      getShell().setCursor(busyCursor);

      // See if the viewer is already active...
      //
      IExecutionViewer active = findViewer(execution.getId(), execution.getName());
      if (active != null) {
        setActiveViewer(active);
        return;
      }

      // Load metadata
      IHopMetadataProvider provider = new SerializableMetadataProvider(execution.getMetadataJson());
      IVariables variables = Variables.getADefaultVariableSpace();
      variables.setVariables(execution.getVariableValues());

      switch (execution.getExecutionType()) {
        case Pipeline:
          {
            Node pipelineNode =
                XmlHandler.loadXmlString(execution.getExecutorXml(), PipelineMeta.XML_TAG);
            PipelineMeta pipelineMeta = new PipelineMeta(pipelineNode, provider);
            PipelineExecutionViewer viewer =
                new PipelineExecutionViewer(
                    tabFolder, hopGui, pipelineMeta, locationName, this, execution, executionState);
            addViewer(viewer);
          }
          break;
        case Workflow:
          {
            Node workflowNode =
                XmlHandler.loadXmlString(execution.getExecutorXml(), WorkflowMeta.XML_TAG);
            WorkflowMeta workflowMeta = new WorkflowMeta(workflowNode, provider, variables);
            WorkflowExecutionViewer viewer =
                new WorkflowExecutionViewer(
                    tabFolder, hopGui, workflowMeta, locationName, this, execution, executionState);
            addViewer(viewer);
          }
          break;
        default:
          break;
      }
    } finally {
      getShell().setCursor(null);
      busyCursor.dispose();
    }
  }

  private Cursor getBusyCursor() {
    return new Cursor(getShell().getDisplay(), SWT.CURSOR_WAIT);
  }

  /**
   * Simply search the tree items to look for the first matching pipeline execution
   *
   * @param locationName the exec info location to use
   * @param executionType The type of execution to look for
   * @param name The name of the pipeline
   */
  public void createLastExecutionView(String locationName, ExecutionType executionType, String name)
      throws Exception {
    try {
      ExecutionInfoLocation location = locationMap.get(locationName);
      if (location == null) {
        return;
      }
      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      Execution execution = iLocation.findLastExecution(executionType, name);

      // Ignore if never executed
      if (execution != null) {
        ExecutionState executionState = iLocation.getExecutionState(execution.getId());
        createExecutionViewer(location.getName(), execution, executionState);
      }
    } catch (Exception e) {
      new ErrorDialog(
          getShell(), CONST_ERROR1, "Error opening view on last execution information", e);
    }
  }

  public void updateGui() {
    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    hopGui
        .getDisplay()
        .asyncExec(
            () ->
                hopGui.handleFileCapabilities(
                    activeHandler.getFileType(), activeHandler.hasChanged(), false, false));
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::ExecutionPerspective.ToolbarElement.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    Cursor busyCursor = getBusyCursor();

    try {
      getShell().setCursor(busyCursor);

      // If there are any cached locations we want to close them before initializing new ones
      //
      for (ExecutionInfoLocation location : locationMap.values()) {
        location.getExecutionInfoLocation().close();
      }
      locationMap.clear();

      tree.setRedraw(false);
      tree.removeAll();

      // top level: the execution information locations
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          metadataProvider.getSerializer(ExecutionInfoLocation.class);

      List<ExecutionInfoLocation> locations = serializer.loadAll();
      Collections.sort(locations, Comparator.comparing(HopMetadataBase::getName));

      for (ExecutionInfoLocation location : locations) {
        IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

        try {
          // Initialize the location first...
          //
          iLocation.initialize(hopGui.getVariables(), hopGui.getMetadataProvider());

          // Keep the location around to close at the next refresh.
          //
          locationMap.put(location.getName(), location);

          TreeItem locationItem = new TreeItem(tree, SWT.NONE);
          locationItem.setText(0, Const.NVL(location.getName(), ""));
          locationItem.setImage(GuiResource.getInstance().getImageLocation());
          TreeMemory.getInstance().storeExpanded(EXECUTION_PERSPECTIVE_TREE, locationItem, true);
          locationItem.setData(location);

          try {

            // Get the data in the location
            //
            List<String> ids = iLocation.getExecutionIds(false, 100);

            // Display the executions
            //
            for (String id : ids) {
              try {
                Execution execution = iLocation.getExecution(id);
                if (execution != null) {
                  TreeItem executionItem = new TreeItem(locationItem, SWT.NONE);
                  switch (execution.getExecutionType()) {
                    case Pipeline:
                      decoratePipelineTreeItem(executionItem, execution);
                      break;
                    case Workflow:
                      decorateWorkflowTreeItem(executionItem, execution);
                      break;
                    default:
                      break;
                  }
                }
              } catch (Exception e) {
                TreeItem errorItem = new TreeItem(locationItem, SWT.NONE);
                errorItem.setText("Error reading " + id + " (double click for details)");
                errorItem.setForeground(GuiResource.getInstance().getColorRed());
                errorItem.setData(CONST_ERROR, e);
                errorItem.setImage(GuiResource.getInstance().getImageError());
              }
            }
          } catch (Exception e) {
            // Error contacting location
            //
            TreeItem errorItem = new TreeItem(locationItem, SWT.NONE);
            errorItem.setText("Not reachable (double click for details)");
            errorItem.setForeground(GuiResource.getInstance().getColorRed());
            errorItem.setData(CONST_ERROR, e);
            errorItem.setImage(GuiResource.getInstance().getImageError());
          }
        } catch (Exception e) {
          // We couldn't initialize a location
          //
          TreeItem locationItem = new TreeItem(tree, SWT.NONE);
          locationItem.setText(
              0, Const.NVL(location.getName(), "") + " (error: double click for details)");
          locationItem.setForeground(GuiResource.getInstance().getColorRed());
          locationItem.setImage(GuiResource.getInstance().getImageLocation());
          locationItem.setData(CONST_ERROR, e);
        }
      }

      TreeUtil.setOptimalWidthOnColumns(tree);
      TreeMemory.setExpandedFromMemory(tree, EXECUTION_PERSPECTIVE_TREE);

      tree.setRedraw(true);
    } catch (Exception e) {
      getShell().setCursor(null);
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "ExecutionPerspective.Refresh.Error.Header"),
          BaseMessages.getString(PKG, "ExecutionPerspective.Refresh.Error.Message"),
          e);
    } finally {
      getShell().setCursor(null);
    }
  }

  private void decoratePipelineTreeItem(TreeItem executionItem, Execution execution) {
    try {
      executionItem.setImage(GuiResource.getInstance().getImagePipeline());

      String label = execution.getName();
      label +=
          " - "
              + new SimpleDateFormat("yyyy/MM/dd HH:mm").format(execution.getExecutionStartDate());
      executionItem.setText(label);
      executionItem.setData(execution);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(), CONST_ERROR1, "Error drawing pipeline execution information tree item", e);
    }
  }

  private void decorateWorkflowTreeItem(TreeItem executionItem, Execution execution) {
    try {
      executionItem.setImage(GuiResource.getInstance().getImageWorkflow());

      String label = execution.getName();
      label +=
          " - "
              + new SimpleDateFormat("yyyy/MM/dd HH:mm").format(execution.getExecutionStartDate());
      executionItem.setText(label);
      executionItem.setData(execution);
    } catch (Exception e) {
      new ErrorDialog(
          getShell(), CONST_ERROR1, "Error drawing workflow execution information tree item", e);
    }
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (typeHandler instanceof MetadataEditor) {
      MetadataEditor<?> editor = (MetadataEditor<?>) typeHandler;

      if (editor.isCloseable()) {

        viewers.remove(editor);

        for (CTabItem item : tabFolder.getItems()) {
          if (editor.equals(item.getData())) {
            item.dispose();
          }
        }
      }
    }

    return false;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "i18n::ExecutionPerspective.ToolbarElement.Delete.Tooltip",
      image = "ui/images/delete.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void delete() {
    try {
      if (tree.getSelectionCount() != 1) {
        return;
      }
      TreeItem item = tree.getSelection()[0];
      Object itemData = item.getData();
      if (itemData instanceof ExecutionInfoLocation location) {
        // Delete the whole location
        //
        MessageBox box = new MessageBox(getShell(), SWT.APPLICATION_MODAL | SWT.NO | SWT.YES);
        box.setText("Confirm delete");
        box.setMessage("Are you sure you want to delete all information in this location?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return;
        }

        IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
        List<String> executionIds = iLocation.getExecutionIds(false, 0);
        for (int i = executionIds.size() - 1; i >= 0; i--) {
          iLocation.deleteExecution(executionIds.get(i));
        }
        refresh();
      } else if (itemData instanceof Execution execution) {
        // Delete one execution: do not ask for confirmation
        //
        TreeItem parentItem = item.getParentItem();
        ExecutionInfoLocation location = (ExecutionInfoLocation) parentItem.getData();
        IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
        iLocation.deleteExecution(execution.getId());
        refresh();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR1, "Error deleting location(s)", e);
    }
  }

  @Override
  public void navigateToPreviousFile() {
    if (hasNavigationPreviousFile()) {
      int index = tabFolder.getSelectionIndex() - 1;
      if (index < 0) {
        index = tabFolder.getItemCount() - 1;
      }
      tabFolder.setSelection(index);
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
    }
  }

  public boolean hasNavigationPreviousFile() {
    return tabFolder.getItemCount() > 1;
  }

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

  /**
   * Gets locationMap
   *
   * @return value of locationMap
   */
  public Map<String, ExecutionInfoLocation> getLocationMap() {
    return locationMap;
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    IExecutionViewer viewer = (IExecutionViewer) tabItem.getData();

    boolean isRemoved = viewers.remove(viewer);
    tabItem.dispose();

    if (!isRemoved && event != null) {
      event.doit = false;
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
}
