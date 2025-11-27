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
 *
 */

package org.apache.hop.ui.hopgui.perspective.execution;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.tab.GuiTabItem;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateComponentMetrics;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.CanvasFacade;
import org.apache.hop.ui.hopgui.CanvasListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.shared.BaseExecutionViewer;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.w3c.dom.Node;

@GuiPlugin
public class PipelineExecutionViewer extends BaseExecutionViewer
    implements IExecutionViewer, PaintListener, MouseListener {
  private static final Class<?> PKG = PipelineExecutionViewer.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "PipelineExecutionViewer-Toolbar";

  public static final String TOOLBAR_ITEM_REFRESH = "PipelineExecutionViewer-Toolbar-10100-Refresh";
  public static final String TOOLBAR_ITEM_ZOOM_LEVEL =
      "PipelineExecutionViewer-ToolBar-10500-Zoom-Level";
  public static final String TOOLBAR_ITEM_ZOOM_FIT_TO_SCREEN =
      "PipelineExecutionViewer-ToolBar-10600-Zoom-Fit-To-Screen";
  public static final String TOOLBAR_ITEM_TO_EDITOR =
      "PipelineExecutionViewer-Toolbar-11100-GoToEditor";
  public static final String TOOLBAR_ITEM_DRILL_DOWN =
      "PipelineExecutionViewer-Toolbar-11200-DrillDown";
  public static final String TOOLBAR_ITEM_GO_UP = "PipelineExecutionViewer-Toolbar-11300-GoUp";
  public static final String TOOLBAR_ITEM_VIEW_EXECUTOR =
      "PipelineExecutionViewer-Toolbar-12000-ViewExecutor";
  public static final String TOOLBAR_ITEM_VIEW_METADATA =
      "PipelineExecutionViewer-Toolbar-12100-ViewMetadata";

  public static final String PIPELINE_EXECUTION_VIEWER_TABS = "PipelineExecutionViewer.Tabs.ID";
  public static final String CONST_ERROR = "Error";

  protected final PipelineMeta pipelineMeta;

  protected TransformMeta selectedTransform;
  private ExecutionData selectedTransformData;

  private CTabItem infoTab;
  private TableView infoView;
  private CTabItem logTab;
  private CTabItem dataTab;
  private SashForm dataSash;
  private org.eclipse.swt.widgets.List dataList;
  private TableView dataView;
  private CTabItem metricsTab;
  private TableView metricsView;

  public PipelineExecutionViewer(
      Composite parent,
      HopGui hopGui,
      PipelineMeta pipelineMeta,
      String locationName,
      ExecutionPerspective perspective,
      Execution execution,
      ExecutionState executionState) {
    super(parent, hopGui, perspective, locationName, execution, executionState);
    this.pipelineMeta = pipelineMeta;

    // Calculate the pipeline size only once since the metadata is read-only
    //
    this.maximum = pipelineMeta.getMaximum();

    addWidgets();
  }

  /** Add the widgets in the execution perspective parent tab folder */
  public void addWidgets() {
    setLayout(new FormLayout());

    // A toolbar at the top
    //
    toolBar = new ToolBar(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
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

    // Below the toolbar we have a horizontal splitter: Canvas above and Execution tabs below
    //
    sash = new SashForm(this, SWT.VERTICAL);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(toolBar, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    sash.setLayoutData(fdSash);

    // In this sash we have a canvas at the top and a tab folder with information at the bottom
    //
    // The canvas at the top
    //
    canvas = new Canvas(sash, SWT.NO_BACKGROUND | SWT.BORDER);
    Listener listener = CanvasListener.getInstance();
    canvas.addListener(SWT.MouseDown, listener);
    canvas.addListener(SWT.MouseMove, listener);
    canvas.addListener(SWT.MouseUp, listener);
    canvas.addListener(SWT.Paint, listener);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    canvas.setLayoutData(fdCanvas);
    canvas.addPaintListener(this);
    canvas.addMouseListener(this);
    if (!EnvironmentUtils.getInstance().isWeb()) {
      canvas.addMouseMoveListener(this);
      canvas.addMouseWheelListener(this::mouseScrolled);
    }

    // The execution information tabs at the bottom
    //
    tabFolder = new CTabFolder(sash, SWT.MULTI);
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    addInfoTab();
    addLogTab();
    addMetricsTab();
    addDataTab();
    addPluginTabs();

    layout();

    refresh();

    // Add keyboard listeners from the main GUI and this class (toolbar etc) to the canvas. That's
    // where the focus should be
    //
    hopGui.replaceKeyboardShortcutListeners(this);

    tabFolder.setSelection(0);
    sash.setWeights(60, 40);
  }

  private void addInfoTab() {
    infoTab = new CTabItem(tabFolder, SWT.NONE);
    infoTab.setFont(GuiResource.getInstance().getFontDefault());
    infoTab.setImage(GuiResource.getInstance().getImageInfo());
    infoTab.setText(BaseMessages.getString(PKG, "PipelineExecutionViewer.InfoTab.Title"));

    ColumnInfo[] infoCols =
        new ColumnInfo[] {
          new ColumnInfo("Item", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
          new ColumnInfo("Value", ColumnInfo.COLUMN_TYPE_TEXT, false, true),
        };

    // Let's simply add a table view with all the details on it.
    //
    infoView =
        new TableView(
            hopGui.getVariables(),
            tabFolder,
            SWT.H_SCROLL | SWT.V_SCROLL,
            infoCols,
            1,
            true,
            null,
            props);
    PropsUi.setLook(infoView);

    infoTab.setControl(infoView);
  }

  private void refreshStatus() {
    try {
      infoView.clearAll();

      ExecutionInfoLocation location = perspective.getLocationMap().get(locationName);
      if (location == null) {
        return;
      }
      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      // Force re-load of the execution information
      //
      iLocation.unBuffer(execution.getId());

      // Don't load execution logging text to prevent memory issues.
      //
      executionState = iLocation.getExecutionState(execution.getId(), false);
      if (executionState == null) {
        return;
      }

      // Calculate information staleness
      //
      String statusDescription = executionState.getStatusDescription();
      if (Pipeline.STRING_RUNNING.equalsIgnoreCase(statusDescription)
          || Pipeline.STRING_INITIALIZING.equalsIgnoreCase(statusDescription)) {
        long loggingInterval = Const.toLong(location.getDataLoggingInterval(), 20000);
        if (System.currentTimeMillis() - executionState.getUpdateTime().getTime()
            > loggingInterval) {
          // The information is stale, not getting updates!
          //
          TableItem item = infoView.add("Update state", STRING_STATE_STALE);
          item.setBackground(GuiResource.getInstance().getColorLightBlue());
          item.setForeground(GuiResource.getInstance().getColorWhite());
        }
      }

      infoView.add("Name", execution.getName());
      infoView.add("Type", execution.getExecutionType().name());
      infoView.add("Filename", execution.getFilename());
      infoView.add("ID", execution.getId());
      infoView.add("Parent ID", execution.getParentId());
      infoView.add("Registration", formatDate(execution.getRegistrationDate()));
      infoView.add("Start", formatDate(execution.getExecutionStartDate()));
      infoView.add("Type", executionState.getExecutionType().name());
      infoView.add("Status", statusDescription);
      infoView.add("Status Last updated", formatDate(executionState.getUpdateTime()));
      infoView.add("Container ID", executionState.getContainerId());

      infoView.optimizeTableView();

      try {
        ExtensionPointHandler.callExtensionPoint(
            LogChannel.UI,
            hopGui.getVariables(),
            HopGuiExtensionPoint.PipelineExecutionViewerUpdate.id,
            this);
      } catch (Exception xe) {
        LogChannel.UI.logError(
            "Error handling extension point 'PipelineExecutionViewerUpdate'", xe);
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error refreshing pipeline status", e);
    }
  }

  private void addPluginTabs() {
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    List<GuiTabItem> tabsList = guiRegistry.getGuiTabsMap().get(PIPELINE_EXECUTION_VIEWER_TABS);

    if (tabsList != null) {
      tabsList.sort(Comparator.comparing(GuiTabItem::getId));
      for (GuiTabItem tabItem : tabsList) {
        try {
          Class<?> pluginTabClass = tabItem.getMethod().getDeclaringClass();
          boolean showTab = true;
          try {
            // Invoke static method showTab(PipelineExecutionViewer)
            //
            Method showTabMethod =
                pluginTabClass.getMethod("showTab", PipelineExecutionViewer.class);
            showTab = (boolean) showTabMethod.invoke(null, this);
          } catch (NoSuchMethodException noSuchMethodException) {
            // Just show the tab
          }
          if (showTab) {
            Constructor<?> constructor =
                pluginTabClass.getConstructor(PipelineExecutionViewer.class);
            Object object = constructor.newInstance(this);
            tabItem.getMethod().invoke(object, tabFolder);
          }
        } catch (Exception e) {
          new ErrorDialog(
              hopGui.getActiveShell(),
              CONST_ERROR,
              "Hop was unable to invoke @GuiTab method "
                  + tabItem.getMethod().getName()
                  + " with the parent composite as argument",
              e);
        }
      }
      tabFolder.layout();
    }
  }

  private void addDataTab() {
    dataTab = new CTabItem(tabFolder, SWT.NONE);
    dataTab.setFont(GuiResource.getInstance().getFontDefault());
    dataTab.setImage(GuiResource.getInstance().getImageData());
    dataTab.setText(BaseMessages.getString(PKG, "PipelineExecutionViewer.DataTab.Title"));

    dataSash = new SashForm(tabFolder, SWT.HORIZONTAL);

    // The list of available data on the left-hand side.
    //
    dataList =
        new org.eclipse.swt.widgets.List(
            dataSash, SWT.SINGLE | SWT.LEFT | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(dataList);
    dataList.addListener(SWT.Selection, e -> showDataRows());

    // An empty table view on the right.  This will be populated during a refresh.
    //
    ColumnInfo[] dataColumns = new ColumnInfo[] {};
    dataView =
        new TableView(
            hopGui.getVariables(),
            dataSash,
            SWT.H_SCROLL | SWT.V_SCROLL,
            dataColumns,
            0,
            true,
            null,
            props);
    PropsUi.setLook(dataView);

    dataView.optimizeTableView();

    dataSash.setWeights(30, 70);

    dataTab.setControl(dataSash);
  }

  private void addMetricsTab() {
    metricsTab = new CTabItem(tabFolder, SWT.NONE);
    metricsTab.setFont(GuiResource.getInstance().getFontDefault());
    metricsTab.setImage(GuiResource.getInstance().getImageShowGrid());
    metricsTab.setText(BaseMessages.getString(PKG, "PipelineExecutionViewer.MetricsTab.Title"));

    // An empty table view on the right.  This will be populated during a refresh.
    //
    ColumnInfo[] dataColumns = new ColumnInfo[] {};
    metricsView =
        new TableView(
            hopGui.getVariables(),
            tabFolder,
            SWT.H_SCROLL | SWT.V_SCROLL,
            dataColumns,
            0,
            true,
            null,
            props);
    PropsUi.setLook(metricsView);

    metricsView.optimizeTableView();

    metricsTab.setControl(metricsView);
  }

  private void refreshMetrics() {
    metricsView.clearAll();
    if (executionState != null) {
      Set<String> metricNames = new HashSet<>();
      // Get a unique list of metric names in the state
      //
      executionState.getMetrics().forEach(m -> metricNames.addAll(m.getMetrics().keySet()));
      Map<String, Integer> indexMap = new HashMap<>();

      // We display everything that makes sense:
      // name, copy, inits, input, read, written, output, updated, rejected, errors, buffer in,
      // buffer out
      //
      List<ColumnInfo> columns = new ArrayList<>();
      columns.add(new ColumnInfo("Name", ColumnInfo.COLUMN_TYPE_TEXT, false, true));
      columns.add(new ColumnInfo("Copy", ColumnInfo.COLUMN_TYPE_TEXT, false, true));

      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_INIT);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_INPUT);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_READ);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_WRITTEN);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_OUTPUT);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_UPDATED);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_REJECTED);
      addColumn(columns, indexMap, metricNames, Pipeline.METRIC_ERROR);

      metricsView =
          new TableView(
              hopGui.getVariables(),
              tabFolder,
              SWT.H_SCROLL | SWT.V_SCROLL,
              columns.toArray(new ColumnInfo[0]),
              executionState.getMetrics().size(),
              true,
              null,
              props);

      for (int i = 0; i < executionState.getMetrics().size(); i++) {
        ExecutionStateComponentMetrics metrics = executionState.getMetrics().get(i);
        TableItem item = metricsView.table.getItem(i);
        item.setText(1, Const.NVL(metrics.getComponentName(), ""));
        item.setText(2, Const.NVL(metrics.getComponentCopy(), ""));
        for (String metricHeader : metrics.getMetrics().keySet()) {
          Integer index = indexMap.get(metricHeader);
          Long value = metrics.getMetrics().get(metricHeader);
          if (value != null && index != null && index > 1 && index <= columns.size()) {
            item.setText(index, value.toString());
          }
        }
      }

      metricsTab.setControl(metricsView);
      metricsView.layout(true, true);
      metricsView.optimizeTableView();
    }
  }

  private void addColumn(
      List<ColumnInfo> columns,
      Map<String, Integer> indexMap,
      Set<String> metricNames,
      IEngineMetric metric) {
    if (metricNames.contains(metric.getHeader())) {
      columns.add(new ColumnInfo(metric.getHeader(), ColumnInfo.COLUMN_TYPE_TEXT, true, true));
      // Index +1 because of the left-hand row number
      indexMap.put(metric.getHeader(), columns.size());
    }
  }

  /** An entry is selected in the data list. Show the corresponding rows. */
  private void showDataRows() {
    int[] weights = dataSash.getWeights();
    Cursor busyCursor = new Cursor(getShell().getDisplay(), SWT.CURSOR_WAIT);

    try {
      getShell().setCursor(busyCursor);

      String[] selection = dataList.getSelection();
      if (selection.length != 1) {
        return;
      }
      String setDescription = selection[0];

      // Clear the data grid to prevent us from showing "old" data.
      //
      dataView.clearAll(false);

      // Look up the key in the metadata...
      //
      ExecutionData data = loadSelectedTransformData();

      for (ExecutionDataSetMeta setMeta : data.getSetMetaData().values()) {
        if (setDescription.equals(setMeta.getDescription())) {
          // What's the data for this metadata?
          //
          RowBuffer rowBuffer = data.getDataSets().get(setMeta.getSetKey());
          if (rowBuffer != null) {
            java.util.List<ColumnInfo> columns = new ArrayList<>();
            IRowMeta rowMeta = rowBuffer.getRowMeta();
            // Add a column for every
            for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
              ColumnInfo columnInfo =
                  new ColumnInfo(
                      valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
              columnInfo.setValueMeta(valueMeta);
              columnInfo.setToolTip(valueMeta.toStringMeta());
              columnInfo.setImage(GuiResource.getInstance().getImage(valueMeta));
              columns.add(columnInfo);
            }

            // Dispose of the old table view
            //
            dataView.dispose();

            // Create a new one
            //
            dataView =
                new TableView(
                    hopGui.getVariables(),
                    dataSash,
                    SWT.H_SCROLL | SWT.V_SCROLL,
                    columns.toArray(new ColumnInfo[0]),
                    rowBuffer.size(),
                    true,
                    null,
                    props);

            for (int r = 0; r < rowBuffer.size(); r++) {
              Object[] row = rowBuffer.getBuffer().get(r);
              TableItem item = dataView.table.getItem(r);
              item.setText(0, Integer.toString(r + 1));
              for (int c = 0; c < rowMeta.size(); c++) {
                String value = rowMeta.getString(row, c);
                if (value == null) {
                  value = "";
                }
                item.setText(c + 1, value);
              }
            }
            dataView.optWidth(true);
            break;
          }
        }
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error showing transform data rows", e);
    } finally {
      layout(true, true);
      dataSash.setWeights(weights);
      getShell().setCursor(null);
    }
  }

  private void addLogTab() {
    logTab = new CTabItem(tabFolder, SWT.NONE);
    logTab.setFont(GuiResource.getInstance().getFontDefault());
    logTab.setImage(GuiResource.getInstance().getImageShowLog());
    logTab.setText(BaseMessages.getString(PKG, "PipelineExecutionViewer.LogTab.Title"));

    loggingText = new Text(tabFolder, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.READ_ONLY);
    PropsUi.setLook(loggingText);

    logTab.setControl(loggingText);

    // When the logging tab comes into focus, re-load the logging text
    //
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          if (tabFolder.getSelection() == logTab) {
            refreshLoggingText();
          }
        });
  }

  @Override
  public Image getTitleImage() {
    return GuiResource.getInstance().getImagePipeline();
  }

  @Override
  public String getTitleToolTip() {
    return pipelineMeta.getDescription();
  }

  @Override
  public void paintControl(PaintEvent e) {
    Point area = getArea();
    if (area.x == 0 || area.y == 0) {
      return; // nothing to do!
    }

    // Do double buffering to prevent flickering on Windows
    //
    boolean needsDoubleBuffering =
        Const.isWindows() && "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());

    Image image = null;
    GC swtGc = e.gc;

    if (needsDoubleBuffering) {
      image = new Image(hopDisplay(), area.x, area.y);
      swtGc = new GC(image);
    }

    drawPipelineImage(swtGc, area.x, area.y, magnification);

    if (needsDoubleBuffering) {
      // Draw the image onto the canvas and get rid of the resources
      //
      e.gc.drawImage(image, 0, 0);
      swtGc.dispose();
      image.dispose();
    }

    setZoomLabel();
  }

  public void setZoomLabel() {
    Combo combo = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (combo == null || combo.isDisposed()) {
      return;
    }
    String newString = Math.round(magnification * 100) + "%";
    String oldString = combo.getText();
    if (!newString.equals(oldString)) {
      combo.setText(Math.round(magnification * 100) + "%");
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_FIT_TO_SCREEN,
      toolTip = "i18n::ExecutionViewer.GuiAction.ZoomFitToScreen.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-fit.svg")
  @Override
  public void zoomFitToScreen() {
    super.zoomFitToScreen();
  }

  @Override
  protected Point getArea() {
    org.eclipse.swt.graphics.Rectangle rect = canvas.getClientArea();
    return new Point(rect.width, rect.height);
  }

  public void drawPipelineImage(GC swtGc, int width, int height, float magnificationFactor) {

    IGc gc = new SwtGc(swtGc, width, height, iconSize);
    try {
      PropsUi propsUi = PropsUi.getInstance();

      int gridSize = propsUi.isShowCanvasGridEnabled() ? propsUi.getCanvasGridSize() : 1;

      PipelinePainter pipelinePainter =
          new PipelinePainter(
              gc,
              hopGui.getVariables(),
              pipelineMeta,
              new Point(width, height),
              new DPoint(0, 0),
              null,
              null,
              areaOwners,
              propsUi.getIconSize(),
              propsUi.getLineWidth(),
              gridSize,
              propsUi.getNoteFont().getName(),
              propsUi.getNoteFont().getHeight(),
              null, // No state yet
              propsUi.isIndicateSlowPipelineTransformsEnabled(),
              propsUi.getZoomFactor(),
              Collections.emptyMap(),
              false,
              null,
              Collections.emptyMap());

      // correct the magnification with the overall zoom factor
      //
      float correctedMagnification = (float) (magnificationFactor * propsUi.getZoomFactor());

      pipelinePainter.setMagnification(correctedMagnification);
      pipelinePainter.setOffset(offset);

      // Draw the navigation viewport at the bottom right
      //
      pipelinePainter.setMaximum(maximum);
      pipelinePainter.setShowingNavigationView(true);
      pipelinePainter.setScreenMagnification(magnification);

      try {
        pipelinePainter.drawPipelineImage();

        viewPort = pipelinePainter.getViewPort();
        graphPort = pipelinePainter.getGraphPort();
      } catch (Exception e) {
        new ErrorDialog(hopGui.getActiveShell(), CONST_ERROR, "Error drawing pipeline image", e);
      }
    } finally {
      gc.dispose();
    }
    CanvasFacade.setData(canvas, magnification, offset, pipelineMeta);
  }

  @Override
  public Control getControl() {
    return this;
  }

  /** Refresh the information in the execution panes */
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.Refresh.Tooltip",
      image = "ui/images/refresh.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    refreshStatus();
    refreshMetrics();
    refreshTransformData();
    setFocus();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_LEVEL,
      label = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Zoom",
      toolTip = "i18n::HopGuiPipelineGraph.GuiAction.ZoomInOut.Tooltip",
      type = GuiToolbarElementType.COMBO,
      alignRight = true,
      comboValuesMethod = "getZoomLevels")
  public void zoomLevel() {
    readMagnification();
    redraw();
  }

  /** Refresh the information in the execution panes */
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_TO_EDITOR,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.NavigateToEditor.Tooltip",
      image = "ui/images/data_orch.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void navigateToEditor() {
    try {
      // First try to see if this pipeline is running in Hop GUI...
      //
      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      HopGuiPipelineGraph pipelineGraph = perspective.findPipeline(execution.getId());
      if (pipelineGraph != null) {
        perspective.setActiveFileTypeHandler(pipelineGraph);
        perspective.activate();
        return;
      }

      // Now see if we have a filename to match with
      //
      String filename = execution.getFilename();
      if (filename != null) {
        hopGui.fileDelegate.fileOpen(filename);
        return;
      }

    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error navigating to pipeline in Hop GUI", e);
    }
  }

  public List<String> getZoomLevels() {
    return Arrays.asList(PipelinePainter.magnificationDescriptions);
  }

  /** Allows for magnifying to any percentage entered by the user... */
  private void readMagnification() {
    Combo zoomLabel = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (zoomLabel == null) {
      return;
    }
    String possibleText = zoomLabel.getText().replace("%", "");

    float possibleFloatMagnification;
    try {
      possibleFloatMagnification = Float.parseFloat(possibleText) / 100;
      magnification = possibleFloatMagnification;
      if (zoomLabel.getText().indexOf('%') < 0) {
        zoomLabel.setText(zoomLabel.getText().concat("%"));
      }
    } catch (Exception e) {
      // Ignore
    }

    refresh();
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return pipelineMeta.getName();
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  @Override
  public String getLogChannelId() {
    return execution.getId();
  }

  @Override
  public void mouseDown(MouseEvent event) {
    pipelineMeta.unselectAll();

    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseHover(event);
    }

    Point real = screen2real(event.x, event.y);
    lastClick = new Point(real.x, real.y);
    boolean control = (event.stateMask & SWT.MOD1) != 0;

    if (setupDragView(event.button, control, new Point(event.x, event.y))) {
      return;
    }

    AreaOwner areaOwner = getVisibleAreaOwner((int) real.x, (int) real.y);
    if (areaOwner == null) {
      // Clicked on background: clear selection
      //
      selectedTransform = null;
    } else {
      switch (areaOwner.getAreaType()) {
        case TRANSFORM_ICON:
          // Show the data for this transform
          //
          selectedTransform = (TransformMeta) areaOwner.getOwner();
          refreshTransformData();
          break;
        case TRANSFORM_NAME:
          // Show the data for this transform
          //
          selectedTransform = (TransformMeta) areaOwner.getParent();
          refreshTransformData();
          break;
        default:
          break;
      }
    }

    redraw();
  }

  public void refreshTransformData() {
    if (selectedTransform != null) {
      showTransformData(selectedTransform);
    }
  }

  private void showTransformData(TransformMeta transformMeta) {
    Cursor busyCursor = new Cursor(getShell().getDisplay(), SWT.CURSOR_WAIT);
    try {
      getShell().setCursor(busyCursor);
      transformMeta.setSelected(true);

      // Remember which entry in the list was selected.
      //
      String previousListSelection = null;
      if (dataList.getSelectionCount() == 1) {
        previousListSelection = dataList.getSelection()[0];
      }
      dataList.removeAll();
      dataView.clearAll();

      try {
        // Get the transform data
        //
        selectedTransformData = loadSelectedTransformData();

        if (selectedTransformData == null) {
          // Nothing collected
          return;
        }

        Map<String, ExecutionDataSetMeta> setMetaData = selectedTransformData.getSetMetaData();
        List<String> items = new ArrayList<>();
        for (String key : setMetaData.keySet()) {
          ExecutionDataSetMeta setMeta = setMetaData.get(key);
          if (transformMeta.getName().equals(setMeta.getName())) {
            // We're in the right place.  We can have different types of data though.
            // We list the types in the List on the left in the data tab.
            //
            items.add(setMeta.getDescription());
          }
        }
        Collections.sort(items);
        dataList.setItems(items.toArray(new String[0]));
        tabFolder.setSelection(dataTab);
        if (previousListSelection != null && items.contains(previousListSelection)) {
          dataList.setSelection(items.indexOf(previousListSelection));
        } else {
          dataList.setSelection(0);
        }
        showDataRows();
      } catch (Exception e) {
        // Ignore this error: there simply isn't any data to be found
        new ErrorDialog(getShell(), CONST_ERROR, "Error showing transform data", e);
      }
    } finally {
      getShell().setCursor(null);
      busyCursor.dispose();
    }
  }

  private ExecutionData loadSelectedTransformData() throws HopException {
    ExecutionInfoLocation location = perspective.getLocationMap().get(locationName);
    if (location == null) {
      return null;
    }
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

    // Load the execution data of all transforms at the same time
    //
    ExecutionData data = iLocation.getExecutionData(execution.getId(), null);

    // If this is for a beam pipeline we won't have data for all transforms but for individual
    // copies...
    //
    if (data == null) {
      ExecutionDataBuilder builder = ExecutionDataBuilder.of().withParentId(execution.getId());
      List<String> childIds =
          location
              .getExecutionInfoLocation()
              .findChildIds(ExecutionType.Pipeline, execution.getId());
      if (childIds != null) {
        for (String childId : childIds) {
          try {
            ExecutionData childData =
                location.getExecutionInfoLocation().getExecutionData(execution.getId(), childId);
            if (childData != null) {
              builder.addDataSets(childData.getDataSets()).addSetMeta(childData.getSetMetaData());
            }
          } catch (Exception e) {
            // Data not yet written for this child ID
            LogChannel.GENERAL.logDetailed(
                "Error find transform data for child ID " + childId + " : " + e.getMessage());
          }
        }
      }
      data = builder.build();
    }

    return data;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DRILL_DOWN,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.DrillDown.Tooltip",
      image = "ui/images/down.svg")
  public void drillDown() {
    if (selectedTransform == null) {
      return;
    }

    ExecutionInfoLocation location = perspective.getLocationMap().get(locationName);
    if (location == null) {
      return;
    }
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

    // We need to look up a pipeline or workflow execution where the parent is the ID of the action
    //
    try {
      // Find the ID of the transform.  That will be the parent of the executing workflow or
      // pipeline
      // Open all copies of the transform
      //
      for (ExecutionDataSetMeta setMeta : selectedTransformData.getSetMetaData().values()) {
        if (setMeta.getName().equals(selectedTransform.getName())) {
          String parentId = setMeta.getLogChannelId();
          List<Execution> childExecutions = iLocation.findExecutions(parentId);
          if (childExecutions.isEmpty()) {
            break;
          }
          Execution child = childExecutions.get(0);

          // Don't load logging text as that can be a lot of data.
          // Lazily load that when the logging text comes into focus.
          //
          ExecutionState executionState = iLocation.getExecutionState(execution.getId(), false);
          perspective.createExecutionViewer(locationName, child, executionState);
          return;
        }
      }

      // If we're still here we're maybe dealing with an executor.
      // In this scenario the transform executed a sub-pipeline or workflow.
      // See: PipelineExecutor, WorkflowExecutor, MetaInject, ...
      //
      // What we do is look at the execution state which gives us the child IDs.
      // We'll find Transform executions with the transform name and the parent
      //
      List<Execution> executions =
          iLocation.findExecutions(
              e ->
                  selectedTransform.getName().equals(e.getName())
                      && e.getExecutionType() == ExecutionType.Transform
                      && execution.getId().equals(e.getParentId()));

      // Since we're here this probably means that we have more than one execution for an executor.
      // We can have multiple copies of this executor running.  Ask which copy to pick...
      //
      if (executions.isEmpty()) {
        return;
      }
      Execution transformExecution;
      if (executions.size() == 1) {
        transformExecution = executions.get(0);
      } else {
        transformExecution = selectExecution(executions);
      }
      if (transformExecution == null) {
        return;
      }

      // Now we have the transform execution that we want we can look for the execution(s) with this
      // as parent
      //
      List<Execution> childExecutions = iLocation.findExecutions(transformExecution.getId());
      if (childExecutions.isEmpty()) {
        return;
      }
      Execution childExecution;
      if (childExecutions.size() == 1) {
        childExecution = childExecutions.get(0);
      } else {
        childExecution = selectExecution(childExecutions);
      }
      // Don't load execution logging text to prevent memory issues.
      //
      ExecutionState executionState = iLocation.getExecutionState(execution.getId(), false);
      perspective.createExecutionViewer(locationName, childExecution, executionState);

    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error drilling down into selected action", e);
    }
  }

  private Execution selectExecution(List<Execution> executions) {
    IRowMeta rowMeta =
        new RowMetaBuilder()
            .addString("Type")
            .addString("Name")
            .addString("Copy")
            .addDate("Start date")
            .build();
    List<RowMetaAndData> rows = new ArrayList<>();
    for (Execution execution : executions) {
      rows.add(
          new RowMetaAndData(
              rowMeta,
              execution.getExecutionType().name(),
              execution.getName(),
              execution.getCopyNr(),
              execution.getExecutionStartDate()));
    }

    SelectRowDialog dialog =
        new SelectRowDialog(
            getShell(), hopGui.getVariables(), SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL, rows);
    RowMetaAndData selectedRow = dialog.open();
    if (selectedRow == null) {
      // Operation is canceled
      return null;
    }
    int index = rows.indexOf(selectedRow);
    if (index < 0) {
      return null;
    }
    return executions.get(index);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_GO_UP,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.GoUp.Tooltip",
      image = "ui/images/up.svg")
  public void goUp() {
    try {
      String parentId = execution.getParentId();
      if (parentId == null) {
        return;
      }

      ExecutionInfoLocation location = perspective.getLocationMap().get(locationName);
      if (location == null) {
        return;
      }
      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      // This parent ID is the ID of the transform or action.
      // We need to find the parent of this child
      //
      String grandParentId = iLocation.findParentId(parentId);
      if (grandParentId == null) {
        return;
      }

      Execution grandParent = iLocation.getExecution(grandParentId);

      // Don't load execution logging text to prevent memory issues.
      //
      ExecutionState executionState = iLocation.getExecutionState(grandParent.getId(), false);

      // Open this one
      //
      perspective.createExecutionViewer(locationName, grandParent, executionState);
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error navigating up to parent execution", e);
    }
  }

  @Override
  public void drillDownOnLocation(Point location) {
    if (location == null) {
      return;
    }
    AreaOwner areaOwner = getVisibleAreaOwner(location.x, location.y);
    if (areaOwner == null) {
      return;
    }
    if (areaOwner.getAreaType() == AreaOwner.AreaType.TRANSFORM_ICON) {
      // Show the data for this transform
      //
      selectedTransform = (TransformMeta) areaOwner.getOwner();
      refreshTransformData();

      // Now drill down
      //
      drillDown();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_VIEW_EXECUTOR,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.ViewExecutor.Tooltip",
      image = "ui/images/view.svg",
      separator = true)
  public void viewExecutor() {
    try {
      String pipelineXml = execution.getExecutorXml();
      Node pipelineNode = XmlHandler.loadXmlString(pipelineXml, PipelineMeta.XML_TAG);

      // Also inflate the metadata
      //
      String metadataJson = execution.getMetadataJson();
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(metadataJson);

      // The variables set
      //
      IVariables variables = Variables.getADefaultVariableSpace();
      variables.setVariables(execution.getVariableValues());
      variables.setVariables(execution.getParameterValues());

      PipelineMeta pipelineMeta = new PipelineMeta(pipelineNode, metadataProvider);

      ExplorerPerspective perspective = HopGui.getExplorerPerspective();
      HopGuiPipelineGraph graph = (HopGuiPipelineGraph) perspective.addPipeline(pipelineMeta);

      graph.setVariables(variables);

      perspective.activate();
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error viewing the executor", e);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_VIEW_METADATA,
      toolTip = "i18n::PipelineExecutionViewer.ToolbarElement.ViewMetadata.Tooltip",
      image = "ui/images/metadata.svg")
  public void viewMetadata() {
    super.viewMetadata(execution);
  }

  @Override
  public String getActiveId() {
    if (selectedTransform != null) {
      // Find the log channel ID of the selected transform
      //
      for (ExecutionDataSetMeta setMeta : selectedTransformData.getSetMetaData().values()) {
        if (selectedTransform.getName().equals(setMeta.getName())) {
          // This is the one
          return setMeta.getLogChannelId();
        }
      }
    }
    // If we're still here, return the pipeline log channel ID
    return getLogChannelId();
  }

  /**
   * Gets pipelineMeta
   *
   * @return value of pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * Gets execution
   *
   * @return value of execution
   */
  @Override
  public Execution getExecution() {
    return execution;
  }

  /**
   * Gets selectedTransform
   *
   * @return value of selectedTransform
   */
  public TransformMeta getSelectedTransform() {
    return selectedTransform;
  }

  /**
   * Sets selectedTransform
   *
   * @param selectedTransform value of selectedTransform
   */
  public void setSelectedTransform(TransformMeta selectedTransform) {
    this.selectedTransform = selectedTransform;
  }

  /**
   * Gets locationName
   *
   * @return value of locationName
   */
  @Override
  public String getLocationName() {
    return locationName;
  }

  /**
   * Gets selectedTransformData
   *
   * @return value of selectedTransformData
   */
  public ExecutionData getSelectedTransformData() {
    return selectedTransformData;
  }

  /**
   * Sets selectedTransformData
   *
   * @param selectedTransformData value of selectedTransformData
   */
  public void setSelectedTransformData(ExecutionData selectedTransformData) {
    this.selectedTransformData = selectedTransformData;
  }

  /**
   * Gets dataList
   *
   * @return value of dataList
   */
  public org.eclipse.swt.widgets.List getDataList() {
    return dataList;
  }

  /**
   * Sets dataList
   *
   * @param dataList value of dataList
   */
  public void setDataList(org.eclipse.swt.widgets.List dataList) {
    this.dataList = dataList;
  }

  /**
   * Gets perspective
   *
   * @return value of perspective
   */
  @Override
  public ExecutionPerspective getPerspective() {
    return perspective;
  }
}
