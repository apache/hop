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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.PipelineMetricDisplayUtil;
import org.apache.hop.ui.hopgui.file.shared.ExecutionGuiSession;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

/**
 * Delegate for the pipeline execution metrics tab. Transforms that run in several copies are one
 * collapsed row with the totals; expanding it shows each copy. A single copy stays one row. Toolbar
 * actions (open transform, show/hide filters, copy), sorting, and double-click behaviour stay on
 * this tab.
 */
@GuiPlugin(description = "Pipeline Graph Grid Delegate")
public class HopGuiPipelineGridDelegate {
  private static final Class<?> PKG = HopGui.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowGridDelegate-ToolBar";

  /** Open-transform button; id 09900 so it appears first in the toolbar. */
  public static final String TOOLBAR_ICON_OPEN_TRANSFORM = "ToolbarIcon-09900-OpenTransform";

  public static final String TOOLBAR_ICON_SHOW_HIDE_INACTIVE = "ToolbarIcon-10000-ShowHideInactive";
  public static final String TOOLBAR_ICON_SHOW_HIDE_SELECTED = "ToolbarIcon-10010-ShowHideSelected";
  public static final String TOOLBAR_ICON_COPY = "ToolbarIcon-10020-Copy";

  public static final long UPDATE_TIME_VIEW = 1000L;

  private static final int METRICS_TREE_STYLE =
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL;

  private final HopGui hopGui;
  private final HopGuiPipelineGraph pipelineGraph;

  @Getter private CTabItem pipelineGridTab;

  private Tree pipelineMetricsTree;

  private Control toolbar;
  private GuiToolbarWidgets toolbarWidget;

  private Composite pipelineGridComposite;

  private boolean hideInactiveTransforms;

  private boolean showSelectedTransforms;

  private final ReentrantLock refreshViewLock;

  private Timer refreshMetricsTimer;

  /**
   * Last sort column from a header click. {@code -1} keeps pipeline order. Only parent rows are
   * sorted; copies under a parent stay in copy-number order.
   */
  private int gridSortColumn = -1;

  private boolean gridSortDescending = false;

  /**
   * Cached from last run so "show selected/inactive" filter still works after the refresh timer
   * stops.
   */
  private EngineMetrics lastEngineMetrics;

  /**
   * Search text for filtering the metrics table by transform name. Empty or &lt; 2 characters means
   * no filter (show all). Matches case-insensitive substring, like the settings panel search.
   */
  private String transformNameSearchText = "";

  private Text searchText;

  /**
   * Keys for current table columns (e.g. "#", "TransformName", "input") used to save/restore column
   * widths. Set when the table is created so we can save widths before dispose.
   */
  private List<String> metricsColumnKeys = null;

  /**
   * Column widths for the metrics grid (session-only, not persisted). Key = column key e.g. "#",
   * "TransformName", "input".
   */
  private final Map<String, Integer> metricsColumnWidths = new HashMap<>();

  /**
   * True while we are programmatically changing column widths. Resize listeners must not persist
   * those changes or they overwrite the user's saved widths.
   */
  private boolean restoringColumnWidths = false;

  /**
   * When true, the next {@link #refreshView()} fully packs columns (View menu toggled units or
   * visibility). Steady-state timer ticks only grow auto-sized columns.
   */
  private boolean packColumnsOnNextRefresh = false;

  /**
   * @param hopGui Hop GUI instance
   * @param pipelineGraph the pipeline graph that owns this delegate
   */
  public HopGuiPipelineGridDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    this.refreshViewLock = new ReentrantLock();
    hideInactiveTransforms = false;
  }

  public void showGridView() {

    if (pipelineGridTab == null || pipelineGridTab.isDisposed()) {
      addPipelineGrid();
    } else {
      pipelineGridTab.dispose();

      pipelineGraph.checkEmptyExtraView();
    }
  }

  /** Add a grid with the execution metrics per transform. */
  public void addPipelineGrid() {

    // First, see if we need to add the extra view...
    //
    if (pipelineGraph.extraViewTabFolder == null || pipelineGraph.extraViewTabFolder.isDisposed()) {
      pipelineGraph.addExtraView();
    } else {
      if (pipelineGridTab != null && !pipelineGridTab.isDisposed()) {
        // Reusing existing grid for a new run: reset sort to default and clear metrics cache.
        // Collapse groups so the new run starts with the totals, not the previous expansion.
        gridSortColumn = -1;
        gridSortDescending = false;
        lastEngineMetrics = null;
        packColumnsOnNextRefresh = true;
        collapseMetricsTree();
        startRefreshMetricsTimer();
        return;
      }
    }

    gridSortColumn = -1;
    gridSortDescending = false;
    TreeMemory.getInstance().clearTree(metricsTreeMemoryName());
    lastEngineMetrics = null;
    transformNameSearchText = "";

    pipelineGridTab = new CTabItem(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineGridTab.setFont(GuiResource.getInstance().getFontDefault());
    pipelineGridTab.setImage(GuiResource.getInstance().getImageShowGrid());
    pipelineGridTab.setText(BaseMessages.getString(PKG, "HopGui.PipelineGraph.GridTab.Name"));

    pipelineGridComposite = new Composite(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineGridComposite.setLayout(new FormLayout());

    addToolBar();

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.TransformName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Copynr"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Input"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Read"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Written"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Output"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Updated"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Rejected"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Errors"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.BuffersInput"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.BuffersOutput"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Duration"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Speed"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Status"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
        };

    columns[1].setAlignment(SWT.RIGHT);
    columns[2].setAlignment(SWT.RIGHT);
    columns[3].setAlignment(SWT.RIGHT);
    columns[4].setAlignment(SWT.RIGHT);
    columns[5].setAlignment(SWT.RIGHT);
    columns[6].setAlignment(SWT.RIGHT);
    columns[7].setAlignment(SWT.RIGHT);
    columns[8].setAlignment(SWT.RIGHT);
    columns[9].setAlignment(SWT.LEFT);
    columns[10].setAlignment(SWT.RIGHT);
    columns[11].setAlignment(SWT.RIGHT);
    columns[12].setAlignment(SWT.RIGHT);
    columns[13].setAlignment(SWT.RIGHT);

    pipelineMetricsTree = createMetricsTree(columns);
    metricsColumnKeys = placeholderColumnKeys();
    attachColumnWidthListeners();
    attachMetricsTreeListeners(pipelineMetricsTree);
    previousRefreshColumns = List.of(columns);

    startRefreshMetricsTimer();
    pipelineGridTab.addDisposeListener(disposeEvent -> stopRefreshMetricsTimer());

    pipelineGridTab.setControl(pipelineGridComposite);
  }

  public void startRefreshMetricsTimer() {
    // A new run replaces any timer left by the previous one. Returning early when a timer already
    // existed dropped the new run on the floor if the previous finish then cancelled that timer.
    //
    ExecutionGuiSession session = pipelineGraph.getExecutionGuiSession();
    ExecutionGuiSession.Snapshot snapshot = session.current();
    if (!(snapshot.engine() instanceof IPipelineEngine<?> engine)) {
      return;
    }
    session.scheduleWhileCurrent(
        snapshot,
        "HopGuiPipelineGraph: " + pipelineGraph.getMeta().getName(),
        UPDATE_TIME_VIEW,
        null,
        timer -> {
          ExecutorUtil.cleanup(refreshMetricsTimer);
          refreshMetricsTimer = timer;
        },
        () -> {
          if (hopGui.getDisplay().isDisposed()) {
            return;
          }
          hopGui
              .getDisplay()
              .asyncExec(
                  () -> {
                    if (!session.isCurrent(engine, snapshot.generation())) {
                      return;
                    }
                    refreshView();
                  });
          if ((engine.isFinished() || engine.isStopped()) && !engine.isReadyToStart()) {
            session.stopIfCurrent(engine, this::stopRefreshMetricsTimer);
          }
        });
  }

  public void stopRefreshMetricsTimer() {
    // Refresh one last time to make sure we're showing the correct data.
    hopGui.getDisplay().asyncExec(this::refreshView);
    ExecutorUtil.cleanup(refreshMetricsTimer);
    refreshMetricsTimer = null;
  }

  /**
   * Called by the pipeline graph when selection changes (e.g. user selects transforms). Refreshes
   * the grid view when "show selected" is on.
   */
  public void onPipelineSelectionChanged() {
    if (!showSelectedTransforms) {
      return;
    }
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    hopGui.getDisplay().asyncExec(this::refreshView);
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiPipelineGridDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiPipelineGraph graph) {
      return graph.pipelineGridDelegate;
    }
    return null;
  }

  private void addToolBar() {
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(
            pipelineGridComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolbar = toolBarContainer.getControl();
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolbarWidget = new GuiToolbarWidgets();
    toolbarWidget.registerGuiPluginObject(this);
    toolbarWidget.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);

    addMetricsViewDropdown(toolbar);
    addSearchBarToToolbar(toolbar);

    toolbar.pack();
  }

  /**
   * Add a "View" dropdown to the toolbar with checkable items for metrics panel options (units,
   * column resizing, hide columns). Syncs with PropsUi so options dialog and toolbar stay in sync.
   * Uses a CLabel with SWT.CENTER so the text is centered consistently across platforms (e.g.
   * Linux).
   */
  private void addMetricsViewDropdown(Control toolbarControl) {
    if (!(toolbarControl instanceof ToolBar tb)) {
      return;
    }
    ToolItem viewItem = new ToolItem(tb, SWT.SEPARATOR);
    CLabel viewLabel = new CLabel(tb, SWT.CENTER);
    viewLabel.setText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.View"));
    viewLabel.setToolTipText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.View.Tooltip"));
    PropsUi.setLook(viewLabel, Props.WIDGET_STYLE_TOOLBAR);
    viewLabel.pack();

    viewLabel.addListener(
        SWT.MouseDown,
        e -> {
          Menu menu = createMetricsViewMenu(tb);
          Rectangle bounds = viewLabel.getBounds();
          Point point = tb.toDisplay(bounds.x, bounds.y + bounds.height);
          menu.setLocation(point.x, point.y);
          menu.setVisible(true);
        });

    viewItem.setWidth(viewLabel.getSize().x);
    viewItem.setControl(viewLabel);
  }

  private Menu createMetricsViewMenu(ToolBar parent) {
    Menu menu = new Menu(parent.getShell(), SWT.POP_UP);
    PropsUi props = PropsUi.getInstance();

    MenuItem showUnitsItem = new MenuItem(menu, SWT.CHECK);
    showUnitsItem.setText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.ShowUnits"));
    showUnitsItem.setSelection(props.isMetricsPanelShowUnits());
    showUnitsItem.addListener(
        SWT.Selection,
        e ->
            setMetricsOptionAndRefresh(
                () -> props.setMetricsPanelShowUnits(showUnitsItem.getSelection())));

    MenuItem dynamicResizeItem = new MenuItem(menu, SWT.CHECK);
    dynamicResizeItem.setText(
        BaseMessages.getString(PKG, "PipelineLog.MetricsView.DynamicColumnResize"));
    dynamicResizeItem.setToolTipText(
        BaseMessages.getString(PKG, "PipelineLog.MetricsView.DynamicColumnResize.Tooltip"));
    dynamicResizeItem.setSelection(props.isMetricsPanelDynamicColumnResize());
    dynamicResizeItem.addListener(
        SWT.Selection,
        e ->
            setMetricsOptionAndRefresh(
                () -> props.setMetricsPanelDynamicColumnResize(dynamicResizeItem.getSelection()),
                false));

    MenuItem fitColumnsItem = new MenuItem(menu, SWT.PUSH);
    fitColumnsItem.setText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.FitColumns"));
    fitColumnsItem.setToolTipText(
        BaseMessages.getString(PKG, "PipelineLog.MetricsView.FitColumns.Tooltip"));
    fitColumnsItem.addListener(
        SWT.Selection,
        e -> {
          packColumnsOnNextRefresh = true;
          refreshView();
        });

    new MenuItem(menu, SWT.SEPARATOR);

    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowInput",
        props.isMetricsPanelShowInput(),
        props::setMetricsPanelShowInput);
    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowRead",
        props.isMetricsPanelShowRead(),
        props::setMetricsPanelShowRead);
    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowOutput",
        props.isMetricsPanelShowOutput(),
        props::setMetricsPanelShowOutput);
    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowUpdated",
        props.isMetricsPanelShowUpdated(),
        props::setMetricsPanelShowUpdated);
    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowRejected",
        props.isMetricsPanelShowRejected(),
        props::setMetricsPanelShowRejected);
    addMetricsViewMenuItem(
        menu,
        "PipelineLog.MetricsView.ShowBuffersInput",
        props.isMetricsPanelShowBuffersInput(),
        props::setMetricsPanelShowBuffersInput);
    MenuItem dataVolumeItem =
        addMetricsViewMenuItem(
            menu,
            "PipelineLog.MetricsView.ShowDataVolume",
            props.isMetricsPanelShowDataVolume(),
            props::setMetricsPanelShowDataVolume);
    boolean dataVolumeEnabled =
        Const.toBoolean(
            HopGui.getInstance().getVariables().getVariable(Const.HOP_METRIC_DATA_VOLUME, "N"));
    dataVolumeItem.setEnabled(dataVolumeEnabled);
    if (!dataVolumeEnabled) {
      dataVolumeItem.setSelection(false);
    }
    MenuItem dataVolumeInItem =
        addMetricsViewMenuItem(
            menu,
            "PipelineLog.MetricsView.ShowDataVolumeIn",
            props.isMetricsPanelShowDataVolumeIn(),
            props::setMetricsPanelShowDataVolumeIn);
    dataVolumeInItem.setEnabled(dataVolumeEnabled);
    if (!dataVolumeEnabled) {
      dataVolumeInItem.setSelection(false);
    }
    MenuItem dataVolumeOutItem =
        addMetricsViewMenuItem(
            menu,
            "PipelineLog.MetricsView.ShowDataVolumeOut",
            props.isMetricsPanelShowDataVolumeOut(),
            props::setMetricsPanelShowDataVolumeOut);
    dataVolumeOutItem.setEnabled(dataVolumeEnabled);
    if (!dataVolumeEnabled) {
      dataVolumeOutItem.setSelection(false);
    }

    return menu;
  }

  private MenuItem addMetricsViewMenuItem(
      Menu menu, String messageKey, boolean selected, Consumer<Boolean> setOption) {
    MenuItem item = new MenuItem(menu, SWT.CHECK);
    item.setText(BaseMessages.getString(PKG, messageKey));
    item.setSelection(selected);
    item.addListener(
        SWT.Selection,
        e -> setMetricsOptionAndRefresh(() -> setOption.accept(item.getSelection())));
    return item;
  }

  private void setMetricsOptionAndRefresh(Runnable setOption) {
    setMetricsOptionAndRefresh(setOption, true);
  }

  private void setMetricsOptionAndRefresh(Runnable setOption, boolean packColumns) {
    setOption.run();
    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception ignored) {
      // best-effort persist
    }
    packColumnsOnNextRefresh = packColumns;
    refreshView();
  }

  /**
   * Add the transform-name search bar to the toolbar. Layout differs for ToolBar (desktop) vs
   * Composite (web flow).
   */
  private void addSearchBarToToolbar(Control toolbarControl) {
    final int searchWidth = 260;
    final int textStyle = SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL | SWT.BORDER;

    if (toolbarControl instanceof ToolBar tb) {
      ToolItem searchSeparator = new ToolItem(tb, SWT.SEPARATOR);
      searchText = new Text(tb, textStyle);
      configureSearchTextListener();
      searchSeparator.setControl(searchText);
      searchSeparator.setWidth(searchWidth);
    } else {
      searchText = new Text((Composite) toolbarControl, textStyle);
      configureSearchTextListener();
      searchText.pack();
      searchText.setLayoutData(new RowData(searchWidth, SWT.DEFAULT));
    }

    searchText.setMessage(
        BaseMessages.getString(PKG, "PipelineLog.Search.TransformName.Placeholder"));
    PropsUi.setLook(searchText, Props.WIDGET_STYLE_TOOLBAR);
  }

  private void configureSearchTextListener() {
    searchText.addListener(
        SWT.Modify,
        e -> {
          if (searchText == null || searchText.isDisposed()) {
            return;
          }
          String raw = searchText.getText();
          transformNameSearchText = raw != null ? raw.trim() : "";
          refreshView();
        });
  }

  /** Opens the transform configuration for the selected metrics row (same as double-click). */
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_OPEN_TRANSFORM,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineGraph.EditTransform.Tooltip",
      image = "ui/images/edit.svg")
  public void openSelectedTransform() {
    openTransformForSelectedRow();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_HIDE_INACTIVE,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ShowOnlyActiveTransforms",
      image = "ui/images/show.svg")
  public void showHideInactive() {
    hideInactiveTransforms = !hideInactiveTransforms;

    String imagePath = hideInactiveTransforms ? "ui/images/hide.svg" : "ui/images/show.svg";
    toolbarWidget.setToolbarItemImage(TOOLBAR_ICON_SHOW_HIDE_INACTIVE, imagePath);
    refreshView();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_HIDE_SELECTED,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ShowOnlySelectedTransforms",
      image = "ui/images/show-all.svg")
  public void showHideSelected() {
    showSelectedTransforms = !showSelectedTransforms;

    String imagePath =
        showSelectedTransforms ? "ui/images/show-selected.svg" : "ui/images/show-all.svg";
    toolbarWidget.setToolbarItemImage(TOOLBAR_ICON_SHOW_HIDE_SELECTED, imagePath);
    refreshView();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_COPY,
      toolTip = "i18n:org.apache.hop.ui.core.widget:TableView.ToolBarWidget.CopySelected.ToolTip",
      image = "ui/images/copy.svg",
      separator = true)
  public void copyMetricsToClipboard() {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    String clip = PipelineMetricsTree.copyText(pipelineMetricsTree);
    if (Utils.isEmpty(clip)) {
      return;
    }
    GuiResource.getInstance().toClipboard(clip);
  }

  private void refreshView() {
    refreshViewLock.lock();
    try {
      if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
        return;
      }

      EngineMetrics engineMetrics = null;
      if (pipelineGraph.pipeline != null) {
        engineMetrics = pipelineGraph.pipeline.getEngineMetrics();
        lastEngineMetrics = engineMetrics;
      } else if (lastEngineMetrics != null) {
        engineMetrics = lastEngineMetrics;
      }
      if (engineMetrics == null) {
        return;
      }
      Set<String> selectedTransformNames = getSelectedTransformNamesFromCanvas();
      List<IEngineComponent> shownComponents =
          getShownComponents(engineMetrics, selectedTransformNames);
      List<IEngineMetric> usedMetrics = getUsedMetrics(engineMetrics);
      List<ColumnInfo> columns = buildColumnList(usedMetrics);
      applySavedColumnWidthsToColumnInfos(columns, usedMetrics);

      boolean recreated = recreateTreeIfColumnsChanged(columns, usedMetrics);
      boolean packColumns = packColumnsOnNextRefresh;
      packColumnsOnNextRefresh = false;

      List<PipelineMetricsNode> nodes =
          PipelineMetricsGrouper.group(shownComponents, engineMetrics, usedMetrics);
      PipelineMetricsGrouper.sortParents(nodes, gridSortColumn, gridSortDescending, usedMetrics);
      PipelineMetricsTree.sync(
          pipelineMetricsTree,
          nodes,
          usedMetrics,
          PropsUi.getInstance().isMetricsPanelShowUnits(),
          metricsTreeMemoryName());

      setSortIndicator();

      // Ignore resize events from packing / restore so we don't treat them as user drags.
      restoringColumnWidths = true;
      try {
        if (packColumns && !recreated) {
          packTreeColumns(false);
          restoreColumnWidths(usedMetrics);
        } else if (recreated || PropsUi.getInstance().isMetricsPanelDynamicColumnResize()) {
          // A fresh tree has no measured widths yet. Later ticks only grow auto-sized columns.
          packTreeColumns(true);
        }
      } finally {
        restoringColumnWidths = false;
      }
      previousRefreshColumns = columns;
      updateEditButtonState();
    } finally {
      refreshViewLock.unlock();
    }
  }

  /**
   * Current canvas selection (transform names). Used so "only show selected" reflects the latest
   * selection even when the refresh timer has stopped and we're using cached engine metrics.
   */
  private Set<String> getSelectedTransformNamesFromCanvas() {
    if (pipelineGraph.getPipelineMeta() == null) {
      return null;
    }
    List<TransformMeta> selected = pipelineGraph.getPipelineMeta().getSelectedTransforms();
    if (selected == null || selected.isEmpty()) {
      return null;
    }
    Set<String> names = new HashSet<>();
    for (TransformMeta t : selected) {
      if (t != null && t.getName() != null) {
        names.add(t.getName());
      }
    }
    return names.isEmpty() ? null : names;
  }

  private List<IEngineComponent> getShownComponents(
      EngineMetrics engineMetrics, Set<String> selectedTransformNames) {
    List<IEngineComponent> shownComponents = new ArrayList<>();
    for (IEngineComponent component : engineMetrics.getComponents()) {
      boolean select = true;
      select = select && (!hideInactiveTransforms || component.isRunning());
      if (showSelectedTransforms
          && selectedTransformNames != null
          && !selectedTransformNames.isEmpty()) {
        select = select && selectedTransformNames.contains(component.getName());
      }
      // When "show selected" is on but no transforms are selected on canvas: show all
      if (select) {
        shownComponents.add(component);
      }
    }
    // Smart search filter by transform name (like settings panel: min 2 chars, case-insensitive)
    if (transformNameSearchText != null && transformNameSearchText.length() >= 2) {
      String lowerSearch = transformNameSearchText.toLowerCase();
      List<IEngineComponent> filtered = new ArrayList<>();
      for (IEngineComponent c : shownComponents) {
        String name = c.getName();
        if (name != null && name.toLowerCase().contains(lowerSearch)) {
          filtered.add(c);
        }
      }
      shownComponents = filtered;
    }
    return shownComponents;
  }

  private List<IEngineMetric> getUsedMetrics(EngineMetrics engineMetrics) {
    List<IEngineMetric> usedMetrics = new ArrayList<>(engineMetrics.getMetricsList());
    usedMetrics.removeIf(this::isMetricHidden);
    Collections.sort(
        usedMetrics, (o1, o2) -> o1.getDisplayPriority().compareTo(o2.getDisplayPriority()));
    return usedMetrics;
  }

  private boolean isMetricHidden(IEngineMetric metric) {
    String code = metric.getCode();
    if (code == null) {
      return false;
    }
    PropsUi props = PropsUi.getInstance();
    boolean dataVolumeVarEnabled =
        Const.toBoolean(
            HopGui.getInstance().getVariables().getVariable(Const.HOP_METRIC_DATA_VOLUME, "N"));
    return switch (code) {
      case Pipeline.METRIC_NAME_INPUT -> !props.isMetricsPanelShowInput();
      case Pipeline.METRIC_NAME_READ -> !props.isMetricsPanelShowRead();
      case Pipeline.METRIC_NAME_OUTPUT -> !props.isMetricsPanelShowOutput();
      case Pipeline.METRIC_NAME_UPDATED -> !props.isMetricsPanelShowUpdated();
      case Pipeline.METRIC_NAME_REJECTED -> !props.isMetricsPanelShowRejected();
      case Pipeline.METRIC_NAME_BUFFER_IN -> !props.isMetricsPanelShowBuffersInput();
      case Pipeline.METRIC_NAME_DATA_VOLUME ->
          !dataVolumeVarEnabled || !props.isMetricsPanelShowDataVolume();
      case Pipeline.METRIC_NAME_DATA_VOLUME_IN ->
          !dataVolumeVarEnabled || !props.isMetricsPanelShowDataVolumeIn();
      case Pipeline.METRIC_NAME_DATA_VOLUME_OUT ->
          !dataVolumeVarEnabled || !props.isMetricsPanelShowDataVolumeOut();
      default -> false;
    };
  }

  private List<ColumnInfo> buildColumnList(List<IEngineMetric> usedMetrics) {
    List<ColumnInfo> columns = new ArrayList<>();

    columns.add(
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.TransformName"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true));

    ColumnInfo copyColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Copynr"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            true,
            true);
    copyColumn.setAlignment(SWT.RIGHT);
    columns.add(copyColumn);

    for (IEngineMetric metric : usedMetrics) {
      ColumnInfo column =
          new ColumnInfo(
              PipelineMetricDisplayUtil.getDisplayHeaderWithUnit(metric),
              ColumnInfo.COLUMN_TYPE_TEXT,
              metric.isNumeric(),
              true);
      column.setToolTip(metric.getTooltip());
      column.setAlignment(SWT.RIGHT);
      columns.add(column);
    }

    ColumnInfo durationColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Duration"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    durationColumn.setAlignment(SWT.RIGHT);
    columns.add(durationColumn);

    ColumnInfo speedColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Speed"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    speedColumn.setAlignment(SWT.RIGHT);
    columns.add(speedColumn);

    columns.add(
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Status"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true));

    return columns;
  }

  /**
   * Applies saved column widths to ColumnInfo so a rebuilt tree starts from the user's widths.
   * Column 0 is the transform name.
   */
  private void applySavedColumnWidthsToColumnInfos(
      List<ColumnInfo> columns, List<IEngineMetric> usedMetrics) {
    List<String> keys = getColumnKeys(usedMetrics);
    for (int i = 0; i < columns.size() && i < keys.size(); i++) {
      Integer width = metricsColumnWidths.get(keys.get(i));
      if (width != null && width > 0) {
        columns.get(i).setWidth(width);
      }
    }
  }

  /**
   * Rebuilds the metrics tree when the column set changes. Returns true when a new tree was
   * created.
   */
  private boolean recreateTreeIfColumnsChanged(
      List<ColumnInfo> columns, List<IEngineMetric> usedMetrics) {
    if (!haveColumnsChanged(columns)) {
      return false;
    }
    saveColumnWidths();
    if (pipelineMetricsTree != null && !pipelineMetricsTree.isDisposed()) {
      pipelineMetricsTree.dispose();
    }
    pipelineMetricsTree = createMetricsTree(columns.toArray(new ColumnInfo[0]));
    metricsColumnKeys = getColumnKeys(usedMetrics);
    attachColumnWidthListeners();
    attachMetricsTreeListeners(pipelineMetricsTree);
    pipelineGridComposite.layout(true, true);
    restoreColumnWidths(usedMetrics);
    return true;
  }

  private Tree createMetricsTree(ColumnInfo[] columns) {
    Tree tree = new Tree(pipelineGridComposite, METRICS_TREE_STYLE);
    tree.setHeaderVisible(true);
    PropsUi.setLook(tree);
    TreeMemory.addTreeListener(tree, metricsTreeMemoryName());
    PipelineMetricsTree.installCopyCountTooltip(tree);
    for (int i = 0; i < columns.length; i++) {
      ColumnInfo column = columns[i];
      TreeColumn treeColumn = new TreeColumn(tree, column.getAlignment());
      treeColumn.setText(column.getName() == null ? "" : column.getName());
      if (!Utils.isEmpty(column.getToolTip())) {
        treeColumn.setToolTipText(column.getToolTip());
      }
      int width = column.getWidth();
      if (width <= 0) {
        width = i == 0 ? 220 : 90;
      }
      treeColumn.setWidth(width);
    }
    if (tree.getColumnCount() > PipelineMetricsGrouper.COLUMN_COPY) {
      TreeColumn copyColumn = tree.getColumn(PipelineMetricsGrouper.COLUMN_COPY);
      if (Utils.isEmpty(copyColumn.getToolTipText())) {
        copyColumn.setToolTipText(
            BaseMessages.getString(PKG, "PipelineLog.Tree.CopyCount.Tooltip"));
      }
    }
    FormData fdView = new FormData();
    fdView.left = new FormAttachment(0, 0);
    fdView.right = new FormAttachment(100, 0);
    fdView.top = new FormAttachment(toolbar, 0);
    fdView.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(fdView);
    return tree;
  }

  /**
   * Column keys used by the placeholder tree created in {@link #addPipelineGrid()} before the first
   * engine-metrics refresh rebuilds the real column set.
   */
  private static List<String> placeholderColumnKeys() {
    List<String> keys = new ArrayList<>();
    keys.add("TransformName");
    keys.add("Copy");
    keys.add(Pipeline.METRIC_NAME_INPUT);
    keys.add(Pipeline.METRIC_NAME_READ);
    keys.add(Pipeline.METRIC_NAME_WRITTEN);
    keys.add(Pipeline.METRIC_NAME_OUTPUT);
    keys.add(Pipeline.METRIC_NAME_UPDATED);
    keys.add(Pipeline.METRIC_NAME_REJECTED);
    keys.add(Pipeline.METRIC_NAME_ERROR);
    keys.add(Pipeline.METRIC_NAME_BUFFER_IN);
    keys.add(Pipeline.METRIC_NAME_BUFFER_OUT);
    keys.add("duration");
    keys.add("speed");
    keys.add("status");
    return keys;
  }

  /**
   * Builds the list of column keys in tree order (TransformName, Copy, metric codes, duration,
   * speed, status).
   */
  private List<String> getColumnKeys(List<IEngineMetric> usedMetrics) {
    List<String> keys = new ArrayList<>();
    keys.add("TransformName");
    keys.add("Copy");
    for (IEngineMetric m : usedMetrics) {
      keys.add(m.getCode());
    }
    keys.add("duration");
    keys.add("speed");
    keys.add("status");
    return keys;
  }

  /**
   * Refreshes native widths for columns the user has already resized. Auto-sized columns are not
   * stored so grow-only packing can still widen them as counts grow.
   */
  private void saveColumnWidths() {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    if (metricsColumnKeys == null || metricsColumnKeys.isEmpty() || metricsColumnWidths.isEmpty()) {
      return;
    }
    int n = Math.min(pipelineMetricsTree.getColumnCount(), metricsColumnKeys.size());
    for (int i = 0; i < n; i++) {
      String key = metricsColumnKeys.get(i);
      if (!metricsColumnWidths.containsKey(key)) {
        continue;
      }
      int width = pipelineMetricsTree.getColumn(i).getWidth();
      if (width > 0) {
        metricsColumnWidths.put(key, width);
      }
    }
  }

  /** Restores user-resized column widths from the session map onto the tree. */
  private void restoreColumnWidths(List<IEngineMetric> usedMetrics) {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    if (metricsColumnWidths.isEmpty()) {
      return;
    }
    boolean nested = restoringColumnWidths;
    restoringColumnWidths = true;
    pipelineMetricsTree.setRedraw(false);
    try {
      List<String> keys = getColumnKeys(usedMetrics);
      int n = Math.min(pipelineMetricsTree.getColumnCount(), keys.size());
      for (int i = 0; i < n; i++) {
        Integer width = metricsColumnWidths.get(keys.get(i));
        if (width != null && width > 0) {
          TreeColumn column = pipelineMetricsTree.getColumn(i);
          if (column.getWidth() != width) {
            column.setWidth(width);
          }
        }
      }
    } finally {
      if (!pipelineMetricsTree.isDisposed()) {
        pipelineMetricsTree.setRedraw(true);
      }
      if (!nested) {
        restoringColumnWidths = false;
      }
    }
  }

  /** Attach resize listeners so user column resizes are stored for the session. */
  private void attachColumnWidthListeners() {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    if (metricsColumnKeys == null || metricsColumnKeys.isEmpty()) {
      return;
    }
    for (int i = 0; i < pipelineMetricsTree.getColumnCount() && i < metricsColumnKeys.size(); i++) {
      final String key = metricsColumnKeys.get(i);
      TreeColumn column = pipelineMetricsTree.getColumn(i);
      column.addListener(
          SWT.Resize,
          event -> {
            if (restoringColumnWidths || column.isDisposed()) {
              return;
            }
            int width = column.getWidth();
            if (width > 0) {
              metricsColumnWidths.put(key, width);
            }
          });
    }
  }

  /**
   * Sizes columns to the header and the visible cells. {@code growOnly} skips columns the user has
   * resized and never shrinks a column.
   */
  private void packTreeColumns(boolean growOnly) {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    int extra = columnWidthMargin();
    pipelineMetricsTree.setRedraw(false);
    try {
      for (int c = 0; c < pipelineMetricsTree.getColumnCount(); c++) {
        String key =
            metricsColumnKeys != null && c < metricsColumnKeys.size()
                ? metricsColumnKeys.get(c)
                : null;
        if (growOnly && key != null && metricsColumnWidths.containsKey(key)) {
          continue;
        }
        TreeColumn column = pipelineMetricsTree.getColumn(c);
        int max = TextSizeUtilFacade.textExtent(Const.NVL(column.getText(), "")).x + extra;
        if (c == gridSortColumn) {
          max += ConstUi.SMALL_ICON_SIZE;
        }
        if (c == 0) {
          max += (int) (PropsUi.getNativeZoomFactor() * 24);
        }
        max = Math.max(max, widestVisibleCell(pipelineMetricsTree.getItems(), c, extra));
        if (!growOnly || max > column.getWidth()) {
          column.setWidth(Math.max(max, 20));
        }
      }
    } finally {
      if (!pipelineMetricsTree.isDisposed()) {
        pipelineMetricsTree.setRedraw(true);
      }
    }
  }

  private static int columnWidthMargin() {
    int extra;
    if (Const.isWindows()) {
      extra = (int) (PropsUi.getNativeZoomFactor() * 8);
    } else {
      extra = (int) (PropsUi.getNativeZoomFactor() * 5);
    }
    extra +=
        Const.toInt(HopConfig.readStringVariable(Const.HOP_TABLE_VIEW_EXTRA_COLUMN_MARGIN, ""), 0);
    return extra;
  }

  private static int widestVisibleCell(TreeItem[] items, int column, int extra) {
    int max = 0;
    if (items == null) {
      return max;
    }
    for (TreeItem item : items) {
      if (item == null || item.isDisposed()) {
        continue;
      }
      String text = item.getText(column);
      if (!Utils.isEmpty(text)) {
        max = Math.max(max, TextSizeUtilFacade.textExtent(text).x + extra);
      }
      if (item.getExpanded() && item.getItemCount() > 0) {
        max = Math.max(max, widestVisibleCell(item.getItems(), column, extra));
      }
    }
    return max;
  }

  /**
   * Attaches sort, double-click, and selection listeners. Call after creating or recreating the
   * tree.
   */
  private void attachMetricsTreeListeners(Tree tree) {
    for (int i = 0; i < tree.getColumnCount(); i++) {
      TreeColumn column = tree.getColumn(i);
      column.addListener(SWT.Selection, event -> onSortColumnSelected(column));
    }
    tree.addListener(SWT.DefaultSelection, event -> openTransformForSelectedRow());
    tree.addListener(SWT.Selection, event -> updateEditButtonState());
    tree.addListener(
        SWT.MouseDown,
        event -> {
          if (tree.isDisposed()) {
            return;
          }
          hopGui.getDisplay().asyncExec(this::updateEditButtonState);
        });
    updateEditButtonState();
  }

  private void onSortColumnSelected(TreeColumn column) {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed() || column.isDisposed()) {
      return;
    }
    int index = pipelineMetricsTree.indexOf(column);
    if (index < 0) {
      return;
    }
    if (gridSortColumn == index) {
      gridSortDescending = !gridSortDescending;
    } else {
      gridSortColumn = index;
      gridSortDescending = false;
    }
    refreshView();
  }

  /**
   * Enables or disables the open-transform toolbar button based on whether a tree row is selected.
   */
  private void updateEditButtonState() {
    if (toolbarWidget == null || toolbar == null || toolbar.isDisposed()) {
      return;
    }
    boolean linesSelected =
        pipelineMetricsTree != null
            && !pipelineMetricsTree.isDisposed()
            && pipelineMetricsTree.getSelectionCount() > 0;
    toolbarWidget.enableToolbarItem(TOOLBAR_ICON_OPEN_TRANSFORM, linesSelected);
  }

  /**
   * Opens the transform configuration for the selected row. The row data holds the transform name,
   * including on a {@code copy N} child.
   */
  private void openTransformForSelectedRow() {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    TreeItem[] selection = pipelineMetricsTree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    if (!(selection[0].getData() instanceof PipelineMetricsTree.RowKey key)) {
      return;
    }
    if (Utils.isEmpty(key.transformName)) {
      return;
    }
    PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();
    if (pipelineMeta == null) {
      return;
    }
    TransformMeta transformMeta = pipelineMeta.findTransform(key.transformName);
    if (transformMeta == null) {
      return;
    }
    pipelineGraph.editTransform(pipelineMeta, transformMeta);
  }

  /** Update the header to show which column is sorted and in which direction. */
  private void setSortIndicator() {
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    if (gridSortColumn < 0 || gridSortColumn >= pipelineMetricsTree.getColumnCount()) {
      pipelineMetricsTree.setSortColumn(null);
      pipelineMetricsTree.setSortDirection(SWT.NONE);
      return;
    }
    TreeColumn wanted = pipelineMetricsTree.getColumn(gridSortColumn);
    int direction = gridSortDescending ? SWT.DOWN : SWT.UP;
    if (pipelineMetricsTree.getSortColumn() != wanted) {
      pipelineMetricsTree.setSortColumn(wanted);
    }
    if (pipelineMetricsTree.getSortDirection() != direction) {
      pipelineMetricsTree.setSortDirection(direction);
    }
  }

  private String metricsTreeMemoryName() {
    return "HopGuiPipelineGrid:" + System.identityHashCode(pipelineGraph);
  }

  /**
   * Forgets remembered expansion and collapses every group so the next run starts on the totals.
   */
  private void collapseMetricsTree() {
    TreeMemory.getInstance().clearTree(metricsTreeMemoryName());
    if (pipelineMetricsTree == null || pipelineMetricsTree.isDisposed()) {
      return;
    }
    for (TreeItem item : pipelineMetricsTree.getItems()) {
      if (!item.isDisposed() && item.getExpanded()) {
        item.setExpanded(false);
      }
    }
  }

  private List<ColumnInfo> previousRefreshColumns = null;

  private boolean haveColumnsChanged(List<ColumnInfo> columns) {
    if (previousRefreshColumns == null) {
      return true;
    }
    if (previousRefreshColumns.size() != columns.size()) {
      return true;
    }
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo newColumn = columns.get(i);
      ColumnInfo prvColumn = previousRefreshColumns.get(i);
      if (!newColumn.getName().equals(prvColumn.getName())) {
        return true;
      }
      if (newColumn.getType() != prvColumn.getType()) {
        return true;
      }
    }
    return false;
  }
}
