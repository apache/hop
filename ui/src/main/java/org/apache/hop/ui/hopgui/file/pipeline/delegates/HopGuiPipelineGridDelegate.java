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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.PipelineMetricDisplayUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * Delegate for the pipeline execution metrics grid tab. Manages the table of transform metrics
 * (rows read/written, duration, etc.), toolbar actions (open transform, show/hide filters, copy),
 * sorting, and double-click / selection behaviour to open the transform configuration.
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

  private final HopGui hopGui;
  private final HopGuiPipelineGraph pipelineGraph;

  @Getter private CTabItem pipelineGridTab;

  private TableView pipelineGridView;

  private Control toolbar;
  private GuiToolbarWidgets toolbarWidget;

  private Composite pipelineGridComposite;

  private boolean hideInactiveTransforms;

  private boolean showSelectedTransforms;

  private final ReentrantLock refreshViewLock;

  private Timer refreshMetricsTimer;

  /** Last sort column/direction from user (column header click); we re-apply on each refresh. */
  private int gridSortColumn = 0;

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
   * True while we are programmatically restoring column widths. Resize listeners must not persist
   * those changes or they overwrite the user's saved widths.
   */
  private boolean restoringColumnWidths = false;

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

  /** Add a grid with the execution metrics per transform in a table view */
  public void addPipelineGrid() {

    // First, see if we need to add the extra view...
    //
    if (pipelineGraph.extraViewTabFolder == null || pipelineGraph.extraViewTabFolder.isDisposed()) {
      pipelineGraph.addExtraView();
    } else {
      if (pipelineGridTab != null && !pipelineGridTab.isDisposed()) {
        // Reusing existing grid for a new run: reset sort to default and clear metrics cache
        gridSortColumn = 0;
        gridSortDescending = false;
        lastEngineMetrics = null;
        startRefreshMetricsTimer();
        return;
      }
    }

    gridSortColumn = 0;
    gridSortDescending = false;
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

    pipelineGridView =
        new TableView(
            pipelineGraph.getVariables(),
            pipelineGridComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            1,
            true, // readonly
            null,
            hopGui.getProps(),
            true,
            null,
            false,
            false); // no TableView toolbar; copy/filter are on our toolbar
    FormData fdView = new FormData();
    fdView.left = new FormAttachment(0, 0);
    fdView.right = new FormAttachment(100, 0);
    fdView.top = new FormAttachment(toolbar, 0);
    fdView.bottom = new FormAttachment(100, 0);
    pipelineGridView.setLayoutData(fdView);
    pipelineGridView.setSortable(true);
    attachMetricsTableListeners(pipelineGridView);

    ColumnInfo numberColumn = pipelineGridView.getNumberColumn();
    IValueMeta numberColumnValueMeta =
        new ValueMetaString("#", HopGuiPipelineGridDelegate::subTransformCompare);
    numberColumn.setValueMeta(numberColumnValueMeta);

    startRefreshMetricsTimer();
    pipelineGridTab.addDisposeListener(disposeEvent -> stopRefreshMetricsTimer());

    pipelineGridTab.setControl(pipelineGridComposite);
  }

  public void startRefreshMetricsTimer() {
    if (refreshMetricsTimer != null) {
      return;
    }

    // Timer updates the view every UPDATE_TIME_VIEW interval
    refreshMetricsTimer = new Timer("HopGuiPipelineGraph: " + pipelineGraph.getMeta().getName());

    TimerTask refreshMetricsTimerTask =
        new TimerTask() {
          @Override
          public void run() {
            if (!hopGui.getDisplay().isDisposed()) {
              hopGui.getDisplay().asyncExec(HopGuiPipelineGridDelegate.this::refreshView);
              if (pipelineGraph.getPipeline() != null
                  && (pipelineGraph.getPipeline().isFinished()
                      || pipelineGraph.getPipeline().isStopped())
                  && !pipelineGraph.getPipeline().isReadyToStart()) {
                ExecutorUtil.cleanup(refreshMetricsTimer, UPDATE_TIME_VIEW + 10);
                refreshMetricsTimer = null;
              }
            }
          }
        };

    refreshMetricsTimer.schedule(refreshMetricsTimerTask, 0L, UPDATE_TIME_VIEW);
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
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
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
   * Add a "View" dropdown to the toolbar with checkable items for metrics panel options (hide
   * units, hide columns). Syncs with PropsUi so options dialog and toolbar stay in sync.
   */
  private void addMetricsViewDropdown(Control toolbarControl) {
    if (!(toolbarControl instanceof ToolBar tb)) {
      return;
    }
    ToolItem viewItem = new ToolItem(tb, SWT.DROP_DOWN);
    viewItem.setText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.View"));
    viewItem.setToolTipText(BaseMessages.getString(PKG, "PipelineLog.MetricsView.View.Tooltip"));

    viewItem.addListener(
        SWT.Selection,
        e -> {
          Menu menu = createMetricsViewMenu(tb);
          Point point;
          if (e.detail == SWT.ARROW) {
            point = tb.toDisplay(e.x, e.y + viewItem.getBounds().height);
          } else {
            // Clicked the "View" label: show menu below the button
            Rectangle bounds = viewItem.getBounds();
            point = tb.toDisplay(bounds.x, bounds.y + bounds.height);
          }
          menu.setLocation(point.x, point.y);
          menu.setVisible(true);
        });
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

    return menu;
  }

  private void addMetricsViewMenuItem(
      Menu menu, String messageKey, boolean selected, Consumer<Boolean> setOption) {
    MenuItem item = new MenuItem(menu, SWT.CHECK);
    item.setText(BaseMessages.getString(PKG, messageKey));
    item.setSelection(selected);
    item.addListener(
        SWT.Selection,
        e -> setMetricsOptionAndRefresh(() -> setOption.accept(item.getSelection())));
  }

  private void setMetricsOptionAndRefresh(Runnable setOption) {
    setOption.run();
    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception ignored) {
      // best-effort persist
    }
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
      toolTip = "i18n::HopGuiPipelineGridDelegate.Toolbar.OpenTransform.Tooltip",
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
      toolTip = "i18n::TableView.ToolBarWidget.CopySelected.ToolTip",
      image = "ui/images/copy.svg",
      separator = true)
  public void copyMetricsToClipboard() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    Table table = pipelineGridView.table;
    boolean hadSelection = table.getSelectionCount() > 0;
    if (!hadSelection) {
      table.selectAll();
    }
    pipelineGridView.clipSelected();
    if (!hadSelection) {
      table.deselectAll();
    }
  }

  private void refreshView() {
    refreshViewLock.lock();
    try {
      if (pipelineGridView == null || pipelineGridView.isDisposed()) {
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
      List<List<String>> componentStringsList =
          buildComponentStringsList(engineMetrics, shownComponents, usedMetrics);

      recreateTableIfColumnsChanged(columns, shownComponents.size(), usedMetrics);

      sortComponentStringsByColumn(componentStringsList, gridSortColumn, gridSortDescending);

      int errorCol = indexOfMetric(usedMetrics, Pipeline.METRIC_ERROR);
      fillTableRows(componentStringsList, errorCol);

      setSortIndicator();

      // Ignore resize events from optWidth and restoreColumnWidths so we don't overwrite saved
      // widths
      restoringColumnWidths = true;
      pipelineGridView.optWidth(true);
      restoreColumnWidths(usedMetrics);
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
    return switch (code) {
      case Pipeline.METRIC_NAME_INPUT -> !props.isMetricsPanelShowInput();
      case Pipeline.METRIC_NAME_READ -> !props.isMetricsPanelShowRead();
      case Pipeline.METRIC_NAME_OUTPUT -> !props.isMetricsPanelShowOutput();
      case Pipeline.METRIC_NAME_UPDATED -> !props.isMetricsPanelShowUpdated();
      case Pipeline.METRIC_NAME_REJECTED -> !props.isMetricsPanelShowRejected();
      case Pipeline.METRIC_NAME_BUFFER_IN -> !props.isMetricsPanelShowBuffersInput();
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
      IValueMeta stringMeta = new ValueMetaString(metric.getCode());
      ValueMetaInteger valueMeta = new ValueMetaInteger(metric.getCode(), 15, 0);
      valueMeta.setConversionMask(METRICS_FORMAT);
      stringMeta.setConversionMetadata(valueMeta);
      column.setValueMeta(stringMeta);
      column.setAlignment(SWT.RIGHT);
      columns.add(column);
    }

    IValueMeta stringMeta = new ValueMetaString("string");

    ColumnInfo durationColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Duration"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    durationColumn.setValueMeta(stringMeta);
    durationColumn.setAlignment(SWT.RIGHT);
    columns.add(durationColumn);

    ValueMetaInteger speedMeta = new ValueMetaInteger("speed", 15, 0);
    speedMeta.setConversionMask(" ###,###,###,##0");
    stringMeta.setConversionMetadata(speedMeta);
    ColumnInfo speedColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "PipelineLog.Column.Speed"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            true);
    speedColumn.setValueMeta(stringMeta);
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
   * Applies saved column widths to ColumnInfo so that TableView.optWidth() will use them instead of
   * auto-sizing. Table column 0 is the "#" column (no ColumnInfo); indices 1..n match our columns.
   */
  private void applySavedColumnWidthsToColumnInfos(
      List<ColumnInfo> columns, List<IEngineMetric> usedMetrics) {
    List<String> keys = getColumnKeys(usedMetrics);
    for (int i = 0; i < columns.size() && (i + 1) < keys.size(); i++) {
      String key = keys.get(i + 1);
      Integer w = metricsColumnWidths.get(key);
      if (w != null && w > 0) {
        columns.get(i).setWidth(w);
      }
    }
  }

  private List<List<String>> buildComponentStringsList(
      EngineMetrics engineMetrics,
      List<IEngineComponent> shownComponents,
      List<IEngineMetric> usedMetrics) {
    List<List<String>> componentStringsList = new ArrayList<>();
    int rowNum = 1;
    for (IEngineComponent component : shownComponents) {
      List<String> componentStrings = new ArrayList<>();
      componentStrings.add(Integer.toString(rowNum++));
      componentStrings.add(Const.NVL(component.getName(), ""));
      componentStrings.add(Integer.toString(component.getCopyNr()));

      boolean showUnits = PropsUi.getInstance().isMetricsPanelShowUnits();
      for (IEngineMetric metric : usedMetrics) {
        Long value = engineMetrics.getComponentMetric(component, metric);
        String unit = showUnits ? PipelineMetricDisplayUtil.getUnitForMetricCell(metric) : null;
        String cell =
            value == null
                ? ""
                : formatMetric(value) + (unit != null && !unit.isEmpty() ? " " + unit : "");
        componentStrings.add(cell);
      }
      String durationStr = calculateDuration(component);
      componentStrings.add(
          durationStr.isEmpty() ? "" : showUnits ? durationStr + " (h:m:s)" : durationStr);
      String speedStr = engineMetrics.getComponentSpeedMap().get(component);
      componentStrings.add(
          speedStr == null || speedStr.isEmpty()
              ? ""
              : showUnits && !speedStr.trim().equals("-")
                  ? speedStr.trim() + " rows/s"
                  : speedStr.trim());
      componentStrings.add(Const.NVL(engineMetrics.getComponentStatusMap().get(component), ""));

      componentStringsList.add(componentStrings);
    }
    return componentStringsList;
  }

  private void recreateTableIfColumnsChanged(
      List<ColumnInfo> columns, int rowCount, List<IEngineMetric> usedMetrics) {
    if (!haveColumnsChanged(columns)) {
      return;
    }
    saveColumnWidths();
    pipelineGridView.dispose();
    pipelineGridView =
        new TableView(
            pipelineGraph.getVariables(),
            pipelineGridComposite,
            SWT.NONE,
            columns.toArray(new ColumnInfo[0]),
            rowCount,
            true,
            null,
            PropsUi.getInstance(),
            true,
            null,
            false,
            false); // no TableView toolbar; copy/filter are on our toolbar
    pipelineGridView.setSortable(true);
    metricsColumnKeys = getColumnKeys(usedMetrics);
    attachColumnWidthListeners();
    attachMetricsTableListeners(pipelineGridView);
    FormData fdView = new FormData();
    fdView.left = new FormAttachment(0, 0);
    fdView.right = new FormAttachment(100, 0);
    fdView.top = new FormAttachment(toolbar, 0);
    fdView.bottom = new FormAttachment(100, 0);
    pipelineGridView.setLayoutData(fdView);
    pipelineGridComposite.layout(true, true);
    restoreColumnWidths(usedMetrics);
  }

  /**
   * Builds the list of column keys in table order (#, TransformName, Copy, metric codes, duration,
   * speed, status).
   */
  private List<String> getColumnKeys(List<IEngineMetric> usedMetrics) {
    List<String> keys = new ArrayList<>();
    keys.add("#");
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

  /** Saves current column widths to the session map so they can be restored after refresh. */
  private void saveColumnWidths() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    if (metricsColumnKeys == null || metricsColumnKeys.size() == 0) {
      return;
    }
    org.eclipse.swt.widgets.Table table = pipelineGridView.table;
    int n = Math.min(table.getColumnCount(), metricsColumnKeys.size());
    for (int i = 0; i < n; i++) {
      int w = table.getColumn(i).getWidth();
      if (w > 0) {
        metricsColumnWidths.put(metricsColumnKeys.get(i), w);
      }
    }
  }

  /**
   * Restores column widths from the session map onto the table. Needed for the "#" column (no
   * ColumnInfo) and as a fallback so widths are applied after optWidth.
   */
  private void restoreColumnWidths(List<IEngineMetric> usedMetrics) {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    restoringColumnWidths = true;
    try {
      List<String> keys = getColumnKeys(usedMetrics);
      Table table = pipelineGridView.table;
      int n = Math.min(table.getColumnCount(), keys.size());
      for (int i = 0; i < n; i++) {
        String key = keys.get(i);
        Integer w = metricsColumnWidths.get(key);
        if (w != null && w > 0) {
          TableColumn tc = table.getColumn(i);
          tc.setWidth(w);
        }
      }
    } finally {
      // Clear flag asynchronously so any Resize events queued by setWidth() are still ignored
      Display display =
          pipelineGridView != null && !pipelineGridView.isDisposed()
              ? pipelineGridView.table.getDisplay()
              : null;
      if (display != null && !display.isDisposed()) {
        display.asyncExec(() -> restoringColumnWidths = false);
      } else {
        restoringColumnWidths = false;
      }
    }
  }

  /** Attach resize listeners so user column resizes are stored for the session. */
  private void attachColumnWidthListeners() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    if (metricsColumnKeys == null || metricsColumnKeys.size() == 0) {
      return;
    }
    Table table = pipelineGridView.table;
    for (int i = 0; i < table.getColumnCount() && i < metricsColumnKeys.size(); i++) {
      final String key = metricsColumnKeys.get(i);
      TableColumn col = table.getColumn(i);
      col.addListener(
          SWT.Resize,
          e -> {
            if (restoringColumnWidths) {
              return;
            }
            if (!col.isDisposed()) {
              metricsColumnWidths.put(key, col.getWidth());
            }
          });
    }
  }

  private void fillTableRows(List<List<String>> componentStringsList, int errorColumnIndex) {
    while (pipelineGridView.table.getItemCount() > componentStringsList.size()) {
      pipelineGridView.table.remove(pipelineGridView.table.getItemCount() - 1);
    }
    int errorsCol = 3 + errorColumnIndex; // row has #, name, copy, then metrics
    Color errorBg = GuiResource.getInstance().getColorLightRed();
    for (int row = 0; row < componentStringsList.size(); row++) {
      List<String> componentStrings = componentStringsList.get(row);
      TableItem item;
      if (row < pipelineGridView.table.getItemCount()) {
        item = pipelineGridView.table.getItem(row);
      } else {
        item = new TableItem(pipelineGridView.table, SWT.NONE);
      }
      for (int col = 0; col < componentStrings.size(); col++) {
        item.setText(col, componentStrings.get(col));
      }
      if (errorColumnIndex >= 0 && errorsCol < componentStrings.size()) {
        long err = parseFormattedLong(componentStrings.get(errorsCol));
        item.setBackground(err > 0 ? errorBg : null);
      }
    }
  }

  private static int indexOfMetric(List<IEngineMetric> usedMetrics, IEngineMetric metric) {
    for (int i = 0; i < usedMetrics.size(); i++) {
      if (usedMetrics.get(i).getHeader().equals(metric.getHeader())) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Attaches all table listeners used by the metrics grid: sort capture, double-click to open
   * transform, and edit toolbar button enable/disable. Call after creating or recreating the table.
   */
  private void attachMetricsTableListeners(TableView view) {
    attachSortListener(view);
    attachDoubleClickListener(view);
    attachEditButtonStateListener(view);
  }

  /**
   * Attach listener to column headers so we remember sort column/direction for the next refresh.
   */
  private void attachSortListener(TableView view) {
    Table table = view.table;
    for (int i = 0; i < table.getColumnCount(); i++) {
      table.getColumn(i).addListener(SWT.Selection, e -> captureSortState());
    }
  }

  /** On double-click, open the transform configuration for the selected row (by transform name). */
  private void attachDoubleClickListener(TableView view) {
    Table table = view.table;
    table.addListener(SWT.DefaultSelection, e -> openTransformForSelectedRow());
  }

  /**
   * Enable/disable the edit (open transform) toolbar button based on table row selection. TableView
   * sets selection inside its MouseDown (editSelected -> setSelection); the Table often does not
   * fire Selection until after mouse handling, so the first click does not run our Selection
   * listener. We run updateEditButtonState on MouseDown via asyncExec so it runs after TableView
   * has applied the selection.
   */
  private void attachEditButtonStateListener(TableView view) {
    Table table = view.table;
    table.addListener(SWT.Selection, e -> updateEditButtonState());
    table.addListener(
        SWT.MouseDown,
        e -> {
          if (table.isDisposed()) {
            return;
          }
          hopGui.getDisplay().asyncExec(this::updateEditButtonState);
        });
    updateEditButtonState();
  }

  /**
   * Enables or disables the open-transform toolbar button based on whether a table row is selected.
   */
  private void updateEditButtonState() {
    if (toolbarWidget == null || toolbar == null || toolbar.isDisposed()) {
      return;
    }
    boolean linesSelected =
        pipelineGridView != null
            && !pipelineGridView.isDisposed()
            && pipelineGridView.table.getSelectionCount() > 0;
    toolbarWidget.enableToolbarItem(TOOLBAR_ICON_OPEN_TRANSFORM, linesSelected);
  }

  /**
   * Opens the transform configuration dialog for the selected table row. Reads the transform name
   * from column 1 (see {@link #buildComponentStringsList}); finds the {@link TransformMeta} and
   * calls {@link HopGuiPipelineGraph#editTransform(PipelineMeta, TransformMeta)}.
   */
  private void openTransformForSelectedRow() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    Table table = pipelineGridView.table;
    TableItem[] selection = table.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    // Column 0 = #, column 1 = transform name (matches buildComponentStringsList)
    String transformName = selection[0].getText(1);
    if (Utils.isEmpty(transformName)) {
      return;
    }
    PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();
    if (pipelineMeta == null) {
      return;
    }
    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
    if (transformMeta == null) {
      return;
    }
    pipelineGraph.editTransform(pipelineMeta, transformMeta);
  }

  private void captureSortState() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    hopGui
        .getDisplay()
        .asyncExec(
            () -> {
              if (pipelineGridView == null || pipelineGridView.isDisposed()) {
                return;
              }
              Table table = pipelineGridView.table;
              TableColumn sortCol = table.getSortColumn();
              if (sortCol != null) {
                gridSortColumn = table.indexOf(sortCol);
                gridSortDescending = (table.getSortDirection() == SWT.DOWN);
              }
            });
  }

  /**
   * Sort the row data by the given column and direction before writing to the table. Column index 0
   * = #, 1 = name, 2 = copy, 3+ = metrics, then duration, speed, status. Numeric columns (formatted
   * with commas) are compared as numbers; others as strings.
   */
  private void sortComponentStringsByColumn(
      List<List<String>> rows, int sortColumn, boolean descending) {
    if (sortColumn < 0 || rows.isEmpty()) {
      return;
    }
    int col = sortColumn;
    boolean desc = descending;
    rows.sort(
        (a, b) -> {
          String sa = col < a.size() ? Const.NVL(a.get(col), "") : "";
          String sb = col < b.size() ? Const.NVL(b.get(col), "") : "";
          int c = compareCellValues(sa, sb);
          return desc ? -c : c;
        });
  }

  private int compareCellValues(String a, String b) {
    String sa = a != null ? a : "";
    String sb = b != null ? b : "";
    try {
      long na = parseFormattedLong(sa);
      long nb = parseFormattedLong(sb);
      return Long.compare(na, nb);
    } catch (NumberFormatException e) {
      // not both numeric, compare as strings
    }
    return sa.compareToIgnoreCase(sb);
  }

  private static long parseFormattedLong(String s) {
    if (s == null || s.isEmpty()) {
      return 0L;
    }
    String trimmed = s.trim();
    if (trimmed.isEmpty()) {
      return 0L;
    }
    // Strip trailing unit suffix so we can parse the number (e.g. "1,234 rows" or "1,234 r" ->
    // 1234)
    trimmed = trimmed.replaceAll("\\s+(rows|r|runs|flushes|rows/s)$", "");
    return Long.parseLong(trimmed.replace(",", ""));
  }

  /** Update the table header to show which column is sorted and in which direction. */
  private void setSortIndicator() {
    if (pipelineGridView == null || pipelineGridView.isDisposed()) {
      return;
    }
    Table table = pipelineGridView.table;
    if (gridSortColumn >= 0 && gridSortColumn < table.getColumnCount()) {
      table.setSortColumn(table.getColumn(gridSortColumn));
      table.setSortDirection(gridSortDescending ? SWT.DOWN : SWT.UP);
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

  private String calculateDuration(IEngineComponent component) {
    String duration;
    Date firstRowReadDate = component.getFirstRowReadDate();
    if (firstRowReadDate != null) {
      long durationMs;
      if (component.getLastRowWrittenDate() == null) {
        durationMs = System.currentTimeMillis() - firstRowReadDate.getTime();
      } else {
        durationMs = component.getLastRowWrittenDate().getTime() - firstRowReadDate.getTime();
      }
      duration = Utils.getDurationHMS(((double) durationMs) / 1000);
    } else {
      duration = "";
    }
    return duration;
  }

  private static final String METRICS_FORMAT = " ###,###,###,###";

  private static NumberFormat metricFormat = new DecimalFormat(METRICS_FORMAT);

  private String formatMetric(Long value) {
    return metricFormat.format(value);
  }

  private void updateRowFromBaseTransform(ITransform baseTransform, TableItem row) {
    TransformStatus transformStatus = new TransformStatus(baseTransform);

    String[] fields = transformStatus.getPipelineLogFields();

    updateCellsIfChanged(fields, row);

    if (baseTransform.getErrors() > 0) {
      row.setBackground(GuiResource.getInstance().getColorLightRed());
    } else {
      row.setBackground(null);
    }
  }

  /**
   * Anti-flicker: if nothing has changed, don't change it on the screen!
   *
   * @param fields
   * @param row
   */
  private void updateCellsIfChanged(String[] fields, TableItem row) {
    for (int f = 1; f < fields.length; f++) {
      if (!fields[f].equalsIgnoreCase(row.getText(f))) {
        row.setText(f, fields[f]);
      }
    }
  }

  /**
   * Sub Transform Compare
   *
   * <p>Note - nulls must be handled outside of this method
   *
   * @param o1 - First object to compare
   * @param o2 - Second object to compare
   * @return 0 if equal, integer greater than 0 if o1 > o2, integer less than 0 if o2 > o1
   */
  static int subTransformCompare(Object o1, Object o2) {
    final String[] string1 = o1.toString().split("\\.");
    final String[] string2 = o2.toString().split("\\.");

    // Compare the base transform first
    int cmp = Integer.compare(Integer.parseInt(string1[0]), Integer.parseInt(string2[0]));

    // if the base transform numbers are equal, then we need to compare the sub transform numbers
    if (cmp == 0) {
      if (string1.length == 2 && string2.length == 2) {
        // compare the sub transform numbers
        cmp = Integer.compare(Integer.parseInt(string1[1]), Integer.parseInt(string2[1]));
      } else if (string1.length < string2.length) {
        cmp = -1;
      } else if (string2.length < string1.length) {
        cmp = 1;
      }
    }
    return cmp;
  }
}
