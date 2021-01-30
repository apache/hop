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

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

@GuiPlugin(description = "Pipeline Graph Grid Delegate")
public class HopGuiPipelineGridDelegate {
  private static final Class<?> PKG = HopGui.class; // For Translator

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowGridDelegate-ToolBar";
  public static final String TOOLBAR_ICON_SHOW_HIDE_INACTIVE = "ToolbarIcon-10000-ShowHideInactive";
  public static final String TOOLBAR_ICON_SHOW_HIDE_SELECTED = "ToolbarIcon-10010-ShowHideSelected";

  public static final long UPDATE_TIME_VIEW = 1000L;

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;

  private CTabItem pipelineGridTab;

  private TableView pipelineGridView;

  private ToolBar toolbar;
  private GuiToolbarWidgets toolbarWidget;

  private Composite pipelineGridComposite;

  private boolean hideInactiveTransforms;

  private boolean showSelectedTransforms;

  private final ReentrantLock refreshViewLock;

  /**
   * @param hopGui
   * @param pipelineGraph
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
    if (pipelineGraph.extraViewComposite == null || pipelineGraph.extraViewComposite.isDisposed()) {
      pipelineGraph.addExtraView();
    } else {
      if (pipelineGridTab != null && !pipelineGridTab.isDisposed()) {
        // just set this one active and get out...
        //
        pipelineGraph.extraViewTabFolder.setSelection(pipelineGridTab);
        return;
      }
    }

    pipelineGridTab = new CTabItem(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineGridTab.setImage(GuiResource.getInstance().getImageShowGrid());
    pipelineGridTab.setText(BaseMessages.getString(PKG, "HopGui.PipelineGraph.GridTab.Name"));

    pipelineGridComposite = new Composite(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineGridComposite.setLayout(new FormLayout());

    addToolBar();

    // ignore whitespace for transformName column valueMeta, causing sorting to ignore whitespace
    String transformNameColumnName =
        BaseMessages.getString(PKG, "PipelineLog.Column.TransformName");
    IValueMeta valueMeta = new ValueMetaString(transformNameColumnName);
    valueMeta.setIgnoreWhitespace(true);
    ColumnInfo transformNameColumnInfo =
        new ColumnInfo(transformNameColumnName, ColumnInfo.COLUMN_TYPE_TEXT, false, true);
    transformNameColumnInfo.setValueMeta(valueMeta);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          transformNameColumnInfo,
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Copynr"),
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
              BaseMessages.getString(PKG, "PipelineLog.Column.Input"),
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
              BaseMessages.getString(PKG, "PipelineLog.Column.Active"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Time"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.Speed"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PipelineLog.Column.PriorityBufferSizes"),
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

    pipelineGridView =
        new TableView(
            pipelineGraph.getVariables(),
            pipelineGridComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            1,
            true, // readonly!
            null, // Listener
            hopGui.getProps());
    FormData fdView = new FormData();
    fdView.left = new FormAttachment(0, 0);
    fdView.right = new FormAttachment(100, 0);
    fdView.top = new FormAttachment(toolbar, 0);
    fdView.bottom = new FormAttachment(100, 0);
    pipelineGridView.setLayoutData(fdView);

    ColumnInfo numberColumn = pipelineGridView.getNumberColumn();
    IValueMeta numberColumnValueMeta =
        new ValueMetaString("#", HopGuiPipelineGridDelegate::subTransformCompare);
    numberColumn.setValueMeta(numberColumnValueMeta);

    // Timer updates the view every UPDATE_TIME_VIEW interval
    final Timer tim = new Timer("HopGuiPipelineGraph: " + pipelineGraph.getMeta().getName());

    TimerTask timtask =
        new TimerTask() {
          public void run() {
            if (!hopGui.getDisplay().isDisposed()) {
              hopGui.getDisplay().asyncExec(HopGuiPipelineGridDelegate.this::refreshView);
            }
          }
        };

    tim.schedule(timtask, 0L, UPDATE_TIME_VIEW);

    pipelineGridTab.addDisposeListener(disposeEvent -> tim.cancel());

    pipelineGridTab.setControl(pipelineGridComposite);

    pipelineGraph.extraViewTabFolder.setSelection(pipelineGridTab);
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiPipelineGridDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiPipelineGraph) {
      HopGuiPipelineGraph graph = (HopGuiPipelineGraph) fileTypeHandler;
      return graph.pipelineGridDelegate;
    }
    return null;
  }

  private void addToolBar() {

    toolbar =
        new ToolBar(
            pipelineGridComposite,
            SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    hopGui.getProps().setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolbarWidget = new GuiToolbarWidgets();
    toolbarWidget.registerGuiPluginObject(this);
    toolbarWidget.createToolbarWidgets(toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolbar.pack();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_HIDE_INACTIVE,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ShowOnlyActiveTransforms",
      image = "ui/images/show.svg")
  public void showHideInactive() {
    hideInactiveTransforms = !hideInactiveTransforms;

    ToolItem toolItem = toolbarWidget.findToolItem(TOOLBAR_ICON_SHOW_HIDE_INACTIVE);
    if (toolItem != null) {
      if (hideInactiveTransforms) {
        toolItem.setImage(GuiResource.getInstance().getImageHide());
      } else {
        toolItem.setImage(GuiResource.getInstance().getImageShow());
      }
    }
    refreshView();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ICON_SHOW_HIDE_SELECTED,
      toolTip = "i18n:org.apache.hop.ui.hopgui:PipelineLog.Button.ShowOnlySelectedTransforms",
      image = "ui/images/show-all.svg")
  public void showHideSelected() {
    showSelectedTransforms = !showSelectedTransforms;

    ToolItem toolItem = toolbarWidget.findToolItem(TOOLBAR_ICON_SHOW_HIDE_SELECTED);
    if (toolItem != null) {
      if (showSelectedTransforms) {
        toolItem.setImage(GuiResource.getInstance().getImageShowSelected());
      } else {
        toolItem.setImage(GuiResource.getInstance().getImageShowAll());
      }
    }
    refreshView();
  }

  private void refreshView() {
    refreshViewLock.lock();
    try {
      if (pipelineGraph.pipeline == null
          || pipelineGridView == null
          || pipelineGridView.isDisposed()) {
        return;
      }

      // Get the metrics from the engine
      //
      EngineMetrics engineMetrics = pipelineGraph.pipeline.getEngineMetrics();
      List<IEngineComponent> shownComponents = new ArrayList<>();
      for (IEngineComponent component : engineMetrics.getComponents()) {
        boolean select = true;
        // If we hide inactive components we only want to see stuff running
        //
        select = select && (!hideInactiveTransforms || component.isRunning());

        // If we opted to only see selected components...
        //
        select = select && (!showSelectedTransforms || component.isSelected());

        if (select) {
          shownComponents.add(component);
        }
      }

      // Build a list of columns to show...
      //
      List<ColumnInfo> columns = new ArrayList<>();

      // First the name of the component (transform):
      // Then the copy number
      //
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

      List<IEngineMetric> usedMetrics = new ArrayList(engineMetrics.getMetricsList());
      Collections.sort(
          usedMetrics, (o1, o2) -> o1.getDisplayPriority().compareTo(o2.getDisplayPriority()));

      for (IEngineMetric metric : usedMetrics) {
        ColumnInfo column =
            new ColumnInfo(
                metric.getHeader(), ColumnInfo.COLUMN_TYPE_TEXT, metric.isNumeric(), true);
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

      // Duration?
      //
      ColumnInfo durationColumn =
          new ColumnInfo("Duration", ColumnInfo.COLUMN_TYPE_TEXT, false, true); // TODO i18n
      durationColumn.setValueMeta(stringMeta);
      durationColumn.setAlignment(SWT.RIGHT);
      columns.add(durationColumn);

      // Also add the status and speed
      //
      ValueMetaInteger speedMeta = new ValueMetaInteger("speed", 15, 0);
      speedMeta.setConversionMask(" ###,###,###,##0");
      stringMeta.setConversionMetadata(speedMeta);
      ColumnInfo speedColumn =
          new ColumnInfo("Speed", ColumnInfo.COLUMN_TYPE_TEXT, false, true); // TODO i18n
      speedColumn.setValueMeta(stringMeta);
      speedColumn.setAlignment(SWT.RIGHT);
      columns.add(speedColumn);

      columns.add(new ColumnInfo("Status", ColumnInfo.COLUMN_TYPE_TEXT, false, true)); // TODO i18n

      // The data in the grid...
      //
      List<List<String>> componentStringsList = new ArrayList<>();
      int row = 1;
      for (IEngineComponent component : shownComponents) {
        List<String> componentStrings = new ArrayList<>();

        componentStrings.add(Integer.toString(row++));
        componentStrings.add(Const.NVL(component.getName(), ""));
        componentStrings.add(Integer.toString(component.getCopyNr()));

        for (IEngineMetric metric : usedMetrics) {
          Long value = engineMetrics.getComponentMetric(component, metric);
          componentStrings.add(value == null ? "" : formatMetric(value));
        }
        String duration = calculateDuration(component);
        componentStrings.add(duration);
        String speed = engineMetrics.getComponentSpeedMap().get(component);
        componentStrings.add(Const.NVL(speed, ""));
        String status = engineMetrics.getComponentStatusMap().get(component);
        componentStrings.add(Const.NVL(status, ""));

        componentStringsList.add(componentStrings);
      }

      // So now we have the columns and the content of the grid...
      //
      // If the number of columns has changed since the last refresh we rebuild the table.
      //
      if (haveColumnsChanged(columns)) {
        // Remove the old stuff on the composite...
        //
        pipelineGridView.dispose();
        pipelineGridView =
            new TableView(
                pipelineGraph.getVariables(),
                pipelineGridComposite,
                SWT.NONE,
                columns.toArray(new ColumnInfo[0]),
                shownComponents.size(),
                null,
                PropsUi.getInstance());
        pipelineGridView.setSortable(false); // TODO: re-implement
        FormData fdView = new FormData();
        fdView.left = new FormAttachment(0, 0);
        fdView.right = new FormAttachment(100, 0);
        fdView.top = new FormAttachment(toolbar, 0);
        fdView.bottom = new FormAttachment(100, 0);
        pipelineGridView.setLayoutData(fdView);
        pipelineGridComposite.layout(true, true);
      }

      // If the number of rows in the table is different then we need to remove all rows and
      // rebuild.
      // Otherwise we're just going to re-use the table items and put new values on the cells...
      //
      while (pipelineGridView.table.getItemCount() > componentStringsList.size()) {
        pipelineGridView.table.remove(pipelineGridView.table.getItemCount() - 1);
      }

      for (row = 0; row < componentStringsList.size(); row++) {
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
      }

      // Optimize the view...
      //
      pipelineGridView.optWidth(true);

      previousRefreshColumns = columns;
    } finally {
      refreshViewLock.unlock();
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

    // Error lines should appear in red:
    if (baseTransform.getErrors() > 0) {
      row.setBackground(GuiResource.getInstance().getColorRed());
    } else {
      row.setBackground(GuiResource.getInstance().getColorWhite());
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

  public CTabItem getPipelineGridTab() {
    return pipelineGridTab;
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
