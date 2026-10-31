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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.hop.core.CursorRowSet;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.debug.DebugController;
import org.apache.hop.pipeline.debug.DebugController.HopPause;
import org.apache.hop.pipeline.debug.DebugController.StepResult;
import org.apache.hop.pipeline.debug.DebugPauseScope;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Execution-results workbench for a debug session. Transform and hop actions are toolbar plugins
 * above those tables. Rows and metadata are two tabs.
 */
@GuiPlugin(description = "Pipeline debug panel")
public class PipelineDebugPanel {

  private static final Class<?> PKG = PipelineDebugPanel.class;
  private static final int DEFAULT_ROW_PAGE_SIZE = 200;

  /** Toolbar above the transform copies. Other plugins add buttons with this parent id. */
  public static final String TRANSFORM_TOOLBAR = "PipelineDebugPanel-Transform-Toolbar";

  /** Toolbar above the hop buffers. Other plugins add buttons with this parent id. */
  public static final String HOP_TOOLBAR = "PipelineDebugPanel-Hop-Toolbar";

  public static final String TOOLBAR_EXIT = "PipelineDebugPanel-Transform-Toolbar-10000-Exit";
  public static final String TOOLBAR_RESET = "PipelineDebugPanel-Transform-Toolbar-10010-Reset";
  public static final String TOOLBAR_PROCESS_ONE =
      "PipelineDebugPanel-Transform-Toolbar-10020-ProcessOne";
  public static final String TOOLBAR_COUNT = "PipelineDebugPanel-Transform-Toolbar-10030-Count";
  public static final String TOOLBAR_PROCESS_MANY =
      "PipelineDebugPanel-Transform-Toolbar-10040-ProcessMany";
  public static final String TOOLBAR_RUN_SELECTED =
      "PipelineDebugPanel-Transform-Toolbar-10050-RunSelected";
  public static final String TOOLBAR_RUN_TO_HERE =
      "PipelineDebugPanel-Transform-Toolbar-10060-RunToHere";
  public static final String TOOLBAR_CONTINUE =
      "PipelineDebugPanel-Transform-Toolbar-10070-Continue";
  public static final String TOOLBAR_PAUSE = "PipelineDebugPanel-Transform-Toolbar-10080-Pause";
  public static final String TOOLBAR_SELECT_OUTPUT =
      "PipelineDebugPanel-Transform-Toolbar-10090-SelectOutput";
  public static final String TOOLBAR_FOLLOW_OUTPUT =
      "PipelineDebugPanel-Transform-Toolbar-10100-FollowOutput";
  public static final String TOOLBAR_REWIND = "PipelineDebugPanel-Hop-Toolbar-10000-Rewind";
  public static final String TOOLBAR_CLEAR = "PipelineDebugPanel-Hop-Toolbar-10010-Clear";
  public static final String TOOLBAR_PAUSE_CONDITION =
      "PipelineDebugPanel-Hop-Toolbar-10020-PauseCondition";
  public static final String TOOLBAR_CLEAR_PAUSE =
      "PipelineDebugPanel-Hop-Toolbar-10030-ClearPause";

  /** Toolbar above the hop row viewer. Other plugins add buttons with this parent id. */
  public static final String ROWS_TOOLBAR = "PipelineDebugPanel-Rows-Toolbar";

  public static final String TOOLBAR_ROWS_PAGE_UP = "PipelineDebugPanel-Rows-Toolbar-10000-PageUp";
  public static final String TOOLBAR_ROWS_PAGE_DOWN =
      "PipelineDebugPanel-Rows-Toolbar-10010-PageDown";
  public static final String TOOLBAR_ROWS_START = "PipelineDebugPanel-Rows-Toolbar-10020-Start";
  public static final String TOOLBAR_ROWS_END = "PipelineDebugPanel-Rows-Toolbar-10030-End";
  public static final String TOOLBAR_ROWS_COUNT = "PipelineDebugPanel-Rows-Toolbar-10040-Count";
  public static final String TOOLBAR_ROWS_NUMBER = "PipelineDebugPanel-Rows-Toolbar-10050-Number";
  public static final String TOOLBAR_ROWS_GO = "PipelineDebugPanel-Rows-Toolbar-10060-Go";
  public static final String TOOLBAR_ROWS_FIND = "PipelineDebugPanel-Rows-Toolbar-10085-Find";
  public static final String TOOLBAR_ROWS_FIND_TEXT =
      "PipelineDebugPanel-Rows-Toolbar-10080-FindText";
  public static final String TOOLBAR_ROWS_FIND_NEXT =
      "PipelineDebugPanel-Rows-Toolbar-10090-FindNext";
  public static final String TOOLBAR_ROWS_CLEAR_FIND =
      "PipelineDebugPanel-Rows-Toolbar-10100-ClearFind";

  private final IVariables variables;
  private final Supplier<DebugController> controller;
  private final Consumer<String> currentTransform;
  private final Runnable exitDebug;
  private final Runnable resetDebug;

  private final Label status;
  private final GuiToolbarWidgets transformToolbar = new GuiToolbarWidgets();
  private final GuiToolbarWidgets hopToolbar = new GuiToolbarWidgets();
  private final GuiToolbarWidgets rowsToolbar = new GuiToolbarWidgets();
  private final Composite rowTableHost;
  private long rowWindow;
  private int rowPageSize = DEFAULT_ROW_PAGE_SIZE;
  private String rowHopKey = "";
  private long pendingRowSelection = -1L;
  private long lastFindIndex = -1L;
  private String lastFindQuery = "";
  private boolean findingRow;
  private volatile int findGeneration;
  private final Table transforms;
  private final Table hops;
  private final Composite rowHost;
  private final Composite metadataHost;

  private TableView rowView;
  private TableView metadataView;
  private String rowColumns = "";
  private boolean refreshing;
  private boolean busy;
  private final Map<String, IRowMeta> calculatedFields = new HashMap<>();

  public PipelineDebugPanel(
      Composite parent,
      IVariables variables,
      Supplier<DebugController> controller,
      Consumer<String> currentTransform,
      Runnable exitDebug,
      Runnable resetDebug) {
    this.variables = variables;
    this.controller = controller;
    this.currentTransform = currentTransform;
    this.exitDebug = exitDebug;
    this.resetDebug = resetDebug;

    GridLayout layout = new GridLayout(1, false);
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    parent.setLayout(layout);

    status = new Label(parent, SWT.WRAP);
    status.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
    status.setText(text("PipelineDebugPanel.Status.NoSession"));

    SashForm sash = new SashForm(parent, SWT.HORIZONTAL);
    sash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

    SashForm lists = new SashForm(sash, SWT.VERTICAL);
    transforms = listWithToolbar(lists, true);
    transforms.addListener(SWT.Selection, e -> onTransformSelected());
    hops = listWithToolbar(lists, false);
    hops.addListener(
        SWT.Selection,
        e -> {
          if (!refreshing) {
            fillDetails();
          }
        });
    lists.setWeights(50, 50);

    CTabFolder tabs = new CTabFolder(sash, SWT.BORDER);
    rowHost = new Composite(tabs, SWT.NONE);
    GridLayout rowsLayout = new GridLayout(1, false);
    rowsLayout.marginWidth = 0;
    rowsLayout.marginHeight = 0;
    rowHost.setLayout(rowsLayout);
    createToolbar(rowHost, ROWS_TOOLBAR, rowsToolbar);
    rowTableHost = new Composite(rowHost, SWT.NONE);
    rowTableHost.setLayout(new FillLayout());
    rowTableHost.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    CTabItem rowsTab = new CTabItem(tabs, SWT.NONE);
    rowsTab.setText(text("PipelineDebugPanel.Tab.Rows"));
    rowsTab.setControl(rowHost);

    metadataHost = new Composite(tabs, SWT.NONE);
    metadataHost.setLayout(new FillLayout());
    CTabItem metadataTab = new CTabItem(tabs, SWT.NONE);
    metadataTab.setText(text("PipelineDebugPanel.Tab.Metadata"));
    metadataTab.setControl(metadataHost);
    tabs.setSelection(rowsTab);
    sash.setWeights(40, 60);
  }

  public TableView getMetadataView() {
    return metadataView;
  }

  public Table getHopTable() {
    return hops;
  }

  /** Selects every copy of this hop, then shows the focused copy's rows. */
  public void selectHop(String from, String to) {
    List<TableItem> matches = new ArrayList<>();
    for (TableItem item : hops.getItems()) {
      if (!(item.getData() instanceof CursorRowSet rowSet)) {
        continue;
      }
      if (from.equals(rowSet.getOriginTransformName())
          && to.equals(rowSet.getDestinationTransformName())) {
        matches.add(item);
      }
    }
    if (!matches.isEmpty()) {
      hops.setSelection(matches.toArray(new TableItem[0]));
      fillDetails();
    }
  }

  public void continueRunning() {
    continueAll();
  }

  public void pauseRunning() {
    pause();
  }

  public void refresh() {
    if (status.isDisposed()) {
      return;
    }
    DebugController session = controller.get();
    boolean stepping = session != null && !busy;
    enable(transformToolbar, TOOLBAR_EXIT, session != null);
    enable(transformToolbar, TOOLBAR_RESET, !busy);
    enable(transformToolbar, TOOLBAR_PROCESS_ONE, stepping);
    enable(transformToolbar, TOOLBAR_COUNT, stepping);
    enable(transformToolbar, TOOLBAR_PROCESS_MANY, stepping);
    enable(transformToolbar, TOOLBAR_RUN_SELECTED, stepping);
    enable(transformToolbar, TOOLBAR_RUN_TO_HERE, stepping);
    enable(transformToolbar, TOOLBAR_CONTINUE, stepping);
    enable(transformToolbar, TOOLBAR_PAUSE, stepping);
    enable(transformToolbar, TOOLBAR_SELECT_OUTPUT, false);
    enable(hopToolbar, TOOLBAR_REWIND, stepping);
    enable(hopToolbar, TOOLBAR_CLEAR, stepping);
    enable(hopToolbar, TOOLBAR_PAUSE_CONDITION, false);
    enable(hopToolbar, TOOLBAR_CLEAR_PAUSE, false);
    enable(rowsToolbar, TOOLBAR_ROWS_PAGE_UP, false);
    enable(rowsToolbar, TOOLBAR_ROWS_PAGE_DOWN, false);
    enable(rowsToolbar, TOOLBAR_ROWS_START, false);
    enable(rowsToolbar, TOOLBAR_ROWS_END, false);
    enable(rowsToolbar, TOOLBAR_ROWS_GO, false);
    enable(rowsToolbar, TOOLBAR_ROWS_FIND, false);
    enable(rowsToolbar, TOOLBAR_ROWS_FIND_NEXT, false);
    enable(rowsToolbar, TOOLBAR_ROWS_CLEAR_FIND, false);
    if (session == null) {
      status.setText(text("PipelineDebugPanel.Status.NoSession"));
      transforms.removeAll();
      hops.removeAll();
      fillDetails();
      return;
    }
    refreshing = true;
    try {
      fillTransforms(session);
      fillHops(session);
    } finally {
      refreshing = false;
    }
    enable(transformToolbar, TOOLBAR_SELECT_OUTPUT, hasSelectedOutputHop());
    enable(hopToolbar, TOOLBAR_PAUSE_CONDITION, focusedHop() != null);
    enable(hopToolbar, TOOLBAR_CLEAR_PAUSE, selectedHopsHavePause());
    boolean rowsReady = focusedHop() != null;
    enable(rowsToolbar, TOOLBAR_ROWS_PAGE_UP, rowsReady);
    enable(rowsToolbar, TOOLBAR_ROWS_PAGE_DOWN, rowsReady);
    enable(rowsToolbar, TOOLBAR_ROWS_START, rowsReady);
    enable(rowsToolbar, TOOLBAR_ROWS_END, rowsReady);
    enable(rowsToolbar, TOOLBAR_ROWS_GO, rowsReady && !findingRow);
    enable(rowsToolbar, TOOLBAR_ROWS_FIND, rowsReady && !findingRow);
    enable(rowsToolbar, TOOLBAR_ROWS_FIND_NEXT, rowsReady && !findingRow);
    enable(rowsToolbar, TOOLBAR_ROWS_CLEAR_FIND, rowsReady);
    fillDetails();
    if (!findingRow) {
      String pauseMessage = session.getPauseMessage();
      status.setText(pauseMessage == null || pauseMessage.isBlank() ? advice() : pauseMessage);
    }
    // GTK keeps the previous cell text until the table is redrawn.
    transforms.redraw();
    hops.redraw();
  }

  /** What to do next with the current transform and hop selection. */
  private String advice() {
    int selectedTransforms = transforms.getSelectionCount();
    if (selectedTransforms == 0) {
      return text("PipelineDebugPanel.Status.AdviceSelectTransform");
    }
    if (selectedTransforms > 1) {
      return BaseMessages.getString(
          PKG, "PipelineDebugPanel.Status.AdviceCopies", Integer.toString(selectedTransforms));
    }
    TableItem focused = transforms.getItem(transforms.getSelectionIndex());
    String name = focused.getText(0);
    String copy = focused.getText(1);
    CursorRowSet hop = focusedHop();
    if (hop == null) {
      return BaseMessages.getString(
          PKG, "PipelineDebugPanel.Status.AdviceTransform", name + " copy " + copy);
    }
    String hopLabel =
        textOf(hop.getOriginTransformName())
            + " copy "
            + hop.getOriginTransformCopy()
            + " -> "
            + textOf(hop.getDestinationTransformName())
            + " copy "
            + hop.getDestinationTransformCopy();
    int selectedHopCount = hops.getSelectionCount();
    if (selectedHopCount > 1) {
      return BaseMessages.getString(
          PKG,
          "PipelineDebugPanel.Status.AdviceHops",
          Integer.toString(selectedHopCount),
          hopLabel);
    }
    if (hop.storedCount() > rowPageSize) {
      long[] span = visibleSpan(hop.storedCount());
      return BaseMessages.getString(
          PKG,
          "PipelineDebugPanel.Status.AdviceHopWindow",
          name,
          copy,
          hopLabel,
          Long.toString(span[0]),
          Long.toString(span[1]),
          Long.toString(hop.storedCount()));
    }
    return BaseMessages.getString(PKG, "PipelineDebugPanel.Status.AdviceHop", name, copy, hopLabel);
  }

  private Table listWithToolbar(Composite parent, boolean transformsList) {
    Composite pane = new Composite(parent, SWT.NONE);
    GridLayout layout = new GridLayout(1, false);
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    pane.setLayout(layout);

    createToolbar(
        pane,
        transformsList ? TRANSFORM_TOOLBAR : HOP_TOOLBAR,
        transformsList ? transformToolbar : hopToolbar);

    Table table =
        new Table(pane, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    table.setHeaderVisible(true);
    table.setLinesVisible(true);
    table.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    for (String key : transformsList ? transformColumns() : hopColumns()) {
      TableColumn column = new TableColumn(table, SWT.LEFT);
      column.setText(text(key));
      column.setWidth(80);
    }
    fitColumns(table);
    return table;
  }

  private static String[] transformColumns() {
    return new String[] {
      "PipelineDebugPanel.Column.Name",
      "PipelineDebugPanel.Column.Copy",
      "PipelineDebugPanel.Column.Status",
      "PipelineDebugPanel.Column.Read",
      "PipelineDebugPanel.Column.Written",
      "PipelineDebugPanel.Column.In",
      "PipelineDebugPanel.Column.Out",
      "PipelineDebugPanel.Column.Errors"
    };
  }

  private static String[] hopColumns() {
    return new String[] {
      "PipelineDebugPanel.Column.From",
      "PipelineDebugPanel.Column.Copy",
      "PipelineDebugPanel.Column.To",
      "PipelineDebugPanel.Column.Copy",
      "PipelineDebugPanel.Column.Stored",
      "PipelineDebugPanel.Column.Cursor",
      "PipelineDebugPanel.Column.Unread"
    };
  }

  private void createToolbar(Composite parent, String root, GuiToolbarWidgets widgets) {
    IToolbarContainer container =
        ToolbarFacade.createToolbarContainer(parent, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolbar = container.getControl();
    toolbar.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
    PropsUi.setLook(toolbar, PropsUi.WIDGET_STYLE_TOOLBAR);
    widgets.registerGuiPluginObject(this);
    widgets.createToolbarWidgets(container, root);
    toolbar.pack();
  }

  private static void enable(GuiToolbarWidgets widgets, String id, boolean enabled) {
    widgets.enableToolbarItem(id, enabled);
  }

  private int processCount() {
    Control control = transformToolbar.findControl(TOOLBAR_COUNT);
    if (!(control instanceof Text text)) {
      return 10;
    }
    try {
      return Math.max(1, Math.min(100_000, Integer.parseInt(text.getText().trim())));
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_EXIT,
      toolTip = "i18n::PipelineDebugPanel.Button.Exit.Tooltip",
      image = "ui/images/close.svg")
  public void exitDebugMode() {
    exitDebug.run();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_RESET,
      toolTip = "i18n::PipelineDebugPanel.Button.Reset.Tooltip",
      image = "ui/images/refresh.svg")
  public void resetDebugMode() {
    resetDebug.run();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_PROCESS_ONE,
      separator = true,
      toolTip = "i18n::PipelineDebugPanel.Button.ProcessOne.Tooltip",
      image = "ui/images/arrow-right.svg")
  public void processOne() {
    step(1);
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_COUNT,
      type = GuiToolbarElementType.TEXT,
      label = "i18n::PipelineDebugPanel.Count.Label",
      toolTip = "i18n::PipelineDebugPanel.Count.Tooltip",
      defaultText = "10",
      extraWidth = -80)
  public void processCountChanged() {
    // Process N reads the field. Enter does not start a step.
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_PROCESS_MANY,
      toolTip = "i18n::PipelineDebugPanel.Button.ProcessMany.Tooltip",
      image = "ui/images/run.svg")
  public void processMany() {
    step(processCount());
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_RUN_SELECTED,
      toolTip = "i18n::PipelineDebugPanel.Button.RunSelected.Tooltip",
      image = "ui/images/run-all.svg")
  public void runSelectedFromToolbar() {
    runSelected();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_RUN_TO_HERE,
      toolTip = "i18n::PipelineDebugPanel.Button.RunToHere.Tooltip",
      image = "ui/images/target.svg")
  public void runToHereFromToolbar() {
    runToHere();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_CONTINUE,
      separator = true,
      toolTip = "i18n::PipelineDebugPanel.Button.Continue.Tooltip",
      image = "ui/images/running.svg")
  public void continueFromToolbar() {
    continueAll();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_PAUSE,
      toolTip = "i18n::PipelineDebugPanel.Button.Pause.Tooltip",
      image = "ui/images/pause.svg")
  public void pauseFromToolbar() {
    pause();
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_SELECT_OUTPUT,
      separator = true,
      toolTip = "i18n::PipelineDebugPanel.Button.SelectOutput.Tooltip",
      image = "ui/images/hop.svg")
  public void selectOutputHops() {
    List<ITransform> selected = selectedTransforms();
    if (selected.isEmpty()) {
      return;
    }
    List<TableItem> matches = new ArrayList<>();
    for (TableItem item : hops.getItems()) {
      if (!(item.getData() instanceof CursorRowSet rowSet)) {
        continue;
      }
      for (ITransform transform : selected) {
        if (transform.getTransformName().equals(rowSet.getOriginTransformName())
            && transform.getCopy() == rowSet.getOriginTransformCopy()) {
          matches.add(item);
          break;
        }
      }
    }
    if (matches.isEmpty()) {
      return;
    }
    hops.setSelection(matches.toArray(new TableItem[0]));
    hops.showSelection();
    fillDetails();
    status.setText(advice());
  }

  @GuiToolbarElement(
      root = TRANSFORM_TOOLBAR,
      id = TOOLBAR_FOLLOW_OUTPUT,
      type = GuiToolbarElementType.CHECKBOX,
      label = "i18n::PipelineDebugPanel.Button.FollowOutput.Label",
      toolTip = "i18n::PipelineDebugPanel.Button.FollowOutput.Tooltip")
  public void followOutputChanged() {
    if (followOutputHop()) {
      selectFirstOutputHop();
    }
  }

  private boolean followOutputHop() {
    Control control = transformToolbar.findControl(TOOLBAR_FOLLOW_OUTPUT);
    return control instanceof Button button && !button.isDisposed() && button.getSelection();
  }

  /** Selects the first output hop of the transform that has the cursor. */
  private void selectFirstOutputHop() {
    int index = transforms.getSelectionIndex();
    if (index < 0) {
      return;
    }
    Object data = transforms.getItem(index).getData();
    if (!(data instanceof String key)) {
      return;
    }
    int split = key.indexOf('\t');
    if (split < 0) {
      return;
    }
    String name = key.substring(0, split);
    int copy = Integer.parseInt(key.substring(split + 1));
    for (TableItem item : hops.getItems()) {
      if (!(item.getData() instanceof CursorRowSet rowSet)) {
        continue;
      }
      if (name.equals(rowSet.getOriginTransformName()) && copy == rowSet.getOriginTransformCopy()) {
        hops.setSelection(item);
        hops.showSelection();
        fillDetails();
        status.setText(advice());
        return;
      }
    }
  }

  private boolean hasSelectedOutputHop() {
    for (ITransform transform : selectedTransforms()) {
      for (TableItem item : hops.getItems()) {
        if (!(item.getData() instanceof CursorRowSet rowSet)) {
          continue;
        }
        if (transform.getTransformName().equals(rowSet.getOriginTransformName())
            && transform.getCopy() == rowSet.getOriginTransformCopy()) {
          return true;
        }
      }
    }
    return false;
  }

  @GuiToolbarElement(
      root = HOP_TOOLBAR,
      id = TOOLBAR_REWIND,
      toolTip = "i18n::PipelineDebugPanel.Button.Rewind.Tooltip",
      image = "ui/images/navigate-back.svg")
  public void rewindFromToolbar() {
    rewindHop();
  }

  @GuiToolbarElement(
      root = HOP_TOOLBAR,
      id = TOOLBAR_CLEAR,
      toolTip = "i18n::PipelineDebugPanel.Button.ClearHop.Tooltip",
      image = "ui/images/clear.svg")
  public void clearFromToolbar() {
    clearHop();
  }

  @GuiToolbarElement(
      root = HOP_TOOLBAR,
      id = TOOLBAR_PAUSE_CONDITION,
      separator = true,
      toolTip = "i18n::PipelineDebugPanel.Button.PauseCondition.Tooltip",
      image = "ui/images/filter.svg")
  public void editPauseCondition() {
    CursorRowSet hop = focusedHop();
    DebugController session = controller.get();
    if (hop == null || session == null) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    IRowMeta fields = fieldsOf(hop);
    if (fields == null || fields.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.NoHopFields"));
      return;
    }
    HopPause existing = session.getHopPause(hop);
    DebugHopPauseDialog dialog =
        new DebugHopPauseDialog(
            status.getShell(),
            fields,
            existing == null ? null : existing.condition(),
            existing == null ? DebugPauseScope.TRANSFORM : existing.scope());
    if (!dialog.open() || dialog.getCondition() == null) {
      return;
    }
    session.setHopPause(hop, dialog.getCondition(), dialog.getScope());
    status.setText(text("PipelineDebugPanel.Status.PauseConditionSet"));
  }

  @GuiToolbarElement(
      root = HOP_TOOLBAR,
      id = TOOLBAR_CLEAR_PAUSE,
      toolTip = "i18n::PipelineDebugPanel.Button.ClearPause.Tooltip",
      image = "ui/images/delete.svg")
  public void clearPauseCondition() {
    DebugController session = controller.get();
    if (session == null) {
      return;
    }
    List<CursorRowSet> selected = selectedHops();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    int removed = 0;
    for (CursorRowSet hop : selected) {
      if (session.getHopPause(hop) != null) {
        session.clearHopPause(hop);
        removed++;
      }
    }
    status.setText(
        removed == 0
            ? text("PipelineDebugPanel.Status.NoPauseCondition")
            : text("PipelineDebugPanel.Status.PauseConditionCleared"));
  }

  private boolean selectedHopsHavePause() {
    DebugController session = controller.get();
    if (session == null) {
      return false;
    }
    for (CursorRowSet hop : selectedHops()) {
      if (session.getHopPause(hop) != null) {
        return true;
      }
    }
    return false;
  }

  private void step(int rows) {
    List<ITransform> selected = selectedTransforms();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectTransform"));
      return;
    }
    runOffScreen(
        () -> {
          StepResult last = null;
          for (ITransform transform : selected) {
            last = controller.get().process(transform, rows);
          }
          return last;
        });
  }

  private void runSelected() {
    List<ITransform> selected = selectedTransforms();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectTransform"));
      return;
    }
    DebugController session = controller.get();
    if (session == null) {
      return;
    }
    session.runSelected(selected);
    status.setText(text("PipelineDebugPanel.Status.Running"));
    markCurrent(selected.get(0).getTransformName());
    refresh();
  }

  private void runToHere() {
    List<ITransform> selected = selectedTransforms();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectTransform"));
      return;
    }
    ITransform target = selected.get(0);
    runOffScreen(() -> controller.get().runToHere(target, 30_000L));
  }

  private void continueAll() {
    DebugController session = controller.get();
    if (session == null) {
      return;
    }
    session.getPipeline().resumeExecution();
    status.setText(text("PipelineDebugPanel.Status.Running"));
    refresh();
  }

  private void pause() {
    DebugController session = controller.get();
    if (session == null) {
      return;
    }
    session.getPipeline().pauseExecution();
    status.setText(text("PipelineDebugPanel.Status.Paused"));
    refresh();
  }

  private void rewindHop() {
    List<CursorRowSet> selected = selectedHops();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    for (CursorRowSet hop : selected) {
      controller.get().rewind(hop, 0);
    }
    shownRows = "";
    refresh();
    status.setText(hopActionText("PipelineDebugPanel.Session.Rewound", selected.size()));
  }

  private void clearHop() {
    List<CursorRowSet> selected = selectedHops();
    if (selected.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    for (CursorRowSet hop : selected) {
      hop.clear();
    }
    shownRows = "";
    refresh();
    status.setText(hopActionText("PipelineDebugPanel.Session.Cleared", selected.size()));
  }

  private static String hopActionText(String oneKey, int count) {
    if (count == 1) {
      return text(oneKey);
    }
    return BaseMessages.getString(PKG, oneKey + "Many", Integer.toString(count));
  }

  private void runOffScreen(Supplier<StepResult> action) {
    if (busy || controller.get() == null) {
      return;
    }
    busy = true;
    refresh();
    status
        .getDisplay()
        .asyncExec(
            () ->
                new Thread(
                        () -> {
                          StepResult result = null;
                          String failure = null;
                          try {
                            result = action.get();
                          } catch (Exception e) {
                            failure = e.getMessage();
                          }
                          StepResult done = result;
                          String error = failure;
                          status
                              .getDisplay()
                              .asyncExec(
                                  () -> {
                                    busy = false;
                                    if (status.isDisposed()) {
                                      return;
                                    }
                                    if (error != null) {
                                      status.setText(error);
                                    } else if (done != null) {
                                      status.setText(done.detail());
                                    }
                                    refresh();
                                  });
                        },
                        "Hop pipeline debug step")
                    .start());
  }

  private void fillTransforms(DebugController session) {
    List<TransformMetaDataCombi> copies = session.getPipeline().getTransforms();
    if (copies == null) {
      transforms.removeAll();
      return;
    }
    if (sameKeys(transforms, transformKeys(copies))) {
      for (int i = 0; i < copies.size(); i++) {
        ITransform transform = copies.get(i).transform;
        TableItem item = transforms.getItem(i);
        item.setText(2, transformStatus(session, transform));
        item.setText(3, Long.toString(transform.getLinesRead()));
        item.setText(4, Long.toString(transform.getLinesWritten()));
        item.setText(5, Integer.toString(queueDepth(transform.getInputRowSets())));
        item.setText(6, Integer.toString(queueDepth(transform.getOutputRowSets())));
        item.setText(7, Long.toString(transform.getErrors()));
      }
      fitColumns(transforms);
      return;
    }
    List<String> selected = selectedKeys(transforms);
    int top = transforms.getTopIndex();
    boolean wasEmpty = transforms.getItemCount() == 0;
    transforms.removeAll();
    for (TransformMetaDataCombi combi : copies) {
      ITransform transform = combi.transform;
      TableItem item = new TableItem(transforms, SWT.NONE);
      item.setText(0, transform.getTransformName());
      item.setText(1, Integer.toString(transform.getCopy()));
      item.setText(2, transformStatus(session, transform));
      item.setText(3, Long.toString(transform.getLinesRead()));
      item.setText(4, Long.toString(transform.getLinesWritten()));
      item.setText(5, Integer.toString(queueDepth(transform.getInputRowSets())));
      item.setText(6, Integer.toString(queueDepth(transform.getOutputRowSets())));
      item.setText(7, Long.toString(transform.getErrors()));
      item.setData(transformKey(transform));
    }
    restoreRows(transforms, selected, wasEmpty, top);
    if (wasEmpty && transforms.getItemCount() > 0) {
      markCurrent(transforms.getItem(0).getText(0));
    }
    fitColumns(transforms);
  }

  private void fillHops(DebugController session) {
    List<CursorRowSet> rowSets = session.getCursorRowSets();
    if (sameKeys(hops, hopKeys(rowSets))) {
      for (int i = 0; i < rowSets.size(); i++) {
        CursorRowSet rowSet = rowSets.get(i);
        TableItem item = hops.getItem(i);
        item.setText(4, Long.toString(rowSet.storedCount()));
        item.setText(5, Integer.toString(rowSet.getCursor()));
        item.setText(6, Integer.toString(rowSet.size()));
        item.setData(rowSet);
      }
      fitColumns(hops);
      return;
    }
    List<String> selected = selectedKeys(hops);
    int top = hops.getTopIndex();
    boolean wasEmpty = hops.getItemCount() == 0;
    hops.removeAll();
    for (CursorRowSet rowSet : rowSets) {
      TableItem item = new TableItem(hops, SWT.NONE);
      item.setText(0, textOf(rowSet.getOriginTransformName()));
      item.setText(1, Integer.toString(rowSet.getOriginTransformCopy()));
      item.setText(2, textOf(rowSet.getDestinationTransformName()));
      item.setText(3, Integer.toString(rowSet.getDestinationTransformCopy()));
      item.setText(4, Long.toString(rowSet.storedCount()));
      item.setText(5, Integer.toString(rowSet.getCursor()));
      item.setText(6, Integer.toString(rowSet.size()));
      item.setData(rowSet);
    }
    restoreRows(hops, selected, wasEmpty, top);
    fitColumns(hops);
  }

  private static List<String> transformKeys(List<TransformMetaDataCombi> copies) {
    List<String> keys = new ArrayList<>();
    for (TransformMetaDataCombi combi : copies) {
      keys.add(transformKey(combi.transform));
    }
    return keys;
  }

  private static List<String> hopKeys(List<CursorRowSet> rowSets) {
    List<String> keys = new ArrayList<>();
    for (CursorRowSet rowSet : rowSets) {
      keys.add(hopKey(rowSet));
    }
    return keys;
  }

  /** True when the table already lists these copies in this order. Selection can stay put. */
  private static boolean sameKeys(Table table, List<String> keys) {
    if (table.getItemCount() != keys.size()) {
      return false;
    }
    for (int i = 0; i < keys.size(); i++) {
      if (!keys.get(i).equals(itemKey(table.getItem(i)))) {
        return false;
      }
    }
    return true;
  }

  private String shownRows = "";
  private String shownMeta = "";

  private void fillDetails() {
    CursorRowSet hop = focusedHop();
    alignRowWindow(hop);
    // A search reads the hop on its own thread. Rebuilding the page on each pipeline tick
    // flashes the tab and looks like the search started again.
    if (findingRow) {
      return;
    }
    String rowsSignature = rowPageSignature(hop);
    if (!rowsSignature.equals(shownRows) || rowView == null || rowView.isDisposed()) {
      shownRows = rowsSignature;
      fillRowGrid(hop);
    } else {
      updateCursorHighlight(hop);
    }
    String metaSignature = metaSignature(hop);
    if (!metaSignature.equals(shownMeta) || metadataView == null || metadataView.isDisposed()) {
      shownMeta = metaSignature;
      fillMetadata(hop);
    }
  }

  /** The visible page only. Rows appended after this page do not change it. */
  private String rowPageSignature(CursorRowSet hop) {
    if (hop == null) {
      return "none";
    }
    long stored = hop.storedCount();
    long visible = Math.min(rowPageSize, Math.max(0L, stored - rowWindow));
    return hopKey(hop)
        + "|"
        + (hop.getRowMeta() == null ? 0 : hop.getRowMeta().size())
        + "|"
        + rowWindow
        + "|"
        + rowPageSize
        + "|"
        + visible;
  }

  private static String metaSignature(CursorRowSet hop) {
    if (hop == null) {
      return "none";
    }
    return hopKey(hop) + "|" + (hop.getRowMeta() == null ? 0 : hop.getRowMeta().size());
  }

  private void updateCursorHighlight(CursorRowSet hop) {
    if (rowView == null || rowView.isDisposed()) {
      return;
    }
    int cursor = hop == null ? -1 : hop.getCursor();
    for (TableItem item : rowView.getTable().getItems()) {
      if (!(item.getData() instanceof Long index)) {
        continue;
      }
      if (index.longValue() == cursor) {
        item.setBackground(rowView.getDisplay().getSystemColor(SWT.COLOR_INFO_BACKGROUND));
      } else if (index.longValue() != pendingRowSelection) {
        item.setBackground(null);
      }
    }
  }

  private void fillRowGrid(CursorRowSet hop) {
    IRowMeta rowMeta = hop == null ? null : fieldsOf(hop);
    String signature = columnSignature(rowMeta);
    if (rowView == null || rowView.isDisposed() || !signature.equals(rowColumns)) {
      if (rowView != null && !rowView.isDisposed()) {
        rowView.dispose();
      }
      rowColumns = signature;
      rowView =
          new TableView(
              variables,
              rowTableHost,
              SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
              rowColumns(rowMeta),
              0,
              true,
              null,
              PropsUi.getInstance());
      rowView.getTable().addListener(SWT.KeyDown, this::pageRowsFromKey);
      rowTableHost.layout();
    }
    rowView.getTable().removeAll();
    if (hop != null) {
      long stored = hop.storedCount();
      int page = rowPageSize;
      int count = (int) Math.min(page, Math.max(0L, stored - rowWindow));
      for (int i = 0; i < count; i++) {
        long index = rowWindow + i;
        if (index < 0 || index > Integer.MAX_VALUE) {
          continue;
        }
        TableItem item = addRow(rowView, rowMeta, hop.copyStoredRow((int) index));
        if (item != null) {
          item.setData(index);
        }
      }
    }
    rowView.optimizeTableView();
    int cursor = hop == null ? -1 : hop.getCursor();
    for (TableItem item : rowView.getTable().getItems()) {
      if (!(item.getData() instanceof Long index)) {
        continue;
      }
      item.setText(0, Long.toString(index + 1L));
      if (index.longValue() == cursor) {
        item.setBackground(rowView.getDisplay().getSystemColor(SWT.COLOR_INFO_BACKGROUND));
      }
      if (index.longValue() == pendingRowSelection) {
        rowView.getTable().setSelection(item);
        rowView.getTable().showItem(item);
      }
    }
  }

  private void fillMetadata(CursorRowSet hop) {
    if (metadataView == null || metadataView.isDisposed()) {
      metadataView =
          new TableView(
              variables,
              metadataHost,
              SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
              metadataColumns(),
              0,
              true,
              null,
              PropsUi.getInstance());
      metadataHost.layout();
    }
    metadataView.getTable().removeAll();
    if (hop != null) {
      IRowMeta rowMeta = fieldsOf(hop);
      if (rowMeta != null) {
        for (int i = 0; i < rowMeta.size(); i++) {
          metadataView.add(DebugFieldRows.cells(rowMeta.getValueMeta(i)));
        }
      }
    }
    metadataView.optimizeTableView();
  }

  private void onTransformSelected() {
    if (refreshing) {
      return;
    }
    int index = transforms.getSelectionIndex();
    if (index < 0) {
      markCurrent(null);
      return;
    }
    String key = (String) transforms.getItem(index).getData();
    int split = key.indexOf('\t');
    markCurrent(split < 0 ? key : key.substring(0, split));
    if (followOutputHop()) {
      selectFirstOutputHop();
    }
  }

  private void markCurrent(String name) {
    if (currentTransform != null) {
      currentTransform.accept(name);
    }
  }

  private List<ITransform> selectedTransforms() {
    DebugController session = controller.get();
    List<ITransform> found = new ArrayList<>();
    if (session == null) {
      return found;
    }
    for (TableItem item : transforms.getSelection()) {
      String key = (String) item.getData();
      int split = key.indexOf('\t');
      String name = key.substring(0, split);
      int copy = Integer.parseInt(key.substring(split + 1));
      ITransform transform = session.getPipeline().getTransform(name, copy);
      if (transform != null) {
        found.add(transform);
      }
    }
    return found;
  }

  /** The hop whose rows are on screen. Other selected copies stay selected. */
  private CursorRowSet focusedHop() {
    int index = hops.getSelectionIndex();
    if (index < 0) {
      return null;
    }
    Object data = hops.getItem(index).getData();
    return data instanceof CursorRowSet rowSet ? rowSet : null;
  }

  private List<CursorRowSet> selectedHops() {
    List<CursorRowSet> selected = new ArrayList<>();
    for (TableItem item : hops.getSelection()) {
      if (item.getData() instanceof CursorRowSet rowSet) {
        selected.add(rowSet);
      }
    }
    return selected;
  }

  private IRowMeta fieldsOf(CursorRowSet rowSet) {
    if (rowSet.getRowMeta() != null) {
      return rowSet.getRowMeta();
    }
    String key = hopKey(rowSet);
    if (calculatedFields.containsKey(key)) {
      return calculatedFields.get(key);
    }
    IRowMeta fields = null;
    DebugController session = controller.get();
    if (session != null && session.getPipeline().getPipelineMeta() != null) {
      try {
        fields =
            session
                .getPipeline()
                .getPipelineMeta()
                .getTransformFields(variables, rowSet.getOriginTransformName());
      } catch (Exception e) {
        fields = null;
      }
    }
    calculatedFields.put(key, fields);
    return fields;
  }

  private static String transformStatus(DebugController session, ITransform transform) {
    if (transform.getExecutionStartDate() != null && !transform.isRunning()) {
      return text("PipelineDebugPanel.Status.Finished");
    }
    if (session.isFreeRunning(transform)) {
      return text("PipelineDebugPanel.Status.Running");
    }
    return text("PipelineDebugPanel.Status.Paused");
  }

  private static int queueDepth(List<org.apache.hop.core.IRowSet> rowSets) {
    if (rowSets == null) {
      return 0;
    }
    int depth = 0;
    for (org.apache.hop.core.IRowSet rowSet : rowSets) {
      depth += rowSet.size();
    }
    return depth;
  }

  private static TableItem addRow(TableView view, IRowMeta rowMeta, Object[] row) {
    if (rowMeta == null || row == null) {
      return view.add("");
    }
    String[] cells = new String[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      try {
        cells[i] = rowMeta.getString(row, i);
      } catch (HopValueException e) {
        cells[i] = row[i] == null ? "" : String.valueOf(row[i]);
      }
    }
    return view.add(cells);
  }

  private static ColumnInfo[] rowColumns(IRowMeta rowMeta) {
    int fields = rowMeta == null ? 0 : rowMeta.size();
    if (fields == 0) {
      return new ColumnInfo[] {new ColumnInfo(" ", ColumnInfo.COLUMN_TYPE_TEXT, false, true)};
    }
    ColumnInfo[] columns = new ColumnInfo[fields];
    for (int i = 0; i < fields; i++) {
      columns[i] =
          new ColumnInfo(
              rowMeta.getValueMeta(i).getName(), ColumnInfo.COLUMN_TYPE_TEXT, false, true);
    }
    return columns;
  }

  private static ColumnInfo[] metadataColumns() {
    String[] names = DebugFieldRows.COLUMNS;
    ColumnInfo[] columns = new ColumnInfo[names.length];
    for (int i = 0; i < names.length; i++) {
      columns[i] = new ColumnInfo(names[i], ColumnInfo.COLUMN_TYPE_TEXT, false, true);
    }
    return columns;
  }

  private static String columnSignature(IRowMeta rowMeta) {
    if (rowMeta == null || rowMeta.isEmpty()) {
      return "";
    }
    StringBuilder signature = new StringBuilder();
    for (int i = 0; i < rowMeta.size(); i++) {
      signature.append(rowMeta.getValueMeta(i).getName()).append('\t');
    }
    return signature.toString();
  }

  private void alignRowWindow(CursorRowSet hop) {
    String key = hop == null ? "" : hopKey(hop);
    if (!key.equals(rowHopKey)) {
      rowHopKey = key;
      rowWindow = 0;
    }
    readPageSize();
    long stored = hop == null ? 0L : hop.storedCount();
    long maxStart = Math.max(0L, stored - rowPageSize);
    if (rowWindow > maxStart) {
      rowWindow = maxStart;
    }
    if (rowWindow < 0) {
      rowWindow = 0;
    }
  }

  /** First and last stored row numbers on the current page, both 1-based. */
  private long[] visibleSpan(long stored) {
    if (stored <= 0) {
      return new long[] {0L, 0L};
    }
    int count = (int) Math.min(rowPageSize, Math.max(0L, stored - rowWindow));
    if (count <= 0) {
      return new long[] {0L, 0L};
    }
    return new long[] {rowWindow + 1L, rowWindow + count};
  }

  private void readPageSize() {
    Control control = rowsToolbar.findControl(TOOLBAR_ROWS_COUNT);
    if (!(control instanceof Text text)) {
      return;
    }
    try {
      int value = Integer.parseInt(text.getText().trim());
      rowPageSize = Math.max(1, Math.min(100_000, value));
    } catch (NumberFormatException e) {
      // Keep the previous page size while the field is incomplete.
    }
  }

  private void pageRowsFromKey(Event event) {
    if (event.keyCode == SWT.PAGE_DOWN) {
      event.doit = false;
      moveRowWindow(1);
    } else if (event.keyCode == SWT.PAGE_UP) {
      event.doit = false;
      moveRowWindow(-1);
    }
  }

  private void moveRowWindow(int pages) {
    CursorRowSet hop = focusedHop();
    if (hop == null) {
      return;
    }
    readPageSize();
    long stored = hop.storedCount();
    long maxStart = Math.max(0L, stored - rowPageSize);
    long next = rowWindow + pages * (long) rowPageSize;
    if (next < 0L) {
      next = 0L;
    }
    if (next > maxStart) {
      next = maxStart;
    }
    if (next == rowWindow) {
      return;
    }
    rowWindow = next;
    shownRows = "";
    fillDetails();
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_PAGE_UP,
      toolTip = "i18n::PipelineDebugPanel.Rows.PageUp.Tooltip",
      image = "ui/images/arrow-up.svg")
  public void pageRowsUp() {
    moveRowWindow(-1);
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_PAGE_DOWN,
      toolTip = "i18n::PipelineDebugPanel.Rows.PageDown.Tooltip",
      image = "ui/images/arrow-down.svg")
  public void pageRowsDown() {
    moveRowWindow(1);
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_START,
      separator = true,
      toolTip = "i18n::PipelineDebugPanel.Rows.Start.Tooltip",
      image = "ui/images/arrow-left.svg")
  public void showRowsStart() {
    rowWindow = 0;
    shownRows = "";
    fillDetails();
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_END,
      toolTip = "i18n::PipelineDebugPanel.Rows.End.Tooltip",
      image = "ui/images/arrow-right.svg")
  public void showRowsEnd() {
    CursorRowSet hop = focusedHop();
    readPageSize();
    long stored = hop == null ? 0L : hop.storedCount();
    rowWindow = Math.max(0L, stored - rowPageSize);
    shownRows = "";
    fillDetails();
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_COUNT,
      type = GuiToolbarElementType.TEXT,
      separator = true,
      label = "i18n::PipelineDebugPanel.Rows.Count.Label",
      toolTip = "i18n::PipelineDebugPanel.Rows.Count.Tooltip",
      defaultText = "200",
      extraWidth = -80)
  public void rowPageSizeChanged() {
    readPageSize();
    alignRowWindow(focusedHop());
    shownRows = "";
    fillDetails();
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_NUMBER,
      type = GuiToolbarElementType.TEXT,
      separator = true,
      label = "i18n::PipelineDebugPanel.Rows.Number.Label",
      toolTip = "i18n::PipelineDebugPanel.Rows.Number.Tooltip",
      extraWidth = -80)
  public void rowNumberChanged() {
    // The go button reads this field.
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_GO,
      toolTip = "i18n::PipelineDebugPanel.Rows.Go.Tooltip",
      image = "ui/images/target.svg")
  public void goToRowNumber() {
    CursorRowSet hop = focusedHop();
    if (hop == null) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    long stored = hop.storedCount();
    if (stored <= 0) {
      status.setText(text("PipelineDebugPanel.Rows.NoRows"));
      return;
    }
    long number;
    try {
      number = Long.parseLong(toolbarText(TOOLBAR_ROWS_NUMBER).trim());
    } catch (NumberFormatException e) {
      status.setText(text("PipelineDebugPanel.Rows.NeedNumber"));
      return;
    }
    if (number < 1L) {
      number = 1L;
    }
    if (number > stored) {
      number = stored;
    }
    revealStoredRow(number - 1L);
    status.setText(
        BaseMessages.getString(
            PKG, "PipelineDebugPanel.Rows.ShowingNumber", Long.toString(number)));
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_FIND_TEXT,
      type = GuiToolbarElementType.TEXT,
      separator = true,
      label = "i18n::PipelineDebugPanel.Rows.Find.Label",
      toolTip = "i18n::PipelineDebugPanel.Rows.FindText.Tooltip",
      extraWidth = 40)
  public void findTextChanged() {
    // Find and Find next read this field.
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_FIND,
      toolTip = "i18n::PipelineDebugPanel.Rows.Find.Tooltip",
      image = "ui/images/search.svg")
  public void findRow() {
    findStoredRow(false);
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_FIND_NEXT,
      toolTip = "i18n::PipelineDebugPanel.Rows.FindNext.Tooltip",
      image = "ui/images/redo.svg")
  public void findNextRow() {
    findStoredRow(true);
  }

  @GuiToolbarElement(
      root = ROWS_TOOLBAR,
      id = TOOLBAR_ROWS_CLEAR_FIND,
      toolTip = "i18n::PipelineDebugPanel.Rows.ClearFind.Tooltip",
      image = "ui/images/clear.svg")
  public void clearFind() {
    findGeneration++;
    findingRow = false;
    lastFindIndex = -1L;
    lastFindQuery = "";
    pendingRowSelection = -1L;
    Control control = rowsToolbar.findControl(TOOLBAR_ROWS_FIND_TEXT);
    if (control instanceof Text text && !text.isDisposed()) {
      text.setText("");
    }
    refresh();
  }

  private void findStoredRow(boolean next) {
    if (findingRow) {
      return;
    }
    CursorRowSet hop = focusedHop();
    if (hop == null) {
      status.setText(text("PipelineDebugPanel.Status.SelectHop"));
      return;
    }
    String query = toolbarText(TOOLBAR_ROWS_FIND_TEXT).trim();
    if (query.isEmpty()) {
      status.setText(text("PipelineDebugPanel.Rows.NeedFind"));
      return;
    }
    if (!query.equals(lastFindQuery)) {
      lastFindQuery = query;
      lastFindIndex = -1L;
    }
    long start = next ? lastFindIndex + 1L : 0L;
    if (start < 0L) {
      start = 0L;
    }
    long from = start;
    IRowMeta rowMeta = fieldsOf(hop);
    long stored = hop.storedCount();
    String hopKey = hopKey(hop);
    String needle = query.toLowerCase(Locale.ROOT);
    int generation = ++findGeneration;
    findingRow = true;
    refresh();
    status.setText(text("PipelineDebugPanel.Rows.Searching"));
    Display display = status.getDisplay();
    Thread search =
        new Thread(
            () -> {
              long found = scanStoredRows(hop, rowMeta, stored, from, needle, generation);
              display.asyncExec(
                  () -> {
                    if (generation != findGeneration || status.isDisposed()) {
                      return;
                    }
                    findingRow = false;
                    refresh();
                    if (!hopKey.equals(rowHopKey)) {
                      return;
                    }
                    if (found < 0L) {
                      status.setText(
                          text(
                              next
                                  ? "PipelineDebugPanel.Rows.NoFurtherMatch"
                                  : "PipelineDebugPanel.Rows.NoMatch"));
                      return;
                    }
                    lastFindIndex = found;
                    revealStoredRow(found);
                    status.setText(
                        BaseMessages.getString(
                            PKG, "PipelineDebugPanel.Rows.Found", Long.toString(found + 1L)));
                  });
            },
            "Hop debug find row");
    search.setDaemon(true);
    search.start();
  }

  private long scanStoredRows(
      CursorRowSet hop, IRowMeta rowMeta, long stored, long start, String needle, int generation) {
    long limit = Math.min(stored, Integer.MAX_VALUE);
    for (long index = start; index < limit; index++) {
      if (generation != findGeneration) {
        return -1L;
      }
      if (rowContains(rowMeta, hop.copyStoredRow((int) index), needle)) {
        return index;
      }
    }
    return -1L;
  }

  private static boolean rowContains(IRowMeta rowMeta, Object[] row, String needle) {
    if (rowMeta == null || row == null) {
      return false;
    }
    int fields = Math.min(rowMeta.size(), row.length);
    for (int i = 0; i < fields; i++) {
      String value;
      try {
        value = rowMeta.getString(row, i);
      } catch (HopValueException e) {
        value = row[i] == null ? null : String.valueOf(row[i]);
      }
      if (value != null && value.toLowerCase(Locale.ROOT).contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private void revealStoredRow(long index) {
    readPageSize();
    CursorRowSet hop = focusedHop();
    long stored = hop == null ? 0L : hop.storedCount();
    if (stored <= 0L) {
      return;
    }
    if (index < 0L) {
      index = 0L;
    }
    if (index >= stored) {
      index = stored - 1L;
    }
    long maxStart = Math.max(0L, stored - rowPageSize);
    rowWindow = Math.min(index, maxStart);
    pendingRowSelection = index;
    shownRows = "";
    fillDetails();
  }

  private String toolbarText(String id) {
    Control control = rowsToolbar.findControl(id);
    if (control instanceof Text text && !text.isDisposed()) {
      return text.getText();
    }
    return "";
  }

  private static List<String> selectedKeys(Table table) {
    List<String> keys = new ArrayList<>();
    for (TableItem item : table.getSelection()) {
      String key = itemKey(item);
      if (key != null) {
        keys.add(key);
      }
    }
    return keys;
  }

  private static void restoreRows(Table table, List<String> keys, boolean wasEmpty, int top) {
    if (wasEmpty && table.getItemCount() > 0) {
      table.setSelection(0);
    } else if (!keys.isEmpty()) {
      List<TableItem> matches = new ArrayList<>();
      for (TableItem item : table.getItems()) {
        if (keys.contains(itemKey(item))) {
          matches.add(item);
        }
      }
      if (!matches.isEmpty()) {
        table.setSelection(matches.toArray(new TableItem[0]));
      }
    }
    if (table.getItemCount() > 0) {
      table.setTopIndex(Math.min(top, table.getItemCount() - 1));
    }
  }

  /**
   * Grows a column to fit its header and the widest cell. Columns are not shrunk, so a width the
   * user chose stays in place.
   */
  private static void fitColumns(Table table) {
    if (table.isDisposed() || table.getColumnCount() == 0) {
      return;
    }
    GC gc = new GC(table);
    try {
      gc.setFont(table.getFont());
      int pad = Math.max(24, PropsUi.getMargin() * 4);
      for (int columnIndex = 0; columnIndex < table.getColumnCount(); columnIndex++) {
        TableColumn column = table.getColumn(columnIndex);
        int width = gc.stringExtent(column.getText()).x + pad;
        for (TableItem item : table.getItems()) {
          width = Math.max(width, gc.stringExtent(item.getText(columnIndex)).x + pad);
        }
        if (width > column.getWidth()) {
          column.setWidth(width);
        }
      }
    } finally {
      gc.dispose();
    }
  }

  private static String itemKey(TableItem item) {
    Object data = item.getData();
    if (data instanceof CursorRowSet rowSet) {
      return hopKey(rowSet);
    }
    return data == null ? null : data.toString();
  }

  private static String transformKey(ITransform transform) {
    return transform.getTransformName() + "\t" + transform.getCopy();
  }

  private static String hopKey(CursorRowSet rowSet) {
    return rowSet.getOriginTransformName()
        + "\t"
        + rowSet.getOriginTransformCopy()
        + "\t"
        + rowSet.getDestinationTransformName()
        + "\t"
        + rowSet.getDestinationTransformCopy();
  }

  private static String textOf(String value) {
    return value == null ? "" : value;
  }

  private static String text(String key) {
    return BaseMessages.getString(PKG, key);
  }
}
