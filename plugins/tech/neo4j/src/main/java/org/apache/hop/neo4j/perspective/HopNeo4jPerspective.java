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

package org.apache.hop.neo4j.perspective;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopConfigException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.logging.Defaults;
import org.apache.hop.neo4j.logging.util.LoggingCore;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;

import java.util.List;
import java.util.*;

@HopPerspectivePlugin(
    id = "HopNeo4jPerspective",
    name = "Neo4j",
    description = "Neo4j Perspective",
    image = "neo4j_logo.svg")
@GuiPlugin
public class HopNeo4jPerspective implements IHopPerspective {

  public static final Class<?> PKG = HopNeo4jPerspective.class;

  public static final String ID_PERSPECTIVE_TOOLBAR_ITEM = "9000-perspective-neo4j";

  private HopGui hopGui;
  private Composite parent;
  private Composite composite;
  private Combo wExecutions;
  private CTabFolder tabFolder;
  private TableView wResults;
  private Text wLogging;
  private Tree wTree;
  private Text wCypher;
  private Color errorLineBackground;
  private Combo wAmount;
  private Button wOnlyRoot;
  private Font defaultTabFont;
  private Text wUsedConnection;

  public HopNeo4jPerspective() {}

  @Override
  public String getId() {
    return "neo4j";
  }

  @Override
  public void activate() {
    // Someone clicked on the Neo4j icon
    //
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    wExecutions.setFocus();

    // Auto-refresh the list
    //
    refreshResults();
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    return new EmptyHopFileTypeHandler(); // Not handling anything really
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler activeFileTypeHandler) {}

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();

    int size = (int) Math.round((double) ConstUi.SMALL_ICON_SIZE * props.getZoomFactor());
    Image neo4jImage =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), this.getClass().getClassLoader(), "neo4j_logo.svg", size, size);
    Image lineageImage =
        SwtSvgImageUtil.getImage(
            hopGui.getDisplay(), this.getClass().getClassLoader(), "lineage.svg", size, size);
    errorLineBackground = new Color(hopGui.getDisplay(), 201, 232, 251);

    composite = new Composite(parent, SWT.NONE);
    props.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginLeft = props.getMargin();
    layout.marginTop = props.getMargin();
    layout.marginLeft = props.getMargin();
    layout.marginBottom = props.getMargin();
    composite.setLayout(layout);

    FormData formData = new FormData();
    formData.left = new FormAttachment(0, 0);
    formData.top = new FormAttachment(0, 0);
    formData.right = new FormAttachment(100, 0);
    formData.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(formData);

    int margin = (int) (props.getMargin() * props.getZoomFactor());

    // Add a simple label to test
    //
    Label wlInfo = new Label(composite, SWT.LEFT);
    props.setLook(wlInfo);
    wlInfo.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Shell.Title"));
    wlInfo.setFont(GuiResource.getInstance().getFontBold());
    FormData fdInfo = new FormData();
    fdInfo.left = new FormAttachment(0, 0);
    fdInfo.right = new FormAttachment(100, 0);
    fdInfo.top = new FormAttachment(0, 0);
    wlInfo.setLayoutData(fdInfo);
    Control lastControl = wlInfo;

    Label wlSep1 = new Label(composite, SWT.SEPARATOR | SWT.HORIZONTAL);
    props.setLook(wlSep1);
    FormData fdlSep1 = new FormData();
    fdlSep1.left = new FormAttachment(0, 0);
    fdlSep1.right = new FormAttachment(100, 0);
    fdlSep1.top = new FormAttachment(lastControl, margin);
    wlSep1.setLayoutData(fdlSep1);
    lastControl = wlSep1;

    Label wlUsedConnection = new Label(composite, SWT.LEFT);
    props.setLook(wlUsedConnection);
    wlUsedConnection.setText(
        BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Neo4J.Logging.Path.Label"));
    FormData fdlLoggingConnection = new FormData();
    fdlLoggingConnection.left = new FormAttachment(0, 0);
    fdlLoggingConnection.top = new FormAttachment(lastControl, margin);
    wlUsedConnection.setLayoutData(fdlLoggingConnection);
    wUsedConnection = new Text(composite, SWT.SINGLE | SWT.BORDER);
    wUsedConnection.setEditable(false);
    props.setLook(wUsedConnection);
    wUsedConnection.setText(
        BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Neo4J.Logging.Path.Label"));
    wUsedConnection.setFont(GuiResource.getInstance().getFontBold());
    FormData fdLoggingConnection = new FormData();
    fdLoggingConnection.left = new FormAttachment(wlUsedConnection, margin);
    fdLoggingConnection.right = new FormAttachment(50, 0);
    fdLoggingConnection.top = new FormAttachment(lastControl, margin);
    wUsedConnection.setLayoutData(fdLoggingConnection);
    lastControl = wUsedConnection;

    // The search query
    //
    Label wlExecutions = new Label(composite, SWT.LEFT);
    props.setLook(wlExecutions);
    wlExecutions.setText(
        BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.PipelineActionName.Label"));
    FormData fdlExecutions = new FormData();
    fdlExecutions.left = new FormAttachment(0, 0);
    fdlExecutions.top = new FormAttachment(lastControl, margin);
    wlExecutions.setLayoutData(fdlExecutions);
    lastControl = wlExecutions;

    wExecutions = new Combo(composite, SWT.BORDER | SWT.SINGLE);
    props.setLook(wExecutions);
    wExecutions.setFont(GuiResource.getInstance().getFontBold());
    FormData fdSearchString = new FormData();
    fdSearchString.left = new FormAttachment(0, 0);
    fdSearchString.top = new FormAttachment(lastControl, margin);
    fdSearchString.right = new FormAttachment(50, 0);
    wExecutions.setLayoutData(fdSearchString);
    wExecutions.addListener(SWT.DefaultSelection, this::search);
    wExecutions.addListener(SWT.Selection, this::search);

    Label wlAmount = new Label(composite, SWT.LEFT);
    props.setLook(wlAmount);
    wlAmount.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ShowLast.Label"));
    FormData fdlAmount = new FormData();
    fdlAmount.left = new FormAttachment(wExecutions, 2 * margin);
    fdlAmount.top = new FormAttachment(lastControl, margin);
    wlAmount.setLayoutData(fdlAmount);

    wAmount = new Combo(composite, SWT.BORDER | SWT.SINGLE);
    props.setLook(wAmount);
    wAmount.setItems("50", "100", "250", "500", "1000");
    wAmount.setText("50");
    FormData fdAmount = new FormData();
    fdAmount.left = new FormAttachment(wlAmount, margin);
    fdAmount.top = new FormAttachment(lastControl, margin);
    fdAmount.width = (int) (200 * props.getZoomFactor());
    wAmount.setLayoutData(fdAmount);
    wAmount.addListener(SWT.DefaultSelection, this::search);
    wAmount.addListener(SWT.Selection, this::search);

    wOnlyRoot = new Button(composite, SWT.CHECK);
    wOnlyRoot.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OnlyRoot.Label"));
    wOnlyRoot.setSelection(true); // default is to limit the amount of entries
    props.setLook(wOnlyRoot);
    FormData fdOnlyRoot = new FormData();
    fdOnlyRoot.left = new FormAttachment(wAmount, margin);
    fdOnlyRoot.top = new FormAttachment(lastControl, margin);
    wOnlyRoot.setLayoutData(fdOnlyRoot);
    wOnlyRoot.addListener(SWT.Selection, this::search);

    lastControl = wExecutions;

    Button wbSearch = new Button(composite, SWT.PUSH);
    props.setLook(wbSearch);
    wbSearch.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Search.Button"));
    FormData fdbSearch = new FormData();
    fdbSearch.left = new FormAttachment(0, 0);
    fdbSearch.top = new FormAttachment(lastControl, margin);
    wbSearch.setLayoutData(fdbSearch);
    wbSearch.addListener(SWT.Selection, this::search);
    lastControl = wExecutions;

    Button wbOpen = new Button(composite, SWT.PUSH);
    props.setLook(wbOpen);
    wbOpen.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Open.Button"));
    FormData fdbOpen = new FormData();
    fdbOpen.left = new FormAttachment(50, 0);
    fdbOpen.bottom = new FormAttachment(100, -margin);
    wbOpen.setLayoutData(fdbOpen);
    wbOpen.addListener(SWT.Selection, this::open);

    SashForm sashForm = new SashForm(composite, SWT.VERTICAL);
    props.setLook(sashForm);
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.top = new FormAttachment(wbSearch, margin);
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.bottom = new FormAttachment(wbOpen, -margin);
    sashForm.setLayoutData(fdSashForm);

    // A table with the execution history results...
    //
    ColumnInfo[] resultsColumns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.ID.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Name.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Type.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Read.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Written.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Input.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Output.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Rejected.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Errors.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Date.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Column.Duration.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
    };

    wResults =
        new TableView(
            hopGui.getVariables(),
            sashForm,
            SWT.V_SCROLL | SWT.V_SCROLL | SWT.SINGLE | SWT.BORDER,
            resultsColumns,
            0,
            null,
            props);
    props.setLook(wResults);
    wResults.setReadonly(true);
    FormData fdResults = new FormData();
    fdResults.left = new FormAttachment(0, 0);
    fdResults.right = new FormAttachment(100, 0);
    fdResults.top = new FormAttachment(0, 0);
    fdResults.bottom = new FormAttachment(100, 0);
    wResults.setLayoutData(fdResults);
    wResults.table.addListener(SWT.DefaultSelection, this::open);
    wResults.table.addListener(SWT.Selection, this::analyze);

    tabFolder = new CTabFolder(sashForm, SWT.MULTI | SWT.BORDER);
    props.setLook(tabFolder, Props.WIDGET_STYLE_TAB);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    fdLabel.top = new FormAttachment(0, 0);
    fdLabel.bottom = new FormAttachment(100, 0);
    tabFolder.setLayoutData(fdLabel);

    CTabItem loggingTab = new CTabItem(tabFolder, SWT.NONE);
    loggingTab.setImage(GuiResource.getInstance().getImageShowLog());
    loggingTab.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Logging.Tab"));
    wLogging = new Text(tabFolder, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wLogging);
    wLogging.setFont(GuiResource.getInstance().getFontFixed());
    loggingTab.setControl(wLogging);

    CTabItem lineageTab = new CTabItem(tabFolder, SWT.NONE);
    lineageTab.setImage(lineageImage);
    lineageTab.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.PathError.Tab"));
    wTree = new Tree(tabFolder, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wTree);
    wTree.setHeaderVisible(true);
    lineageTab.setControl(wTree);
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("#");
      column.setWidth((int) (50 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("id");
      column.setWidth((int) (250 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("Name");
      column.setWidth((int) (300 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("Type");
      column.setWidth((int) (100 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("errors");
      column.setWidth((int) (50 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("date");
      column.setWidth((int) (200 * props.getZoomFactor()));
    }
    {
      TreeColumn column = new TreeColumn(wTree, SWT.LEFT);
      column.setText("duration");
      column.setWidth((int) (150 * props.getZoomFactor()));
    }

    CTabItem cypherTab = new CTabItem(tabFolder, SWT.NONE);
    cypherTab.setText(BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.Cypher.Tab"));
    cypherTab.setImage(neo4jImage);
    wCypher = new Text(tabFolder, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wCypher);
    wCypher.setFont(GuiResource.getInstance().getFontFixed());
    cypherTab.setControl(wCypher);

    tabFolder.setSelection(0);
    sashForm.setWeights(new int[] {30, 70});

    defaultTabFont = lineageTab.getFont();

    wTree.addListener(SWT.DefaultSelection, this::openItem);
  }

  /**
   * Analyze a log record entry when a user clicks on it.
   *
   * @param event
   */
  private void analyze(Event event) {
    ILogChannel log = hopGui.getLog();

    System.out.println("Analyze");
    if (!(event.item instanceof TableItem)) {
      return;
    }
    TableItem item = (TableItem) event.item;
    String id = item.getText(1);
    String name = item.getText(2);
    String type = item.getText(3);
    int errors = Const.toInt(item.getText(9), -1);

    try {

      final NeoConnection connection = findLoggingConnection();
      if (connection == null) {
        return;
      }
      log.logDetailed("Logging workflow information to Neo4j connection : " + connection.getName());

      try (Driver driver = connection.getDriver(log, hopGui.getVariables())) {
        try (Session session = connection.getSession(log, driver, hopGui.getVariables())) {
          analyzeLogging(session, id, name, type);
          List<List<HistoryResult>> shortestPaths =
              analyzeErrorLineage(session, id, name, type, errors);
          analyzeCypherStatements(connection, session, id, name, type, errors, shortestPaths);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ErrorAnalyze.Dialog.Header"),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ErrorAnalyze.Dialog.Message"),
          e);
    }
  }

  private void analyzeLogging(Session session, String id, String name, String type) {
    // Read the logging data for the selected execution
    //
    Map<String, Object> loggingParameters = new HashMap<>();
    loggingParameters.put("name", name);
    loggingParameters.put("id", id);
    loggingParameters.put("type", type);

    final StringBuilder loggingCypher = new StringBuilder();
    loggingCypher.append("MATCH(e:Execution) ");
    loggingCypher.append("WHERE e.id = $id ");
    loggingCypher.append("AND   e.name = $name ");
    loggingCypher.append("AND   e.type = $type ");
    loggingCypher.append("RETURN e.loggingText ");

    session.readTransaction(
        tx -> {
          Result result = tx.run(loggingCypher.toString(), loggingParameters);
          while (result.hasNext()) {
            Record record = result.next();
            Value value = record.get(0);
            String loggingText = value.asString();
            wLogging.setText(Const.NVL(loggingText, "<no logging found>"));
          }
          return null;
        });
  }

  private List<List<HistoryResult>> analyzeErrorLineage(
      Session session, String id, String name, String type, int errors) {

    // List of shortest paths to errors...
    //
    List<List<HistoryResult>> shortestPaths = new ArrayList<>();

    // Remove the elements in the tree
    //
    for (TreeItem treeItem : wTree.getItems()) {
      treeItem.dispose();
    }

    if (errors > 0) {
      tabFolder.getItem(1).setFont(GuiResource.getInstance().getFontBold());
    } else {
      tabFolder.getItem(1).setFont(defaultTabFont);
    }

    // Now get the lineage to the error (if any)
    //
    if (errors > 0) {
      Map<String, Object> errorPathParams = new HashMap<>();
      errorPathParams.put("subjectName", name);
      errorPathParams.put("subjectType", type);
      errorPathParams.put("subjectId", id);

      StringBuilder errorPathCypher = new StringBuilder();
      errorPathCypher.append(
          "MATCH(top:Execution { name : $subjectName, type : $subjectType, id : $subjectId })-[rel:EXECUTES*]-(err:Execution) ");
      errorPathCypher.append("   , p=shortestpath((top)-[:EXECUTES*]-(err)) ");
      errorPathCypher.append("WHERE top.registrationDate IS NOT NULL ");
      errorPathCypher.append("  AND err.errors > 0 ");
      errorPathCypher.append("  AND size((err)-[:EXECUTES]->())=0 ");
      errorPathCypher.append("RETURN p ");
      errorPathCypher.append("ORDER BY size(RELATIONSHIPS(p)) DESC ");
      errorPathCypher.append("LIMIT 10");

      session.readTransaction(
          tx -> {
            Result pathResult = tx.run(errorPathCypher.toString(), errorPathParams);

            while (pathResult.hasNext()) {
              Record pathRecord = pathResult.next();
              Value pathValue = pathRecord.get(0);
              Path path = pathValue.asPath();
              List<HistoryResult> shortestPath = new ArrayList<>();
              for (Node node : path.nodes()) {
                HistoryResult pathExecution = new HistoryResult();
                pathExecution.setId(LoggingCore.getStringValue(node, "id"));
                pathExecution.setName(LoggingCore.getStringValue(node, "name"));
                pathExecution.setType(LoggingCore.getStringValue(node, "type"));
                pathExecution.setCopy(LoggingCore.getStringValue(node, "copy"));
                pathExecution.setRegistrationDate(
                    LoggingCore.getStringValue(node, "registrationDate"));
                pathExecution.setWritten(LoggingCore.getLongValue(node, "linesWritten"));
                pathExecution.setRead(LoggingCore.getLongValue(node, "linesRead"));
                pathExecution.setInput(LoggingCore.getLongValue(node, "linesInput"));
                pathExecution.setOutput(LoggingCore.getLongValue(node, "linesOutput"));
                pathExecution.setRejected(LoggingCore.getLongValue(node, "linesRejected"));
                pathExecution.setErrors(LoggingCore.getLongValue(node, "errors"));
                pathExecution.setLoggingText(LoggingCore.getStringValue(node, "loggingText"));
                pathExecution.setDurationMs(LoggingCore.getLongValue(node, "durationMs"));

                shortestPath.add(0, pathExecution);
              }
              shortestPaths.add(shortestPath);
            }

            // Populate the tree...
            //
            String treeName = "Execution History of " + name + "(" + type + ")";

            for (int p = shortestPaths.size() - 1; p >= 0; p--) {
              List<HistoryResult> shortestPath = shortestPaths.get(p);

              TreeItem pathItem = new TreeItem(wTree, SWT.NONE);
              pathItem.setText(0, Integer.toString(p + 1));

              for (int e = 0; e < shortestPath.size(); e++) {
                HistoryResult exec = shortestPath.get(e);
                TreeItem execItem = new TreeItem(pathItem, SWT.NONE);
                int x = 0;
                execItem.setText(x++, Integer.toString(e + 1));
                execItem.setText(x++, Const.NVL(exec.getId(), ""));
                execItem.setText(x++, Const.NVL(exec.getName(), ""));
                execItem.setText(x++, Const.NVL(exec.getType(), ""));
                execItem.setText(x++, toString(exec.getErrors()));
                execItem.setText(x++, Const.NVL(exec.getRegistrationDate(), "").replace("T", " "));
                execItem.setText(x++, LoggingCore.getFancyDurationFromMs(exec.getDurationMs()));
                execItem.setExpanded(true);
              }
              if (p == shortestPaths.size() - 1) {
                TreeMemory.getInstance().storeExpanded(treeName, pathItem, true);
              }
            }

            TreeMemory.setExpandedFromMemory(wTree, treeName);

            if (wTree.getItemCount() > 0) {
              TreeItem firstItem = wTree.getItem(0);
              wTree.setSelection(firstItem);
            }

            //
            return null;
          });
    }
    return shortestPaths;
  }

  private void analyzeCypherStatements(
      NeoConnection connection,
      Session session,
      String id,
      String name,
      String type,
      int errors,
      List<List<HistoryResult>> shortestPaths) {

    HistoryResult result = new HistoryResult();
    result.setId(id);
    result.setName(name);
    result.setType(type);
    result.setShortestPaths(shortestPaths);

    IVariables vars = hopGui.getVariables();

    StringBuffer cypher = new StringBuffer();

    cypher.append("Below are a few Cypher statements you can run in the Neo4j browser");
    cypher.append(Const.CR);
    String neoServer = vars.resolve(connection.getServer());
    String neoBrowserPort = vars.resolve(connection.getBrowserPort());
    String browserUrl = "http://" + neoServer + ":" + Const.NVL(neoBrowserPort, "7474");
    cypher.append("URL of the Neo4j browser: ").append(browserUrl);
    cypher.append(Const.CR);
    cypher.append(Const.CR);

    cypher.append("Execution info cypher: ").append(Const.CR);
    cypher.append("--------------------------------------------").append(Const.CR);
    cypher.append(result.getExecutionInfoCommand());
    cypher.append(Const.CR);

    if (errors > 0) {
      cypher.append("Error path lookup: ").append(Const.CR);
      cypher.append("--------------------------------------------").append(Const.CR);
      String errorCypher = result.getErrorPathCommand();
      cypher.append(errorCypher);
      cypher.append(Const.CR);

      if (!shortestPaths.isEmpty()) {
        cypher.append("Error path with metadata: ").append(Const.CR);
        cypher.append("--------------------------------------------").append(Const.CR);
        String allCypher = result.getErrorPathWithMetadataCommand(0);
        cypher.append(allCypher);
        cypher.append(Const.CR);
      }
    }

    wCypher.setText(cypher.toString());
  }

  private String toString(Long lng) {
    if (lng == null) {
      return "";
    }
    return lng.toString();
  }

  private void open(Event event) {

    try {
      NeoConnection connection = findLoggingConnection();
      if (connection == null) {
        return;
      }

      // See if there is a selected line in the error tree...
      //
      TreeItem[] treeSelection = wTree.getSelection();
      if (treeSelection != null || treeSelection.length > 0) {
        TreeItem treeItem = treeSelection[0];
        String id = treeItem.getText(1);
        if (StringUtils.isNotEmpty(id)) {
          openItem(treeItem);
          return;
        }
      }
      // Fallback is opening the selected root workflow or pipeline
      //
      TableItem[] resultsSelection = wResults.table.getSelection();
      if (resultsSelection == null || resultsSelection.length == 0) {
        return;
      }
      TableItem tableItem = resultsSelection[0];
      String id = tableItem.getText(1);
      String name = tableItem.getText(2);
      String type = tableItem.getText(3);
      openItem(connection, id, name, type);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ErrorOpeningPipeline.Dialog.Header"),
          "Neo4jPerspectiveDialog.ErrorOpeningPipeline.Dialog.Message",
          e);
    }
  }

  private void search(Event event) {
    refreshResults();
  }

  private NeoConnection findLoggingConnection() throws HopException {
    IVariables variables = hopGui.getVariables();
    ILogChannel log = hopGui.getLog();
    if (!LoggingCore.isEnabled(variables)) {
      return null;
    }
    String connectionName = variables.getVariable(Defaults.VARIABLE_NEO4J_LOGGING_CONNECTION);

    final NeoConnection connection =
        LoggingCore.getConnection(hopGui.getMetadataProvider(), variables);
    if (connection == null) {
      log.logBasic("Warning! Unable to find Neo4j connection to log to : " + connectionName);
      return null;
    }
    return connection;
  }

  private void refreshResults() {

    // See if logging is enabled
    //
    ILogChannel log = hopGui.getLog();
    String searchName = wExecutions.getText();
    int amount = Const.toInt(wAmount.getText(), 50);
    boolean onlyRoot = wOnlyRoot.getSelection();

    try {

      final NeoConnection connection = findLoggingConnection();
      if (connection == null) {
        wUsedConnection.setText("");
        return;
      }
      wUsedConnection.setText(Const.NVL(connection.getName(), ""));

      log.logDetailed("Logging workflow information to Neo4j connection : " + connection.getName());

      Map<String, Object> resultsParameters = new HashMap<>();

      final StringBuilder resultsCypher = new StringBuilder();
      resultsCypher.append("MATCH(e:Execution) ");
      resultsCypher.append("WHERE e.type in [ 'PIPELINE', 'WORKFLOW' ] ");
      if (StringUtils.isNotEmpty(searchName)) {
        resultsCypher.append("AND e.name = $name ");
        resultsParameters.put("name", searchName);
      }
      if (onlyRoot) {
        resultsCypher.append("AND e.root = true ");
      }
      resultsCypher.append(
          "RETURN e.id, e.name, e.type, e.linesRead, e.linesWritten, e.linesInput, e.linesOutput, e.linesRejected, e.errors,  e.executionStart, e.durationMs ");
      resultsCypher.append("ORDER BY e.executionStart desc ");
      resultsCypher.append("LIMIT " + amount);

      wResults.clearAll(false);
      try (Driver driver = connection.getDriver(log, hopGui.getVariables())) {
        try (Session session = connection.getSession(log, driver, hopGui.getVariables())) {

          session.readTransaction(
              tx -> {
                Result result = tx.run(resultsCypher.toString(), resultsParameters);
                while (result.hasNext()) {
                  Record record = result.next();
                  TableItem item = new TableItem(wResults.table, SWT.NONE);
                  int pos = 0;
                  Value vId = record.get(pos++);
                  item.setText(pos, Const.NVL(vId.asString(), ""));
                  Value vName = record.get(pos++);
                  item.setText(pos, Const.NVL(vName.asString(), ""));
                  Value vType = record.get(pos++);
                  item.setText(pos, Const.NVL(vType.asString(), ""));
                  Value vLinesRead = record.get(pos++);
                  item.setText(pos, Long.toString(vLinesRead.asLong(0)));
                  Value vLinesWritten = record.get(pos++);
                  item.setText(pos, Long.toString(vLinesWritten.asLong(0)));
                  Value vLinesInput = record.get(pos++);
                  item.setText(pos, Long.toString(vLinesInput.asLong(0)));
                  Value vLinesOutput = record.get(pos++);
                  item.setText(pos, Long.toString(vLinesOutput.asLong(0)));
                  Value vLinesRejected = record.get(pos++);
                  item.setText(pos, Long.toString(vLinesRejected.asLong(0)));
                  Value vErrors = record.get(pos++);
                  long errors = vErrors.asLong(0);
                  item.setText(pos, Long.toString(vErrors.asLong(0)));
                  Value vExecutionStart = record.get(pos++);
                  item.setText(pos, Const.NVL(vExecutionStart.asString(), "").replace("T", " "));
                  Value vDurationMs = record.get(pos++);
                  String durationHMS =
                      LoggingCore.getFancyDurationFromMs(Long.valueOf(vDurationMs.asLong(0)));
                  item.setText(pos, durationHMS);

                  if (errors != 0) {
                    item.setBackground(errorLineBackground);
                  }
                }

                wResults.removeEmptyRows();
                wResults.setRowNums();
                wResults.optWidth(true);

                return null;
              });

          // Also populate the executions combo box for pipelines and workflows
          //
          String execCypher =
              "match(e:Execution) where e.type in ['PIPELINE', 'WORKFLOW'] return distinct e.name order by e.name";
          session.readTransaction(
              tx -> {
                List<String> list = new ArrayList<>();
                Result result = tx.run(execCypher);
                while (result.hasNext()) {
                  Record record = result.next();
                  Value value = record.get(0);
                  list.add(value.asString());
                }
                wExecutions.setItems(list.toArray(new String[0]));
                return null;
              });
        }
      } finally {
        wExecutions.setText(Const.NVL(searchName, ""));
      }
    } catch (Throwable e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ErrorSearching.Dialog.Header"),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.ErrorSearching.Dialog.Message"),
          e);
    }
  }

  private void openItem(Event event) {
    if (event.item == null || !(event.item instanceof TreeItem)) {
      return;
    }
    TreeItem item = (TreeItem) event.item;
    openItem(item);
  }

  private void openItem(TreeItem item) {
    try {
      NeoConnection connection = findLoggingConnection();
      if (connection == null) {
        return;
      }

      String id = item.getText(1);
      String name = item.getText(2);
      String type = item.getText(3);

      openItem(connection, id, name, type);

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningItem.Dialog.Header"),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningItem.Dialog.Message"),
          e);
    }
  }

  private void openItem(NeoConnection connection, String id, String name, String type) throws HopConfigException {

    try (Driver driver = connection.getDriver(hopGui.getLog(), hopGui.getVariables())) {
      try (Session session =
          connection.getSession(hopGui.getLog(), driver, hopGui.getVariables())) {

        if ("PIPELINE".equals(type)) {
          openPipelineOrWorkflow(session, name, type, id, "Pipeline", "EXECUTION_OF_PIPELINE");
        } else if ("WORKFLOW".equals(type)) {
          openPipelineOrWorkflow(session, name, type, id, "Workflow", "EXECUTION_OF_WORKFLOW");
        } else if ("TRANSFORM".equals(type)) {
          openTransform(session, name, type, id);
        } else if ("ACTION".equals(type)) {
          openAction(session, name, type, id);
        }
      }
    }
  }

  private void openTransform(Session session, String name, String type, String id) {

    LogChannel.UI.logDetailed("Open transform : " + id + ", name : " + name + ", type: " + type);

    Map<String, Object> params = new HashMap<>();
    params.put("subjectName", name);
    params.put("subjectType", type);
    params.put("subjectId", id);

    StringBuilder cypher = new StringBuilder();
    cypher.append(
        "MATCH(e:Execution { name : $subjectName, type : $subjectType, id : $subjectId } )"); // TRANSFORM
    cypher.append(
        "-[:EXECUTION_OF_TRANSFORM]->(t:Transform { name : $subjectName } )"); // Transform
    cypher.append("-[:TRANSFORM_OF_PIPELINE]->(p:Pipeline) ");
    cypher.append("RETURN p.filename, t.name ");

    String[] names =
        session.readTransaction(
            tx -> {
              Result statementResult = tx.run(cypher.toString(), params);
              if (!statementResult.hasNext()) {
                statementResult.consume();
                return null; // No file found
              }
              Record record = statementResult.next();
              statementResult.consume();

              String filename = LoggingCore.getStringValue(record, 0);
              String transformName = LoggingCore.getStringValue(record, 1);

              return new String[] {filename, transformName};
            });

    if (names == null) {
      return;
    }

    String filename = names[0];
    String transformName = names[1];

    if (StringUtils.isEmpty(filename)) {
      return;
    }

    try {
      hopGui.fileDelegate.fileOpen(filename);

      if (StringUtils.isEmpty(transformName)) {
        return;
      }

      HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
      IHopFileTypeHandler typeHandler = perspective.getActiveFileTypeHandler();
      if (typeHandler == null || !(typeHandler instanceof HopGuiPipelineGraph)) {
        return;
      }
      HopGuiPipelineGraph graph = (HopGuiPipelineGraph) typeHandler;
      PipelineMeta pipelineMeta = graph.getPipelineMeta();
      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
      if (transformMeta == null) {
        return;
      }
      pipelineMeta.unselectAll();
      transformMeta.setSelected(true);
      graph.editTransform(pipelineMeta, transformMeta);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningTransform.Dialog.Header"),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningTransform.Dialog.Message"),
          e);
    }
  }

  private void openAction(Session session, String name, String type, String id) {

    LogChannel.UI.logDetailed("Open action : " + id + ", name : " + name + ", type: " + type);

    Map<String, Object> params = new HashMap<>();
    params.put("subjectName", name);
    params.put("subjectType", type);
    params.put("subjectId", id);

    StringBuilder cypher = new StringBuilder();
    cypher.append(
        "MATCH(e:Execution { name : $subjectName, type : $subjectType, id : $subjectId } )"); // ACTION
    cypher.append("-[:EXECUTION_OF_ACTION]->(a:Action { name : $subjectName } )"); // Action
    cypher.append("-[:ACTION_OF_WORKFLOW]->(w:Workflow) "); // Workflow
    cypher.append("RETURN w.filename, a.name ");

    String[] names =
        session.readTransaction(
            tx -> {
              Result statementResult = tx.run(cypher.toString(), params);
              if (!statementResult.hasNext()) {
                statementResult.consume();
                return null; // No file found
              }
              Record record = statementResult.next();
              statementResult.consume();

              return new String[] {
                LoggingCore.getStringValue(record, 0), // filename
                LoggingCore.getStringValue(record, 1) // action name
              };
            });
    if (names == null) {
      return;
    }

    String filename = names[0];
    String actionName = names[1];

    if (StringUtils.isEmpty(filename)) {
      return;
    }

    try {
      hopGui.fileDelegate.fileOpen(filename);
      if (StringUtils.isNotEmpty(actionName)) {
        IHopFileTypeHandler typeHandler =
            HopGui.getDataOrchestrationPerspective().getActiveFileTypeHandler();
        if (typeHandler == null || !(typeHandler instanceof HopGuiWorkflowGraph)) {
          return;
        }
        HopGuiWorkflowGraph graph = (HopGuiWorkflowGraph) typeHandler;
        WorkflowMeta workflowMeta = graph.getWorkflowMeta();
        ActionMeta actionMeta = workflowMeta.findAction(actionName);
        if (actionMeta != null) {
          workflowMeta.unselectAll();
          actionMeta.setSelected(true);
          graph.editAction(workflowMeta, actionMeta);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningAction.Dialog.Header"),
          BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningAction.Dialog.Message"),
          e);
    }
  }

  private void openPipelineOrWorkflow(
      Session session, String name, String type, String id, String nodeLabel, String relationship) {
    Map<String, Object> params = new HashMap<>();
    params.put("subjectName", name);
    params.put("subjectType", type);
    params.put("subjectId", id);

    StringBuilder cypher = new StringBuilder();
    cypher.append(
        "MATCH(ex:Execution { name : $subjectName, type : $subjectType, id : $subjectId }) ");
    cypher.append("MATCH(tr:" + nodeLabel + " { name : $subjectName }) ");
    cypher.append("MATCH(ex)-[:" + relationship + "]->(tr) ");
    cypher.append("RETURN tr.filename ");

    String filename =
        session.readTransaction(
            tx -> {
              Result statementResult = tx.run(cypher.toString(), params);
              if (!statementResult.hasNext()) {
                statementResult.consume();
                return null; // No file found
              }
              Record record = statementResult.next();
              statementResult.consume();

              // The filename
              return LoggingCore.getStringValue(record, 0);
            });

    if (StringUtils.isNotEmpty(filename)) {
      try {
        hopGui.fileDelegate.fileOpen(filename);
      } catch (Exception e) {
        new ErrorDialog(
            hopGui.getShell(),
            BaseMessages.getString(PKG, "Neo4jPerspectiveDialog.OpeningFile.Dialog.Header"),
            BaseMessages.getString(
                PKG, "Neo4jPerspectiveDialog.OpeningFile.Dialog.Message", filename),
            e);
      }
    }
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    return false; // Nothing to do here
  }

  @Override
  public List<TabItemHandler> getItems() {
    return null;
  }

  @Override
  public void navigateToPreviousFile() {}

  @Override
  public void navigateToNextFile() {}

  @Override
  public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override
  public boolean hasNavigationNextFile() {
    return false;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /** @param hopGui The hopGui to set */
  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  /**
   * Gets parent
   *
   * @return value of parent
   */
  public Composite getParent() {
    return parent;
  }

  /** @param parent The parent to set */
  public void setParent(Composite parent) {
    this.parent = parent;
  }

  /**
   * Gets composite
   *
   * @return value of composite
   */
  @Override
  public Composite getControl() {
    return composite;
  }

  /** @param composite The composite to set */
  public void setComposite(Composite composite) {
    this.composite = composite;
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
