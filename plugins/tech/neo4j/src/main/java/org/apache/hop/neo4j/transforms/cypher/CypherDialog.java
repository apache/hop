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

package org.apache.hop.neo4j.transforms.cypher;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.transforms.output.Neo4JOutputDialog;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Layout;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CypherDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = CypherMeta.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabFolder;

  private Text wTransformName;

  private int middle;
  private int margin;

  private MetaSelectionLine<NeoConnection> wConnection;
  private TextVar wBatchSize;
  private Button wReadOnly;
  private Button wRetryOnDisconnect;
  private TextVar wNrRetriesOnError;
  private Button wCypherFromField;
  private CCombo wCypherField;
  private Button wUnwind;
  private Label wlUnwindMap;
  private TextVar wUnwindMap;
  private Button wReturnGraph;
  private Label wlReturnGraphField;
  private TextVar wReturnGraphField;

  private Text wCypher;

  private TableView wParameters;

  private TableView wReturns;

  private CypherMeta input;

  public CypherDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, pipelineMeta, transformName);
    input = (CypherMeta) inputMetadata;

    metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    shell.setLayout(createFormLayout());
    shell.setText(BaseMessages.getString(PKG, "Cypher.Transform.Name"));

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // Buttons go at the bottom...
    //
    // Some buttons
    // Position the buttons at the bottom of the dialog.
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, 0);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addOptionsTab();
    addParametersTab();
    addCypherTab();
    addReturnsTab();

    wTabFolder.setLayoutData(
        new FormDataBuilder()
            .left()
            .top(new FormAttachment(wTransformName, margin))
            .right()
            .bottom(new FormAttachment(wOk, -2 * margin))
            .result());

    getData();

    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addOptionsTab() {
    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setText(BaseMessages.getString(PKG, "CypherDialog.Tab.Options.Label"));
    wOptionsTab.setToolTipText(BaseMessages.getString(PKG, "CypherDialog.Tab.Options.ToolTip"));
    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOptionsComp);
    wOptionsComp.setLayout(createFormLayout());

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            NeoConnection.class,
            wOptionsComp,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Neo4j Connection",
            "The name of the Neo4j connection to use");
    props.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(0, 0);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }
    Control lastControl = wConnection;

    Label wlBatchSize = new Label(wOptionsComp, SWT.RIGHT);
    wlBatchSize.setText("Batch size (rows)");
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchSize);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    Label wlReadOnly = new Label(wOptionsComp, SWT.RIGHT);
    wlReadOnly.setText("Read only statement? ");
    props.setLook(wlReadOnly);
    FormData fdlReadOnly = new FormData();
    fdlReadOnly.left = new FormAttachment(0, 0);
    fdlReadOnly.right = new FormAttachment(middle, -margin);
    fdlReadOnly.top = new FormAttachment(lastControl, 2 * margin);
    wlReadOnly.setLayoutData(fdlReadOnly);
    wReadOnly = new Button(wOptionsComp, SWT.CHECK | SWT.BORDER);
    props.setLook(wReadOnly);
    FormData fdReadOnly = new FormData();
    fdReadOnly.left = new FormAttachment(middle, 0);
    fdReadOnly.right = new FormAttachment(100, 0);
    fdReadOnly.top = new FormAttachment(wlReadOnly, 0, SWT.CENTER);
    wReadOnly.setLayoutData(fdReadOnly);
    lastControl = wReadOnly;

    Label wlRetry = new Label(wOptionsComp, SWT.RIGHT);
    wlRetry.setText("Reconnect once after disconnection? ");
    props.setLook(wlRetry);
    FormData fdlRetry = new FormData();
    fdlRetry.left = new FormAttachment(0, 0);
    fdlRetry.right = new FormAttachment(middle, -margin);
    fdlRetry.top = new FormAttachment(lastControl, 2 * margin);
    wlRetry.setLayoutData(fdlRetry);
    wRetryOnDisconnect = new Button(wOptionsComp, SWT.CHECK | SWT.BORDER);
    props.setLook(wRetryOnDisconnect);
    FormData fdRetry = new FormData();
    fdRetry.left = new FormAttachment(middle, 0);
    fdRetry.right = new FormAttachment(100, 0);
    fdRetry.top = new FormAttachment(wlRetry, 0, SWT.CENTER);
    wRetryOnDisconnect.setLayoutData(fdRetry);
    lastControl = wRetryOnDisconnect;

    Label wlNrRetriesOnError = new Label(wOptionsComp, SWT.RIGHT);
    wlNrRetriesOnError.setText("Number of retries on error");
    props.setLook(wlNrRetriesOnError);
    FormData fdlNrRetriesOnError = new FormData();
    fdlNrRetriesOnError.left = new FormAttachment(0, 0);
    fdlNrRetriesOnError.right = new FormAttachment(middle, -margin);
    fdlNrRetriesOnError.top = new FormAttachment(lastControl, 2 * margin);
    wlNrRetriesOnError.setLayoutData(fdlNrRetriesOnError);
    wNrRetriesOnError = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wNrRetriesOnError);
    FormData fdNrRetriesOnError = new FormData();
    fdNrRetriesOnError.left = new FormAttachment(middle, 0);
    fdNrRetriesOnError.right = new FormAttachment(100, 0);
    fdNrRetriesOnError.top = new FormAttachment(wlNrRetriesOnError, 0, SWT.CENTER);
    wNrRetriesOnError.setLayoutData(fdNrRetriesOnError);
    lastControl = wNrRetriesOnError;

    Label wlCypherFromField = new Label(wOptionsComp, SWT.RIGHT);
    wlCypherFromField.setText("Get Cypher from input field? ");
    props.setLook(wlCypherFromField);
    FormData fdlCypherFromField = new FormData();
    fdlCypherFromField.left = new FormAttachment(0, 0);
    fdlCypherFromField.right = new FormAttachment(middle, -margin);
    fdlCypherFromField.top = new FormAttachment(lastControl, 2 * margin);
    wlCypherFromField.setLayoutData(fdlCypherFromField);
    wCypherFromField = new Button(wOptionsComp, SWT.CHECK | SWT.BORDER);
    props.setLook(wCypherFromField);
    FormData fdCypherFromField = new FormData();
    fdCypherFromField.left = new FormAttachment(middle, 0);
    fdCypherFromField.right = new FormAttachment(100, 0);
    fdCypherFromField.top = new FormAttachment(wlCypherFromField, 0, SWT.CENTER);
    wCypherFromField.setLayoutData(fdCypherFromField);
    lastControl = wCypherFromField;

    wCypherFromField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            enableFields();
          }
        });

    Label wlCypherField = new Label(wOptionsComp, SWT.RIGHT);
    wlCypherField.setText("Cypher input field");
    props.setLook(wlCypherField);
    FormData fdlCypherField = new FormData();
    fdlCypherField.left = new FormAttachment(0, 0);
    fdlCypherField.right = new FormAttachment(middle, -margin);
    fdlCypherField.top = new FormAttachment(lastControl, 2 * margin);
    wlCypherField.setLayoutData(fdlCypherField);
    wCypherField = new CCombo(wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wCypherField);
    FormData fdCypherField = new FormData();
    fdCypherField.left = new FormAttachment(middle, 0);
    fdCypherField.right = new FormAttachment(100, 0);
    fdCypherField.top = new FormAttachment(wlCypherField, 0, SWT.CENTER);
    wCypherField.setLayoutData(fdCypherField);
    lastControl = wCypherField;

    Label wlUnwind = new Label(wOptionsComp, SWT.RIGHT);
    wlUnwind.setText("Collect parameter values map?");
    wlUnwind.setToolTipText(
        "Collect the specified parameters field data and expose it into a single variable to support UNWIND statements");
    props.setLook(wlUnwind);
    FormData fdlUnwind = new FormData();
    fdlUnwind.left = new FormAttachment(0, 0);
    fdlUnwind.right = new FormAttachment(middle, -margin);
    fdlUnwind.top = new FormAttachment(lastControl, 2 * margin);
    wlUnwind.setLayoutData(fdlUnwind);
    wUnwind = new Button(wOptionsComp, SWT.CHECK | SWT.BORDER);
    props.setLook(wUnwind);
    FormData fdUnwind = new FormData();
    fdUnwind.left = new FormAttachment(middle, 0);
    fdUnwind.right = new FormAttachment(100, 0);
    fdUnwind.top = new FormAttachment(wlUnwind, 0, SWT.CENTER);
    wUnwind.setLayoutData(fdUnwind);
    lastControl = wUnwind;
    wUnwind.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            enableFields();
          }
        });

    wlUnwindMap = new Label(wOptionsComp, SWT.RIGHT);
    wlUnwindMap.setText("Name of values map list");
    wlUnwindMap.setToolTipText(
        "You can use this parameter in your Cypher usually in UNWIND statements");
    props.setLook(wlUnwindMap);
    FormData fdlUnwindMap = new FormData();
    fdlUnwindMap.left = new FormAttachment(0, 0);
    fdlUnwindMap.right = new FormAttachment(middle, -margin);
    fdlUnwindMap.top = new FormAttachment(lastControl, 2 * margin);
    wlUnwindMap.setLayoutData(fdlUnwindMap);
    wUnwindMap = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUnwindMap);
    FormData fdUnwindMap = new FormData();
    fdUnwindMap.left = new FormAttachment(middle, 0);
    fdUnwindMap.right = new FormAttachment(100, 0);
    fdUnwindMap.top = new FormAttachment(wlUnwindMap, 0, SWT.CENTER);
    wUnwindMap.setLayoutData(fdUnwindMap);
    lastControl = wUnwindMap;

    Label wlReturnGraph = new Label(wOptionsComp, SWT.RIGHT);
    wlReturnGraph.setText("Return graph data?");
    String returnGraphTooltipText = "Returns the whole result of a query as a Graph Hop data type";
    wlReturnGraph.setToolTipText(returnGraphTooltipText);
    props.setLook(wlReturnGraph);
    FormData fdlReturnGraph = new FormData();
    fdlReturnGraph.left = new FormAttachment(0, 0);
    fdlReturnGraph.right = new FormAttachment(middle, -margin);
    fdlReturnGraph.top = new FormAttachment(lastControl, 2 * margin);
    wlReturnGraph.setLayoutData(fdlReturnGraph);
    wReturnGraph = new Button(wOptionsComp, SWT.CHECK | SWT.BORDER);
    wReturnGraph.setToolTipText(returnGraphTooltipText);
    props.setLook(wReturnGraph);
    FormData fdReturnGraph = new FormData();
    fdReturnGraph.left = new FormAttachment(middle, 0);
    fdReturnGraph.right = new FormAttachment(100, 0);
    fdReturnGraph.top = new FormAttachment(wlReturnGraph, 0, SWT.CENTER);
    wReturnGraph.setLayoutData(fdReturnGraph);
    lastControl = wReturnGraph;
    wReturnGraph.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            enableFields();
          }
        });

    wlReturnGraphField = new Label(wOptionsComp, SWT.RIGHT);
    wlReturnGraphField.setText("Graph output field name");
    props.setLook(wlReturnGraphField);
    FormData fdlReturnGraphField = new FormData();
    fdlReturnGraphField.left = new FormAttachment(0, 0);
    fdlReturnGraphField.right = new FormAttachment(middle, -margin);
    fdlReturnGraphField.top = new FormAttachment(lastControl, 2 * margin);
    wlReturnGraphField.setLayoutData(fdlReturnGraphField);
    wReturnGraphField = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wReturnGraphField);
    FormData fdReturnGraphField = new FormData();
    fdReturnGraphField.left = new FormAttachment(middle, 0);
    fdReturnGraphField.right = new FormAttachment(100, 0);
    fdReturnGraphField.top = new FormAttachment(wlReturnGraphField, 0, SWT.CENTER);
    wReturnGraphField.setLayoutData(fdReturnGraphField);
    // lastControl = wReturnGraphField;

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void addParametersTab() {
    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setText(BaseMessages.getString(PKG, "CypherDialog.Tab.Parameters.Label"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "CypherDialog.Tab.Parameters.ToolTip"));
    Composite wParametersComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wParametersComp);
    wParametersComp.setLayout(createFormLayout());

    // Get the input field names...
    //
    String[] fieldNames;
    try {
      fieldNames = pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
    } catch (Exception e) {
      logError("Unable to get fields from previous transform", e);
      fieldNames = new String[] {};
    }

    // Table: parameter and field
    //
    ColumnInfo[] parameterColumns =
        new ColumnInfo[] {
          new ColumnInfo("Parameter", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false),
          new ColumnInfo(
              "Neo4j Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false),
        };

    Label wlParameters = new Label(wParametersComp, SWT.LEFT);
    wlParameters.setText("Parameters: (NOTE that parameters for labels are not supported)");
    props.setLook(wlParameters);
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment(0, 0);
    fdlParameters.right = new FormAttachment(100, 0);
    fdlParameters.top = new FormAttachment(0, 0);
    wlParameters.setLayoutData(fdlParameters);

    Button wbGetParameters = new Button(wParametersComp, SWT.PUSH);
    wbGetParameters.setText("Get parameters");
    FormData fdbGetParameters = new FormData();
    fdbGetParameters.right = new FormAttachment(100, 0);
    fdbGetParameters.top = new FormAttachment(wlParameters, 0, SWT.BOTTOM);
    wbGetParameters.setLayoutData(fdbGetParameters);
    wbGetParameters.addListener(
        SWT.Selection,
        (e) -> {
          try {
            IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformMeta);

            BaseTransformDialog.getFieldsFromPrevious(
                r,
                wParameters,
                2,
                new int[] {2},
                new int[] {},
                -1,
                -1,
                (item, valueMeta) ->
                    Neo4JOutputDialog.getPropertyNameTypePrimary(
                        item, valueMeta, new int[] {1}, new int[] {3}, -1));
          } catch (Exception ex) {
            new ErrorDialog(shell, "Error", "Error getting transform input fields", ex);
          }
        });

    wParameters =
        new TableView(
            variables,
            wParametersComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            parameterColumns,
            input.getParameterMappings().size(),
            null,
            props);
    props.setLook(wParameters);
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment(0, 0);
    fdParameters.right = new FormAttachment(wbGetParameters, -margin);
    fdParameters.top = new FormAttachment(wlParameters, margin);
    fdParameters.bottom = new FormAttachment(100, 0);
    wParameters.setLayoutData(fdParameters);

    wParametersComp.layout();
    wParametersTab.setControl(wParametersComp);
  }

  private void addCypherTab() {
    CTabItem wCypherTab = new CTabItem(wTabFolder, SWT.NONE);
    wCypherTab.setText(BaseMessages.getString(PKG, "CypherDialog.Tab.Cypher.Label"));
    wCypherTab.setToolTipText(BaseMessages.getString(PKG, "CypherDialog.Tab.Cypher.ToolTip"));
    Composite wCypherComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wCypherComp);
    wCypherComp.setLayout(createFormLayout());

    Label wlCypher = new Label(wCypherComp, SWT.LEFT);
    wlCypher.setText("Cypher:");
    props.setLook(wlCypher);
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment(0, 0);
    fdlCypher.right = new FormAttachment(middle, -margin);
    fdlCypher.top = new FormAttachment(0, 0);
    wlCypher.setLayoutData(fdlCypher);
    wCypher =
        new Text(wCypherComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wCypher.setFont(GuiResource.getInstance().getFontFixed());
    props.setLook(wCypher);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(wlCypher, margin);
    fdCypher.bottom = new FormAttachment(100, 0);
    wCypher.setLayoutData(fdCypher);

    wCypherComp.layout();
    wCypherTab.setControl(wCypherComp);
  }

  private void addReturnsTab() {
    CTabItem wReturnsTab = new CTabItem(wTabFolder, SWT.NONE);
    wReturnsTab.setText(BaseMessages.getString(PKG, "CypherDialog.Tab.Returns.Label"));
    wReturnsTab.setToolTipText(BaseMessages.getString(PKG, "CypherDialog.Tab.Returns.ToolTip"));
    Composite wReturnsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wReturnsComp);
    wReturnsComp.setLayout(createFormLayout());

    // Table: return field name and type
    //
    ColumnInfo[] returnColumns =
        new ColumnInfo[] {
          new ColumnInfo("Field name", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              "Return type",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getAllValueMetaNames(),
              false),
          new ColumnInfo(
              "Source type",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphPropertyDataType.getNames(),
              false),
        };

    Label wlReturns = new Label(wReturnsComp, SWT.LEFT);
    wlReturns.setText("Returns");
    props.setLook(wlReturns);
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment(0, 0);
    fdlReturns.right = new FormAttachment(middle, -margin);
    fdlReturns.top = new FormAttachment(0, 0);
    wlReturns.setLayoutData(fdlReturns);

    Button wbGetReturnFields = new Button(wReturnsComp, SWT.PUSH);
    wbGetReturnFields.setText("Get Output Fields");
    FormData fdbGetReturnFields = new FormData();
    fdbGetReturnFields.right = new FormAttachment(100, 0);
    fdbGetReturnFields.top = new FormAttachment(wlReturns, 0, SWT.BOTTOM);
    wbGetReturnFields.setLayoutData(fdbGetReturnFields);
    wbGetReturnFields.addListener(SWT.Selection, (e) -> getReturnValues());

    wReturns =
        new TableView(
            variables,
            wReturnsComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            returnColumns,
            input.getReturnValues().size(),
            null,
            props);
    props.setLook(wReturns);
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment(0, 0);
    fdReturns.right = new FormAttachment(wbGetReturnFields, 0);
    fdReturns.top = new FormAttachment(wlReturns, margin);
    fdReturns.bottom = new FormAttachment(100, 0);
    wReturns.setLayoutData(fdReturns);

    wReturnsComp.layout();
    wReturnsTab.setControl(wReturnsComp);
  }

  private Layout createFormLayout() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = Const.FORM_MARGIN;
    formLayout.marginRight = Const.FORM_MARGIN;
    formLayout.marginTop = Const.FORM_MARGIN;
    formLayout.marginBottom = Const.FORM_MARGIN;
    return formLayout;
  }

  private void enableFields() {
    boolean fromField = wCypherFromField.getSelection();

    wCypher.setEnabled(!fromField);
    wCypherField.setEnabled(fromField);

    boolean usingUnwind = wUnwind.getSelection();

    wlUnwindMap.setEnabled(usingUnwind);
    wUnwindMap.setEnabled(usingUnwind);

    boolean returningGraph = wReturnGraph.getSelection();

    wlReturnGraphField.setEnabled(returningGraph);
    wReturnGraphField.setEnabled(returningGraph);
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  public void getData() {

    wTransformName.setText(Const.NVL(transformName, ""));
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));

    wReadOnly.setSelection(input.isReadOnly());
    wRetryOnDisconnect.setSelection(input.isRetryingOnDisconnect());
    wNrRetriesOnError.setText(Const.NVL(input.getNrRetriesOnError(), ""));
    wCypherFromField.setSelection(input.isCypherFromField());
    wCypherField.setText(Const.NVL(input.getCypherField(), ""));
    try {
      wCypherField.setItems(
          pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames());
    } catch (HopTransformException e) {
      log.logError("Error getting fields from previous transform", e);
    }

    wUnwind.setSelection(input.isUsingUnwind());
    wUnwindMap.setText(Const.NVL(input.getUnwindMapName(), ""));
    wReturnGraph.setSelection(input.isReturningGraph());
    wReturnGraphField.setText(Const.NVL(input.getReturnGraphField(), ""));
    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wCypher.setText(Const.NVL(input.getCypher(), ""));

    for (int i = 0; i < input.getParameterMappings().size(); i++) {
      ParameterMapping mapping = input.getParameterMappings().get(i);
      TableItem item = wParameters.table.getItem(i);
      item.setText(1, Const.NVL(mapping.getParameter(), ""));
      item.setText(2, Const.NVL(mapping.getField(), ""));
      item.setText(3, Const.NVL(mapping.getNeoType(), ""));
    }
    wParameters.removeEmptyRows();
    wParameters.setRowNums();
    wParameters.optWidth(true);

    for (int i = 0; i < input.getReturnValues().size(); i++) {
      ReturnValue returnValue = input.getReturnValues().get(i);
      TableItem item = wReturns.table.getItem(i);
      item.setText(1, Const.NVL(returnValue.getName(), ""));
      item.setText(2, Const.NVL(returnValue.getType(), ""));
      item.setText(3, Const.NVL(returnValue.getSourceType(), ""));
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth(true);

    enableFields();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);
    input.setChanged();
    dispose();
  }

  private void getInfo(CypherMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setBatchSize(wBatchSize.getText());
    meta.setCypher(wCypher.getText());

    meta.setReadOnly(wReadOnly.getSelection());
    meta.setRetryingOnDisconnect(wRetryOnDisconnect.getSelection());
    meta.setNrRetriesOnError(wNrRetriesOnError.getText());
    meta.setCypherFromField(wCypherFromField.getSelection());
    meta.setCypherField(wCypherField.getText());

    meta.setUsingUnwind(wUnwind.getSelection());
    meta.setUnwindMapName(wUnwindMap.getText());

    meta.setReturningGraph(wReturnGraph.getSelection());
    meta.setReturnGraphField(wReturnGraphField.getText());

    List<ParameterMapping> mappings = new ArrayList<>();
    for (int i = 0; i < wParameters.nrNonEmpty(); i++) {
      TableItem item = wParameters.getNonEmpty(i);
      mappings.add(new ParameterMapping(item.getText(1), item.getText(2), item.getText(3)));
    }
    meta.setParameterMappings(mappings);

    List<ReturnValue> returnValues = new ArrayList<>();
    for (int i = 0; i < wReturns.nrNonEmpty(); i++) {
      TableItem item = wReturns.getNonEmpty(i);
      returnValues.add(new ReturnValue(item.getText(1), item.getText(2), item.getText(3)));
    }
    meta.setReturnValues(returnValues);
  }

  private synchronized void preview() {
    CypherMeta oneMeta = new CypherMeta();
    this.getInfo(oneMeta);
    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            HopGui.getInstance().getMetadataProvider(), oneMeta, this.wTransformName.getText());
    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            this.shell,
            this.props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "CypherDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "CypherDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              this.shell,
              variables,
              previewMeta,
              new String[] {this.wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();
      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();
      if (!progressDialog.isCancelled()
          && pipeline.getResult() != null
          && pipeline.getResult().getNrErrors() > 0L) {
        EnterTextDialog etd =
            new EnterTextDialog(
                this.shell,
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title", new String[0]),
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message", new String[0]),
                loggingText,
                true);
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              this.shell,
              variables,
              0,
              this.wTransformName.getText(),
              progressDialog.getPreviewRowsMeta(this.wTransformName.getText()),
              progressDialog.getPreviewRows(this.wTransformName.getText()),
              loggingText);
      prd.open();
    }
  }

  private void getReturnValues() {

    IHopMetadataProvider metadataProvider = HopGui.getInstance().getMetadataProvider();
    Driver driver = null;
    Session session = null;
    Transaction transaction = null;

    CypherMeta meta = new CypherMeta();
    getInfo(meta);

    try {
      NeoConnection neoConnection =
          metadataProvider.getSerializer(NeoConnection.class).load(meta.getConnectionName());
      driver = neoConnection.getDriver(log, variables);
      session = driver.session();
      transaction = session.beginTransaction();
      Map<String, Object> parameters = new HashMap<>();
      for (ParameterMapping mapping : meta.getParameterMappings()) {
        parameters.put(variables.resolve(mapping.getParameter()), "");
      }
      String cypher = variables.resolve(wCypher.getText());
      Result result = transaction.run(cypher, parameters);

      // Evaluate the result
      //
      if (result.hasNext()) {
        Record record = result.next();
        List<Pair<String, Value>> fields = record.fields();
        for (Pair<String, Value> fieldPair : fields) {
          String returnField = fieldPair.key();
          Value returnValue = fieldPair.value();
          Type valueType = returnValue.type();

          String typeName = valueType.name().replaceAll("_", "");
          GraphPropertyDataType type = GraphPropertyDataType.parseCode(typeName);
          int kettleType = type.getHopType();

          TableItem item = new TableItem(wReturns.table, SWT.NONE);
          item.setText(1, returnField);
          item.setText(2, ValueMetaFactory.getValueMetaName(kettleType));
          item.setText(3, type.name());
        }
      }
      wReturns.removeEmptyRows();
      wReturns.setRowNums();
      wReturns.optWidth(true);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting return values from Cypher statement", e);
    } finally {
      if (transaction != null) {
        transaction.rollback();
        transaction.close();
      }
      if (session != null) {
        session.close();
      }
      if (driver != null) {
        driver.close();
      }
    }
  }
}
