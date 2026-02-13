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

package org.apache.hop.neo4j.transforms.cypherbuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.BaseOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.CreateOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.DeleteOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.EdgeMatchOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.IOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.MatchOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.MergeOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.OperationFactory;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.OperationType;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.OrderByOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.ReturnOperation;
import org.apache.hop.neo4j.transforms.cypherbuilder.operation.SetOperation;
import org.apache.hop.neo4j.transforms.output.Neo4JOutputDialog;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Layout;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

public class CypherBuilderDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      CypherBuilderMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String STRING_EXPERIMENTAL_WARNING = "CypherBuilderWarning";
  public static final String CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_LABEL =
      "CypherBuilderDialog.Operation.NodeAlias.Label";
  public static final String CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_TOOLTIP =
      "CypherBuilderDialog.Operation.NodeAlias.Tooltip";
  public static final String CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_NAME =
      "CypherBuilderDialog.Operation.Properties.Column.Name";
  public static final String CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_EXPRESSION =
      "CypherBuilderDialog.Operation.Properties.Column.Expression";

  private CTabFolder wTabFolder;

  // The options tab
  //
  private MetaSelectionLine<NeoConnection> wConnection;
  private TextVar wBatchSize;
  private TextVar wUnwindAlias;
  private TextVar wRetries;

  // The parameters tab
  private TableView wParameters;

  // The operations tab
  private org.eclipse.swt.widgets.List wOperationsList;
  private Composite wOperationComp;

  // The cypher tab
  private Text wCypher;

  private CypherBuilderMeta input;

  private CypherBuilderMeta copy;

  private boolean warningShown;

  public CypherBuilderDialog(
      Shell parent,
      IVariables variables,
      CypherBuilderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;

    this.copy = input.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CypherBuilder.Transform.Name"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addOptionsTab();
    addParametersTab();
    addOperationsTab();
    addCypherTab();

    wTabFolder.setLayoutData(
        new FormDataBuilder()
            .left()
            .top(new FormAttachment(wSpacer, margin))
            .right()
            .bottom(new FormAttachment(100, -50))
            .result());

    getData();

    // When the shell activates, show a warning
    shell.addListener(SWT.Activate, e -> showExperimentalWarning());

    wTabFolder.setSelection(0);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addOptionsTab() {
    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOptionsTab.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Options.Label"));
    wOptionsTab.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Options.ToolTip"));
    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);
    wOptionsComp.setLayout(new FormLayout());

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            NeoConnection.class,
            wOptionsComp,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Neo4j Connection",
            "The name of the Neo4j connection to use");
    PropsUi.setLook(wConnection);
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
    PropsUi.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBatchSize);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    wBatchSize.addListener(SWT.Modify, e -> copy.setBatchSize(wBatchSize.getText()));
    lastControl = wBatchSize;

    Label wlUnwindAlias = new Label(wOptionsComp, SWT.RIGHT);
    wlUnwindAlias.setText("Unwind map alias");
    wlUnwindAlias.setToolTipText(
        "Set this to enable UNWIND style cypher building.  The map is called $rows so you can call this row");
    PropsUi.setLook(wlUnwindAlias);
    FormData fdlUnwindAlias = new FormData();
    fdlUnwindAlias.left = new FormAttachment(0, 0);
    fdlUnwindAlias.right = new FormAttachment(middle, -margin);
    fdlUnwindAlias.top = new FormAttachment(lastControl, margin);
    wlUnwindAlias.setLayoutData(fdlUnwindAlias);
    wUnwindAlias = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUnwindAlias);
    FormData fdUnwindAlias = new FormData();
    fdUnwindAlias.left = new FormAttachment(middle, 0);
    fdUnwindAlias.right = new FormAttachment(100, 0);
    fdUnwindAlias.top = new FormAttachment(wlUnwindAlias, 0, SWT.CENTER);
    wUnwindAlias.setLayoutData(fdUnwindAlias);
    wUnwindAlias.addListener(SWT.Modify, e -> copy.setUnwindAlias(wUnwindAlias.getText()));
    lastControl = wUnwindAlias;

    Label wlRetries = new Label(wOptionsComp, SWT.RIGHT);
    wlRetries.setText("Maximum retries");
    wlRetries.setToolTipText(
        "This is the maximum number of times a transaction will be re-tried on the database before giving up.");
    PropsUi.setLook(wlRetries);
    FormData fdlRetries = new FormData();
    fdlRetries.left = new FormAttachment(0, 0);
    fdlRetries.right = new FormAttachment(middle, -margin);
    fdlRetries.top = new FormAttachment(lastControl, margin);
    wlRetries.setLayoutData(fdlRetries);
    wRetries = new TextVar(variables, wOptionsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRetries);
    FormData fdRetries = new FormData();
    fdRetries.left = new FormAttachment(middle, 0);
    fdRetries.right = new FormAttachment(100, 0);
    fdRetries.top = new FormAttachment(wlRetries, 0, SWT.CENTER);
    wRetries.setLayoutData(fdRetries);
    wRetries.addListener(SWT.Modify, e -> copy.setRetries(wRetries.getText()));

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void addParametersTab() {
    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setFont(GuiResource.getInstance().getFontDefault());
    wParametersTab.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Parameters.Label"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Parameters.ToolTip"));
    Composite wParametersComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wParametersComp);
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
          new ColumnInfo("Input field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false),
          new ColumnInfo(
              "Neo4j Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false),
        };

    Label wlParameters = new Label(wParametersComp, SWT.LEFT);
    wlParameters.setText("Parameters: (NOTE that parameters for labels are not supported)");
    PropsUi.setLook(wlParameters);
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
        e -> {
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
            input.getParameters().size(),
            null,
            props);
    PropsUi.setLook(wParameters);
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment(0, 0);
    fdParameters.right = new FormAttachment(wbGetParameters, -margin);
    fdParameters.top = new FormAttachment(wlParameters, margin);
    fdParameters.bottom = new FormAttachment(100, 0);
    wParameters.setLayoutData(fdParameters);

    wParametersComp.layout();
    wParametersTab.setControl(wParametersComp);
  }

  private void addOperationsTab() {
    CTabItem wOperationsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOperationsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOperationsTab.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Operations.Label"));
    wOperationsTab.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Operations.ToolTip"));
    SashForm sashForm = new SashForm(wTabFolder, SWT.HORIZONTAL);
    wOperationsTab.setControl(sashForm);

    Composite wOperationsComp = new Composite(sashForm, SWT.NONE);
    PropsUi.setLook(wOperationsComp);
    wOperationsComp.setLayout(createFormLayout());
    FormData fdOperationsComp = new FormData();
    fdOperationsComp.left = new FormAttachment(0, 0);
    fdOperationsComp.top = new FormAttachment(0, 0);
    fdOperationsComp.right = new FormAttachment(100, 0);
    fdOperationsComp.bottom = new FormAttachment(100, 0);
    wOperationsComp.setLayoutData(fdOperationsComp);

    // A list of operations on the left-hand side.
    //
    // A toolbar:
    //
    ToolBar wOperationsBar = new ToolBar(wOperationsComp, SWT.FLAT);
    PropsUi.setLook(wOperationsBar);
    FormData fdOperationsBar = new FormData();
    fdOperationsBar.top = new FormAttachment(0, 0);
    fdOperationsBar.left = new FormAttachment(0, 0);
    fdOperationsBar.right = new FormAttachment(100, 0);
    wOperationsBar.setLayoutData(fdOperationsBar);

    // Add some toolbar items: add, delete, move up, move down
    //
    ToolItem addOperationItem = new ToolItem(wOperationsBar, SWT.PUSH);
    addOperationItem.setImage(GuiResource.getInstance().getImageAdd());
    addOperationItem.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.OperationAdd.Tooltip"));
    addOperationItem.addListener(SWT.Selection, e -> operationAdd());

    ToolItem deleteOperationItem = new ToolItem(wOperationsBar, SWT.PUSH);
    deleteOperationItem.setImage(GuiResource.getInstance().getImageDelete());
    deleteOperationItem.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.OperationDelete.Tooltip"));
    deleteOperationItem.addListener(SWT.Selection, e -> operationDelete());

    ToolItem moveUpOperationItem = new ToolItem(wOperationsBar, SWT.PUSH);
    moveUpOperationItem.setImage(GuiResource.getInstance().getImageUp());
    moveUpOperationItem.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.OperationMoveUp.Tooltip"));
    moveUpOperationItem.addListener(SWT.Selection, e -> operationMoveUp());

    ToolItem moveDownOperationItem = new ToolItem(wOperationsBar, SWT.PUSH);
    moveDownOperationItem.setImage(GuiResource.getInstance().getImageDown());
    moveDownOperationItem.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.OperationMoveDown.Tooltip"));
    moveDownOperationItem.addListener(SWT.Selection, e -> operationMoveDown());

    // Below that toolbar we have the list of operations
    //
    wOperationsList =
        new org.eclipse.swt.widgets.List(wOperationsComp, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wOperationsList);
    FormData fdOperationsList = new FormData();
    fdOperationsList.left = new FormAttachment(0, 0);
    fdOperationsList.right = new FormAttachment(100, 0);
    fdOperationsList.top = new FormAttachment(wOperationsBar, margin);
    fdOperationsList.bottom = new FormAttachment(100, 0);
    wOperationsList.setLayoutData(fdOperationsList);
    wOperationsList.addListener(SWT.Selection, e -> operationEdit());
    wOperationsList.setItems(copy.getOperationNames());

    wOperationComp = new Composite(sashForm, SWT.NONE);
    FormData fdOperationComp = new FormData();
    fdOperationComp.left = new FormAttachment(0, 0);
    fdOperationComp.right = new FormAttachment(100, 0);
    fdOperationComp.top = new FormAttachment(0, 0);
    fdOperationComp.bottom = new FormAttachment(100, 0);
    wOperationComp.setLayoutData(fdOperationComp);
    wOperationComp.setLayout(createFormLayout());
    PropsUi.setLook(wOperationsComp);
    wOperationsComp.setBackground(GuiResource.getInstance().getColorBackground());

    sashForm.setWeights(new int[] {20, 80});
  }

  private void operationEdit() {
    // Which operation is selected.
    //
    int selectionIndex = wOperationsList.getSelectionIndex();
    if (selectionIndex < 0) {
      // There is nothing to edit.
      return;
    }
    String operationName = wOperationsList.getItem(selectionIndex);

    // Find the corresponding operation.
    //
    IOperation operation = copy.findOperation(operationName);
    if (operation == null) {
      return;
    }

    // Remove all widgets from the operation composite.
    //
    removeOperationsWidgets();

    // Re-add all widgets
    //
    switch (operation.getOperationType()) {
      case MATCH:
        addOperationMatch(operation);
        break;
      case MERGE:
        addOperationMerge(operation);
        break;
      case CREATE:
        addOperationCreate(operation);
        break;
      case DELETE:
        addOperationDelete(operation);
        break;
      case EDGE_MATCH, EDGE_MERGE, EDGE_CREATE:
        addOperationEdgeMatch(operation);
        break;
      case ORDER_BY:
        addOperationOrderBy(operation);
        break;
      case RETURN:
        addOperationReturn(operation);
        break;
      case SET:
        addOperationSet(operation);
        break;
    }

    wOperationComp.layout(true, true);
  }

  private void removeOperationsWidgets() {
    for (Control child : wOperationComp.getChildren()) {
      child.dispose();
    }
  }

  private void addOperationMatch(IOperation operation) {
    MatchOperation matchOperation = (MatchOperation) operation;

    // Name, type, labels, node alias, keys
    Control lastControl = addOperationWidgetName(matchOperation);
    lastControl = addOperationWidgetType(matchOperation, lastControl);
    lastControl = addOperationWidgetLabels(matchOperation, lastControl);
    lastControl =
        addOperationWidgetText(
            matchOperation,
            lastControl,
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_LABEL),
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_TOOLTIP),
            matchOperation.getAlias(),
            c -> matchOperation.setAlias(c.getText()));
    addOperationWidgetKeys(matchOperation, lastControl);
  }

  private void addOperationMerge(IOperation operation) {
    MergeOperation mergeOperation = (MergeOperation) operation;

    // Name, type, labels, node alias, keys, properties to set
    //
    Control lastControl = addOperationWidgetName(mergeOperation);
    lastControl = addOperationWidgetType(mergeOperation, lastControl);
    lastControl = addOperationWidgetLabels(mergeOperation, lastControl);
    lastControl =
        addOperationWidgetText(
            mergeOperation,
            lastControl,
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_LABEL),
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_TOOLTIP),
            mergeOperation.getAlias(),
            c -> mergeOperation.setAlias(c.getText()));
    lastControl = addOperationWidgetKeys(mergeOperation, lastControl);
    addOperationWidgetProperties(mergeOperation, lastControl);
  }

  private void addOperationCreate(IOperation operation) {
    CreateOperation createOperation = (CreateOperation) operation;

    // Name, type, labels, node alias, keys, properties to set
    //
    Control lastControl = addOperationWidgetName(createOperation);
    lastControl = addOperationWidgetType(createOperation, lastControl);
    lastControl = addOperationWidgetLabels(createOperation, lastControl);
    lastControl =
        addOperationWidgetText(
            createOperation,
            lastControl,
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_LABEL),
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_TOOLTIP),
            createOperation.getAlias(),
            c -> createOperation.setAlias(c.getText()));
    lastControl = addOperationWidgetKeys(createOperation, lastControl);
    addOperationWidgetProperties(createOperation, lastControl);
  }

  private void addOperationDelete(IOperation operation) {
    DeleteOperation deleteOperation = (DeleteOperation) operation;

    // Name, type, Labels, node alias, keys
    Control lastControl = addOperationWidgetName(deleteOperation);
    lastControl = addOperationWidgetType(deleteOperation, lastControl);
    lastControl = addOperationWidgetDetachDelete(deleteOperation, lastControl);
    lastControl = addOperationWidgetLabels(deleteOperation, lastControl);
    lastControl =
        addOperationWidgetText(
            deleteOperation,
            lastControl,
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_LABEL),
            BaseMessages.getString(PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_NODE_ALIAS_TOOLTIP),
            deleteOperation.getAlias(),
            c -> deleteOperation.setAlias(c.getText()));
    addOperationWidgetKeys(deleteOperation, lastControl);
  }

  private void addOperationEdgeMatch(IOperation operation) {
    EdgeMatchOperation edgeMatchOperation = (EdgeMatchOperation) operation;

    // Name, type, Source alias, edge alias, edge label, target alias
    Control lastControl = addOperationWidgetName(edgeMatchOperation);
    lastControl = addOperationWidgetType(edgeMatchOperation, lastControl);
    lastControl =
        addOperationWidgetText(
            edgeMatchOperation,
            lastControl,
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.SourceNodeAlias.Label"),
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.SourceNodeAlias.Tooltip"),
            edgeMatchOperation.getSourceAlias(),
            c -> edgeMatchOperation.setSourceAlias(c.getText()));
    lastControl =
        addOperationWidgetText(
            edgeMatchOperation,
            lastControl,
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.EdgeAlias.Label"),
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.EdgeAlias.Tooltip"),
            edgeMatchOperation.getEdgeAlias(),
            c -> edgeMatchOperation.setEdgeAlias(c.getText()));
    lastControl =
        addOperationWidgetText(
            edgeMatchOperation,
            lastControl,
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.EdgeLabel.Label"),
            BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.EdgeLabel.Tooltip"),
            edgeMatchOperation.getEdgeLabel(),
            c -> edgeMatchOperation.setEdgeLabel(c.getText()));
    addOperationWidgetText(
        edgeMatchOperation,
        lastControl,
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.TargetNodeAlias.Label"),
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.TargetNodeAlias.Tooltip"),
        edgeMatchOperation.getTargetAlias(),
        c -> edgeMatchOperation.setTargetAlias(c.getText()));
  }

  private void addOperationOrderBy(IOperation operation) {
    OrderByOperation orderByOperation = (OrderByOperation) operation;

    // Name, type, order by properties
    Control lastControl = addOperationWidgetName(orderByOperation);
    lastControl = addOperationWidgetType(orderByOperation, lastControl);
    addOperationWidgetOrderByProperties(orderByOperation, lastControl);
  }

  private void addOperationReturn(IOperation operation) {
    ReturnOperation returnOperation = (ReturnOperation) operation;

    // Name, type, return properties
    Control lastControl = addOperationWidgetName(returnOperation);
    lastControl = addOperationWidgetType(returnOperation, lastControl);
    addOperationWidgetReturnValues(returnOperation, lastControl);
  }

  private void addOperationSet(IOperation operation) {
    SetOperation setOperation = (SetOperation) operation;

    // Name, type, set operations
    Control lastControl = addOperationWidgetName(setOperation);
    lastControl = addOperationWidgetType(setOperation, lastControl);
    addOperationWidgetSetProperties(setOperation, lastControl);
  }

  /** Shows a dialog asking which type of operation to pick */
  private void operationAdd() {
    String[] operationDescriptions = OperationType.getOperationDescriptions();
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            shell,
            operationDescriptions,
            "Add Operation",
            "Select the operation to add:",
            null,
            variables);
    String operationChoice = dialog.open();
    if (operationChoice == null) {
      return;
    }
    OperationType operationType = OperationType.getOperationByDescription(operationChoice);
    if (operationType == null) {
      return;
    }
    IOperation operation = OperationFactory.createOperation(operationType);
    operation.setName(operation.getOperationType().getDescription());
    copy.getOperations().add(operation);
    wOperationsList.add(operation.getName());
    wOperationsList.setSelection(copy.getOperations().size() - 1);
    operationEdit();
  }

  private void operationDelete() {
    int selectionIndex = wOperationsList.getSelectionIndex();
    if (selectionIndex < 0) {
      return;
    }

    // Remove all widgets from the operation composite.
    //
    removeOperationsWidgets();

    // Remove this operation
    //
    copy.getOperations().remove(selectionIndex);
    wOperationsList.remove(selectionIndex);

    // If there's still something selected, edit it.
    // If there's nothing selected, nothing will happen.
    //
    operationEdit();
  }

  private void operationMoveUp() {
    // Do nothing
  }

  private void operationMoveDown() {
    // Do noting
  }

  private Control addOperationWidgetText(
      BaseOperation operation,
      Control lastControl,
      String label,
      String toolTip,
      String value,
      ITextChanged changed) {
    Label wlText = new Label(wOperationComp, SWT.RIGHT);
    wlText.setText(label);
    wlText.setToolTipText(toolTip);
    FormData fdlText = new FormData();
    fdlText.left = new FormAttachment(0, 0);
    fdlText.right = new FormAttachment(middle, 0);
    fdlText.top = new FormAttachment(lastControl, margin);
    wlText.setLayoutData(fdlText);

    Text wText = new Text(wOperationComp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    wText.setText(Const.NVL(value, ""));
    FormData fdText = new FormData();
    fdText.left = new FormAttachment(middle, margin);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(wlText, 0, SWT.CENTER);
    wText.setLayoutData(fdText);
    wText.addListener(SWT.Modify, e -> changed.changed(wText));

    return wText;
  }

  private Control addOperationWidgetType(IOperation operation, Control lastControl) {
    Label wlOperationType = new Label(wOperationComp, SWT.RIGHT);
    wlOperationType.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.NodeOperationType.Label"));
    wlOperationType.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.NodeOperationType.Tooltip"));
    FormData fdlOperationType = new FormData();
    fdlOperationType.top = new FormAttachment(lastControl, margin);
    fdlOperationType.left = new FormAttachment(0, 0);
    fdlOperationType.right = new FormAttachment(middle, 0);
    wlOperationType.setLayoutData(fdlOperationType);

    Text wOperationType = new Text(wOperationComp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    wOperationType.setText(Const.NVL(operation.getOperationType().keyWord(), ""));
    wOperationType.setEditable(false);
    FormData fdOperationType = new FormData();
    fdOperationType.left = new FormAttachment(middle, margin);
    fdOperationType.right = new FormAttachment(100, 0);
    fdOperationType.top = new FormAttachment(wlOperationType, 0, SWT.CENTER);
    wOperationType.setLayoutData(fdOperationType);

    return wOperationType;
  }

  private Control addOperationWidgetDetachDelete(DeleteOperation operation, Control lastControl) {
    Label wlDetachDelete = new Label(wOperationComp, SWT.RIGHT);
    wlDetachDelete.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.DetachDelete.Label"));
    wlDetachDelete.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.DetachDelete.Tooltip"));
    FormData fdlDetachDelete = new FormData();
    fdlDetachDelete.left = new FormAttachment(0, 0);
    fdlDetachDelete.right = new FormAttachment(middle, 0);
    fdlDetachDelete.top = new FormAttachment(0, 0);
    wlDetachDelete.setLayoutData(fdlDetachDelete);

    Button wDetachDelete = new Button(wOperationComp, SWT.CHECK | SWT.LEFT);
    wDetachDelete.setSelection(operation.isDetach());
    FormData fdDetachDelete = new FormData();
    fdDetachDelete.left = new FormAttachment(middle, margin);
    fdDetachDelete.right = new FormAttachment(100, 0);
    fdDetachDelete.top = new FormAttachment(wlDetachDelete, 0, SWT.CENTER);
    wDetachDelete.setLayoutData(fdDetachDelete);
    wDetachDelete.addListener(
        SWT.Selection, e -> operation.setDetach(wDetachDelete.getSelection()));

    return wDetachDelete;
  }

  private Control addOperationWidgetLabels(BaseOperation operation, Control lastControl) {
    Label wlLabels = new Label(wOperationComp, SWT.RIGHT);
    wlLabels.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Labels.Label"));
    wlLabels.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Labels.Tooltip"));
    FormData fdlLabels = new FormData();
    fdlLabels.top = new FormAttachment(lastControl, margin);
    fdlLabels.left = new FormAttachment(0, 0);
    fdlLabels.right = new FormAttachment(middle, 0);
    wlLabels.setLayoutData(fdlLabels);

    // Table view with 1 column, change listeners modifying operation.labels
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Labels.Column.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false)
        };
    TableView wLabels =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getLabels().size(),
            false,
            null,
            props);
    FormData fdLabels = new FormData();
    fdLabels.top = new FormAttachment(lastControl, margin);
    fdLabels.left = new FormAttachment(middle, margin);
    fdLabels.right = new FormAttachment(100, 0);
    fdLabels.bottom = new FormAttachment(lastControl, margin + (int) (100 * props.getZoomFactor()));
    wLabels.setLayoutData(fdLabels);

    for (int i = 0; i < operation.getLabels().size(); i++) {
      TableItem item = wLabels.table.getItem(i);
      item.setText(1, Const.NVL(operation.getLabels().get(i), ""));
    }
    wLabels.optimizeTableView();

    // Keep track of any changes with a listener
    //
    Function<TableItem, String> itemFunction = tableItem -> tableItem.getText(1);
    wLabels.addModifyListener(
        new TableViewModified<>(wLabels, operation.getLabels(), itemFunction));
    return wLabels;
  }

  private Control addOperationWidgetName(IOperation operation) {
    Label wlName = new Label(wOperationComp, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Name.Label"));
    wlName.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Name.Tooltip"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);

    Text wName = new Text(wOperationComp, SWT.SINGLE | SWT.BORDER | SWT.LEFT);
    wName.setText(Const.NVL(operation.getName(), ""));
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    wName.addListener(
        SWT.Modify,
        e -> {
          operation.setName(wName.getText());
          // Also change the selected item in the operations list widget
          wOperationsList.setItem(
              wOperationsList.getSelectionIndex(), Const.NVL(wName.getText(), ""));
        });

    return wlName;
  }

  private Control addOperationWidgetKeys(BaseOperation operation, Control lastControl) {
    Label wlKeys = new Label(wOperationComp, SWT.RIGHT);
    wlKeys.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Keys.Label"));
    wlKeys.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Keys.Tooltip"));
    FormData fdlKeys = new FormData();
    fdlKeys.top = new FormAttachment(lastControl, margin);
    fdlKeys.left = new FormAttachment(0, 0);
    fdlKeys.right = new FormAttachment(middle, 0);
    wlKeys.setLayoutData(fdlKeys);

    // Table view with 2 columns(property name and parameter), change listeners modifying
    // operation.labels
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Keys.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Keys.Column.Parameter"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              false),
        };

    // List of parameters:
    columns[1].setComboValues(copy.getParameterNames());

    TableView wKeys =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getKeys().size(),
            false,
            null,
            props);
    FormData fdKeys = new FormData();
    fdKeys.left = new FormAttachment(middle, margin);
    fdKeys.right = new FormAttachment(100, 0);
    fdKeys.top = new FormAttachment(lastControl, margin);
    fdKeys.bottom = new FormAttachment(lastControl, margin + (int) (150 * props.getZoomFactor()));
    wKeys.setLayoutData(fdKeys);

    for (int i = 0; i < operation.getKeys().size(); i++) {
      TableItem item = wKeys.table.getItem(i);
      Property key = operation.getKeys().get(i);
      item.setText(1, Const.NVL(key.getName(), ""));
      item.setText(2, Const.NVL(key.getParameter(), ""));
    }
    wKeys.optimizeTableView();

    // Keep track of any changes with a listener
    //
    Function<TableItem, Property> itemFunction =
        tableItem -> new Property(operation.getAlias(), tableItem.getText(1), tableItem.getText(2));
    wKeys.addModifyListener(new TableViewModified<>(wKeys, operation.getKeys(), itemFunction));

    return wKeys;
  }

  private Control addOperationWidgetProperties(BaseOperation operation, Control lastControl) {
    Label wlProperties = new Label(wOperationComp, SWT.RIGHT);
    wlProperties.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Properties.Label"));
    wlProperties.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Properties.Tooltip"));
    FormData fdlProperties = new FormData();
    fdlProperties.left = new FormAttachment(0, 0);
    fdlProperties.right = new FormAttachment(middle, 0);
    fdlProperties.top = new FormAttachment(lastControl, margin);
    wlProperties.setLayoutData(fdlProperties);

    // Table view with 2 columns(property name and parameter), change listeners modifying
    // operation.labels
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_NAME),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_EXPRESSION),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.Properties.Column.Parameter"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              false,
              false),
        };

    // List of parameters:
    columns[2].setComboValues(copy.getParameterNames());

    TableView wProperties =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getProperties().size(),
            false,
            null,
            props);
    FormData fdProperties = new FormData();
    fdProperties.left = new FormAttachment(middle, margin);
    fdProperties.right = new FormAttachment(100, 0);
    fdProperties.top = new FormAttachment(lastControl, margin);
    fdProperties.bottom = new FormAttachment(100, 0);
    wProperties.setLayoutData(fdProperties);

    for (int i = 0; i < operation.getProperties().size(); i++) {
      TableItem item = wProperties.table.getItem(i);
      Property property = operation.getProperties().get(i);
      item.setText(1, Const.NVL(property.getName(), ""));
      item.setText(2, Const.NVL(property.getExpression(), ""));
      item.setText(3, Const.NVL(property.getParameter(), ""));
    }
    wProperties.optimizeTableView();

    // Keep track of any changes with a listener
    //
    Function<TableItem, Property> itemFunction =
        item ->
            new Property(
                operation.getAlias(),
                item.getText(1),
                item.getText(2),
                item.getText(3),
                item.getText(4));
    wProperties.addModifyListener(
        new TableViewModified<>(wProperties, operation.getProperties(), itemFunction));

    return wProperties;
  }

  private Control addOperationWidgetOrderByProperties(
      BaseOperation operation, Control lastControl) {
    Label wlProperties = new Label(wOperationComp, SWT.RIGHT);
    wlProperties.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.OrderByProperties.Label"));
    wlProperties.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.OrderByProperties.Tooltip"));
    FormData fdlProperties = new FormData();
    fdlProperties.left = new FormAttachment(0, 0);
    fdlProperties.right = new FormAttachment(middle, 0);
    fdlProperties.top = new FormAttachment(lastControl, margin);
    wlProperties.setLayoutData(fdlProperties);

    // Table view with 2 columns(property name and parameter), change listeners modifying
    // operation.labels
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Properties.Column.Alias"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_NAME),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_EXPRESSION),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.Properties.Column.Descending"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"N", "Y"},
              false),
        };

    TableView wProperties =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getProperties().size(),
            false,
            null,
            props);
    FormData fdProperties = new FormData();
    fdProperties.left = new FormAttachment(middle, margin);
    fdProperties.right = new FormAttachment(100, 0);
    fdProperties.top = new FormAttachment(lastControl, margin);
    fdProperties.bottom = new FormAttachment(100, 0);
    wProperties.setLayoutData(fdProperties);

    for (int i = 0; i < operation.getProperties().size(); i++) {
      TableItem item = wProperties.table.getItem(i);
      Property property = operation.getProperties().get(i);
      item.setText(1, Const.NVL(property.getAlias(), ""));
      item.setText(2, Const.NVL(property.getName(), ""));
      item.setText(3, Const.NVL(property.getExpression(), ""));
      item.setText(4, property.isDescending() ? "Y" : "N");
    }
    wProperties.optimizeTableView();

    // Keep track of any changes with a listener
    //
    Function<TableItem, Property> itemFunction =
        item ->
            new Property(
                item.getText(1),
                item.getText(2),
                item.getText(3),
                null,
                null,
                "Y".equalsIgnoreCase(item.getText(4)));
    wProperties.addModifyListener(
        new TableViewModified<>(wProperties, operation.getProperties(), itemFunction));

    return wProperties;
  }

  private Control addOperationWidgetReturnValues(ReturnOperation operation, Control lastControl) {
    Label wlReturnValues = new Label(wOperationComp, SWT.RIGHT);
    wlReturnValues.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.ReturnValues.Label"));
    wlReturnValues.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.ReturnValues.Tooltip"));
    FormData fdlReturnValues = new FormData();
    fdlReturnValues.left = new FormAttachment(0, 0);
    fdlReturnValues.right = new FormAttachment(middle, 0);
    fdlReturnValues.top = new FormAttachment(lastControl, margin);
    wlReturnValues.setLayoutData(fdlReturnValues);

    // Table view with columns: alias, name, expression, parameter, rename.
    // Change listeners are modifying the operation.
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.Alias"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.Property"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.Expression"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.Rename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.NeoType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphPropertyDataType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.ReturnValues.Column.HopType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
        };

    TableView wReturnValues =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getReturnValues().size(),
            false,
            null,
            props);
    FormData fdReturnValues = new FormData();
    fdReturnValues.left = new FormAttachment(middle, margin);
    fdReturnValues.right = new FormAttachment(100, 0);
    fdReturnValues.top = new FormAttachment(lastControl, margin);
    fdReturnValues.bottom = new FormAttachment(100, 0);
    wReturnValues.setLayoutData(fdReturnValues);

    for (int i = 0; i < operation.getReturnValues().size(); i++) {
      TableItem item = wReturnValues.table.getItem(i);
      ReturnValue returnValue = operation.getReturnValues().get(i);
      item.setText(1, Const.NVL(returnValue.getAlias(), ""));
      item.setText(2, Const.NVL(returnValue.getProperty(), ""));
      item.setText(3, Const.NVL(returnValue.getExpression(), ""));
      item.setText(3, Const.NVL(returnValue.getParameter(), ""));
      item.setText(4, Const.NVL(returnValue.getRename(), ""));
    }
    wReturnValues.optimizeTableView();

    // Keep track of any changes with a listener
    //
    wReturnValues.addModifyListener(
        new TableViewModified<>(
            wReturnValues,
            operation.getReturnValues(),
            item ->
                new ReturnValue(
                    item.getText(1),
                    item.getText(2),
                    item.getText(3),
                    item.getText(4),
                    item.getText(5),
                    item.getText(6),
                    item.getText(7))));

    return wReturnValues;
  }

  private Control addOperationWidgetSetProperties(SetOperation operation, Control lastControl) {
    Label wlProperties = new Label(wOperationComp, SWT.RIGHT);
    wlProperties.setText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.SetProperties.Label"));
    wlProperties.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.SetProperties.Tooltip"));
    FormData fdlProperties = new FormData();
    fdlProperties.left = new FormAttachment(0, 0);
    fdlProperties.right = new FormAttachment(middle, 0);
    fdlProperties.top = new FormAttachment(lastControl, margin);
    wlProperties.setLayoutData(fdlProperties);

    // Table view with property columns: alias, name, parameter, expression.
    // Adds change listeners modifying the operation.
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CypherBuilderDialog.Operation.Properties.Column.Alias"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_NAME),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, "CypherBuilderDialog.Operation.Properties.Column.Parameter"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0],
              false),
          new ColumnInfo(
              BaseMessages.getString(
                  PKG, CONST_CYPHER_BUILDER_DIALOG_OPERATION_PROPERTIES_COLUMN_EXPRESSION),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columns[2].setComboValues(copy.getParameterNames());

    TableView wProperties =
        new TableView(
            variables,
            wOperationComp,
            SWT.NONE,
            columns,
            operation.getProperties().size(),
            false,
            null,
            props);
    FormData fdProperties = new FormData();
    fdProperties.left = new FormAttachment(middle, margin);
    fdProperties.right = new FormAttachment(100, 0);
    fdProperties.top = new FormAttachment(lastControl, margin);
    fdProperties.bottom = new FormAttachment(100, 0);
    wProperties.setLayoutData(fdProperties);

    for (int i = 0; i < operation.getProperties().size(); i++) {
      TableItem item = wProperties.table.getItem(i);
      Property property = operation.getProperties().get(i);
      item.setText(1, Const.NVL(property.getAlias(), ""));
      item.setText(2, Const.NVL(property.getName(), ""));
      item.setText(3, Const.NVL(property.getParameter(), ""));
      item.setText(4, Const.NVL(property.getExpression(), ""));
    }
    wProperties.optimizeTableView();

    // Keep track of any changes with a listener
    //
    Function<TableItem, Property> itemFunction =
        item ->
            new Property(
                item.getText(1), item.getText(2), item.getText(4), item.getText(3), null, false);
    wProperties.addModifyListener(
        new TableViewModified<>(wProperties, operation.getProperties(), itemFunction));

    return wProperties;
  }

  private void addCypherTab() {
    CTabItem wCypherTab = new CTabItem(wTabFolder, SWT.NONE);
    wCypherTab.setFont(GuiResource.getInstance().getFontDefault());
    wCypherTab.setText(BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Cypher.Label"));
    wCypherTab.setToolTipText(
        BaseMessages.getString(PKG, "CypherBuilderDialog.Tab.Cypher.ToolTip"));
    Composite wCypherComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wCypherComp);
    wCypherComp.setLayout(createFormLayout());

    wCypher =
        new Text(wCypherComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wCypher.setFont(GuiResource.getInstance().getFontFixed());
    PropsUi.setLook(wCypher);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(0, 0);
    fdCypher.bottom = new FormAttachment(100, 0);
    wCypher.setLayoutData(fdCypher);
    wCypher.setEditable(false);

    wCypherComp.layout();
    wCypherTab.setControl(wCypherComp);

    // Refresh the content every time the user switches to this tab
    //
    Listener cypherRefresh =
        e -> {
          try {
            wCypher.setText(Const.NVL(copy.getCypher(variables), ""));
          } catch (Exception ex) {
            wCypher.setText(
                "Error building cypher statement: "
                    + Const.CR
                    + Const.CR
                    + Const.getSimpleStackTrace(ex));
          }
        };

    wTabFolder.addListener(
        SWT.Selection,
        e -> {
          if (e.item.equals(wCypherTab)) {
            cypherRefresh.handleEvent(e);
          }
        });
  }

  private Layout createFormLayout() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = PropsUi.getFormMargin();
    formLayout.marginRight = PropsUi.getFormMargin();
    formLayout.marginTop = PropsUi.getFormMargin();
    formLayout.marginBottom = PropsUi.getFormMargin();
    return formLayout;
  }

  private void enableFields() {
    // Do nothing
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  public void getData() {
    wConnection.setText(Const.NVL(copy.getConnectionName(), ""));
    wBatchSize.setText(Const.NVL(copy.getBatchSize(), ""));
    wUnwindAlias.setText(Const.NVL(copy.getUnwindAlias(), ""));
    wRetries.setText(Const.NVL(copy.getRetries(), ""));

    for (int i = 0; i < copy.getParameters().size(); i++) {
      Parameter parameter = copy.getParameters().get(i);
      TableItem item = wParameters.table.getItem(i);
      item.setText(1, Const.NVL(parameter.getName(), ""));
      item.setText(2, Const.NVL(parameter.getInputFieldName(), ""));
      item.setText(3, Const.NVL(parameter.getNeoType(), ""));
    }
    wParameters.removeEmptyRows();
    wParameters.setRowNums();
    wParameters.optWidth(true);

    if (!copy.getOperations().isEmpty()) {
      // Select and edit the first operation
      //
      wOperationsList.select(0);
      operationEdit();
    }

    enableFields();
  }

  public void showExperimentalWarning() {
    // Only show this warning once.
    if (warningShown) {
      return;
    }
    warningShown = true;

    // Tell folks about the experimental nature of this code
    //
    MessageDialogWithToggle md =
        new MessageDialogWithToggle(
            shell,
            "Work in progress",
            "This transform is not finished yet! \n"
                + "It's intended to get feedback from you. \n"
                + "It's not intended to be used in production",
            SWT.ICON_WARNING,
            new String[] {BaseMessages.getString(PKG, "System.Button.OK")},
            "Don't show this message again",
            "N".equalsIgnoreCase(props.getCustomParameter(STRING_EXPERIMENTAL_WARNING, "Y")));
    md.open();
    props.setCustomParameter(STRING_EXPERIMENTAL_WARNING, md.getToggleState() ? "N" : "Y");
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

  private void getInfo(CypherBuilderMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setBatchSize(wBatchSize.getText());
    meta.setUnwindAlias(wUnwindAlias.getText());
    meta.setRetries(wRetries.getText());

    List<Parameter> parameters = new ArrayList<>();
    for (int i = 0; i < wParameters.nrNonEmpty(); i++) {
      TableItem item = wParameters.getNonEmpty(i);
      parameters.add(new Parameter(item.getText(1), item.getText(2), item.getText(3)));
    }
    meta.setParameters(parameters);

    // The other information is stored in the copy we created
    //
    meta.setOperations(copy.getOperations());
  }

  private interface ITextChanged {
    void changed(Text text);
  }

  private class TableViewModified<T> implements ModifyListener {
    private final TableView tableView;
    private final List<T> list;
    private final Function<TableItem, T> itemFunction;

    public TableViewModified(
        TableView tableView, List<T> list, Function<TableItem, T> itemFunction) {
      this.tableView = tableView;
      this.list = list;
      this.itemFunction = itemFunction;
    }

    @Override
    public void modifyText(ModifyEvent e) {
      // e.widget : Text/TextVar being edited
      // We also want the text in the text editor buffer.
      //
      String newValue = "";
      if (e.widget instanceof Text text) {
        newValue = text.getText();
      } else if (e.widget instanceof TextVar textVar) {
        newValue = textVar.getText();
      } else if (e.widget instanceof CCombo cCombo) {
        newValue = cCombo.getText();
      } else if (e.widget instanceof Combo combo) {
        newValue = combo.getText();
      } else if (e.widget instanceof ComboVar comboVar) {
        newValue = comboVar.getText();
      }

      TableItem activeItem = tableView.table.getSelection()[0];
      int activeColumn = tableView.getActiveTableColumn();
      String oldValue = activeItem.getText(activeColumn);
      activeItem.setText(activeColumn, newValue);

      // If the text is modified we want re-build the whole list of items T
      //
      list.clear();
      for (TableItem item : tableView.getNonEmptyItems()) {
        list.add(itemFunction.apply(item));
      }
    }
  }
}
