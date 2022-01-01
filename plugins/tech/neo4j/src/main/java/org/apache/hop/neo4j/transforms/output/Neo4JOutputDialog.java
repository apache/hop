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

package org.apache.hop.neo4j.transforms.output;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.core.Neo4jUtil;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class Neo4JOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG =
      Neo4JOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Neo4JOutputMeta input;

  private MetaSelectionLine<NeoConnection> wConnection;
  private Label wlBatchSize;
  private TextVar wBatchSize;
  private Label wlCreateIndexes;
  private Button wCreateIndexes;
  private Label wlUseCreate;
  private Button wUseCreate;
  private Label wlOnlyCreateRelationships;
  private Button wOnlyCreateRelationships;
  private Button wReturnGraph;
  private Label wlReturnGraphField;
  private TextVar wReturnGraphField;

  private Combo wRel;
  private TextVar wRelValue;
  private TableView wFromPropsGrid;
  private TableView wFromLabelGrid;
  private TableView wToPropsGrid;
  private TableView wToLabelGrid;
  private TableView wRelPropsGrid;

  private Button wReadOnlyFromNode;
  private Button wReadOnlyToNode;

  public Neo4JOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (Neo4JOutputMeta) in;

    metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Fields
    String[] fieldNames;
    try {
      IRowMeta prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
      fieldNames = prevFields.getFieldNames();
    } catch (HopTransformException kse) {
      logError(BaseMessages.getString(PKG, "TripleOutput.Log.ErrorGettingFieldNames"));
      fieldNames = new String[] {};
    }

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.StepName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            NeoConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Neo4j Connection",
            "The name of the Neo4j connection to use");
    props.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(lastControl, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }
    lastControl = wConnection;

    wlBatchSize = new Label(shell, SWT.RIGHT);
    wlBatchSize.setText("Batch size (rows)");
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    wlCreateIndexes = new Label(shell, SWT.RIGHT);
    wlCreateIndexes.setText("Create indexes? ");
    props.setLook(wlCreateIndexes);
    FormData fdlCreateIndexes = new FormData();
    fdlCreateIndexes.left = new FormAttachment(0, 0);
    fdlCreateIndexes.right = new FormAttachment(middle, -margin);
    fdlCreateIndexes.top = new FormAttachment(lastControl, 2 * margin);
    wlCreateIndexes.setLayoutData(fdlCreateIndexes);
    wCreateIndexes = new Button(shell, SWT.CHECK | SWT.BORDER);
    props.setLook(wCreateIndexes);
    FormData fdCreateIndexes = new FormData();
    fdCreateIndexes.left = new FormAttachment(middle, 0);
    fdCreateIndexes.right = new FormAttachment(100, 0);
    fdCreateIndexes.top = new FormAttachment(wlCreateIndexes, 0, SWT.CENTER);
    wCreateIndexes.setLayoutData(fdCreateIndexes);
    lastControl = wCreateIndexes;

    wlUseCreate = new Label(shell, SWT.RIGHT);
    wlUseCreate.setText("Use CREATE instead of MERGE? ");
    props.setLook(wlUseCreate);
    FormData fdlUseCreate = new FormData();
    fdlUseCreate.left = new FormAttachment(0, 0);
    fdlUseCreate.right = new FormAttachment(middle, -margin);
    fdlUseCreate.top = new FormAttachment(lastControl, 2 * margin);
    wlUseCreate.setLayoutData(fdlUseCreate);
    wUseCreate = new Button(shell, SWT.CHECK | SWT.BORDER);
    props.setLook(wUseCreate);
    FormData fdUseCreate = new FormData();
    fdUseCreate.left = new FormAttachment(middle, 0);
    fdUseCreate.right = new FormAttachment(100, 0);
    fdUseCreate.top = new FormAttachment(wlUseCreate, 0, SWT.CENTER);
    wUseCreate.setLayoutData(fdUseCreate);
    lastControl = wUseCreate;

    wlOnlyCreateRelationships = new Label(shell, SWT.RIGHT);
    wlOnlyCreateRelationships.setText("Only create relationships? ");
    props.setLook(wlOnlyCreateRelationships);
    FormData fdlOnlyCreateRelationships = new FormData();
    fdlOnlyCreateRelationships.left = new FormAttachment(0, 0);
    fdlOnlyCreateRelationships.right = new FormAttachment(middle, -margin);
    fdlOnlyCreateRelationships.top = new FormAttachment(lastControl, 2 * margin);
    wlOnlyCreateRelationships.setLayoutData(fdlOnlyCreateRelationships);
    wOnlyCreateRelationships = new Button(shell, SWT.CHECK | SWT.BORDER);
    props.setLook(wOnlyCreateRelationships);
    FormData fdOnlyCreateRelationships = new FormData();
    fdOnlyCreateRelationships.left = new FormAttachment(middle, 0);
    fdOnlyCreateRelationships.right = new FormAttachment(100, 0);
    fdOnlyCreateRelationships.top = new FormAttachment(wlOnlyCreateRelationships, 0, SWT.CENTER);
    wOnlyCreateRelationships.setLayoutData(fdOnlyCreateRelationships);
    wOnlyCreateRelationships.addListener(SWT.Selection, e -> enableFields());
    lastControl = wOnlyCreateRelationships;

    Label wlReturnGraph = new Label(shell, SWT.RIGHT);
    wlReturnGraph.setText("Return graph data?");
    String returnGraphTooltipText =
        "The update data to be updated in the form of Graph a value in the output of this transform";
    wlReturnGraph.setToolTipText(returnGraphTooltipText);
    props.setLook(wlReturnGraph);
    FormData fdlReturnGraph = new FormData();
    fdlReturnGraph.left = new FormAttachment(0, 0);
    fdlReturnGraph.right = new FormAttachment(middle, -margin);
    fdlReturnGraph.top = new FormAttachment(lastControl, 2 * margin);
    wlReturnGraph.setLayoutData(fdlReturnGraph);
    wReturnGraph = new Button(shell, SWT.CHECK | SWT.BORDER);
    wReturnGraph.setToolTipText(returnGraphTooltipText);
    props.setLook(wReturnGraph);
    FormData fdReturnGraph = new FormData();
    fdReturnGraph.left = new FormAttachment(middle, 0);
    fdReturnGraph.right = new FormAttachment(100, 0);
    fdReturnGraph.top = new FormAttachment(wlReturnGraph, 0, SWT.CENTER);
    wReturnGraph.setLayoutData(fdReturnGraph);
    wReturnGraph.addListener(SWT.Selection, e -> enableFields());
    lastControl = wReturnGraph;

    wlReturnGraphField = new Label(shell, SWT.RIGHT);
    wlReturnGraphField.setText("Graph output field name");
    props.setLook(wlReturnGraphField);
    FormData fdlReturnGraphField = new FormData();
    fdlReturnGraphField.left = new FormAttachment(0, 0);
    fdlReturnGraphField.right = new FormAttachment(middle, -margin);
    fdlReturnGraphField.top = new FormAttachment(lastControl, 2 * margin);
    wlReturnGraphField.setLayoutData(fdlReturnGraphField);
    wReturnGraphField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wReturnGraphField);
    wReturnGraphField.addModifyListener(lsMod);
    FormData fdReturnGraphField = new FormData();
    fdReturnGraphField.left = new FormAttachment(middle, 0);
    fdReturnGraphField.right = new FormAttachment(100, 0);
    fdReturnGraphField.top = new FormAttachment(wlReturnGraphField, 0, SWT.CENTER);
    wReturnGraphField.setLayoutData(fdReturnGraphField);
    lastControl = wReturnGraphField;

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Add listeners
    //
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(lastControl, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    /*
     * STRING_FROM
     */
    CTabItem wFromTab = new CTabItem(wTabFolder, SWT.NONE);
    wFromTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.FromTab"));

    FormLayout fromLayout = new FormLayout();
    fromLayout.marginWidth = 3;
    fromLayout.marginHeight = 3;

    Composite wFromComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFromComp);
    wFromComp.setLayout(fromLayout);

    // Read only "from" node?
    //
    wReadOnlyFromNode = new Button(wFromComp, SWT.CHECK);
    wReadOnlyFromNode.setText(
        BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.ReadOnlyFromNode"));
    props.setLook(wReadOnlyFromNode);
    FormData fdReadOnlyFromNode = new FormData();
    fdReadOnlyFromNode.left = new FormAttachment(middle, margin);
    fdReadOnlyFromNode.right = new FormAttachment(100, 0);
    fdReadOnlyFromNode.top = new FormAttachment(0, margin * 3);
    wReadOnlyFromNode.setLayoutData(fdReadOnlyFromNode);
    Control lastFromControl = wReadOnlyFromNode;

    // Labels
    Label wlFromLabel = new Label(wFromComp, SWT.RIGHT);
    wlFromLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.FromLabel"));
    props.setLook(wlFromLabel);
    FormData fdlFromLabels = new FormData();
    fdlFromLabels.left = new FormAttachment(0, 0);
    fdlFromLabels.right = new FormAttachment(middle, 0);
    fdlFromLabels.top = new FormAttachment(lastFromControl, margin);
    wlFromLabel.setLayoutData(fdlFromLabels);
    final int fromLabelRows =
        (input.getFromNodeLabels() != null ? input.getFromNodeLabels().length : 10);
    ColumnInfo[] fromLabelInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.FromLabelsTable.FromFields"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.FromLabelsTable.FromValues"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    fromLabelInf[1].setUsingVariables(true);
    wFromLabelGrid =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wFromComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            fromLabelInf,
            fromLabelRows,
            null,
            PropsUi.getInstance());
    props.setLook(wFromLabelGrid);

    Button wGetFromLabel = new Button(wFromComp, SWT.PUSH);
    wGetFromLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button"));
    wGetFromLabel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            get(0);
          }
        });
    FormData fdGetFromLabel = new FormData();
    fdGetFromLabel.right = new FormAttachment(100, 0);
    fdGetFromLabel.top = new FormAttachment(lastFromControl, margin);

    wGetFromLabel.setLayoutData(fdGetFromLabel);

    FormData fdFromLabelGrid = new FormData();
    fdFromLabelGrid.left = new FormAttachment(middle, margin);
    fdFromLabelGrid.top = new FormAttachment(lastFromControl, margin);
    fdFromLabelGrid.right = new FormAttachment(wGetFromLabel, 0);
    fdFromLabelGrid.bottom =
        new FormAttachment(0, margin * 2 + (int) (props.getZoomFactor() * 150));
    wFromLabelGrid.setLayoutData(fdFromLabelGrid);
    lastFromControl = wFromLabelGrid;

    // Node properties
    Label wlFromFields = new Label(wFromComp, SWT.RIGHT);
    wlFromFields.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFields.Properties"));
    props.setLook(wlFromFields);
    FormData fdlFromFields = new FormData();
    fdlFromFields.left = new FormAttachment(0, 0);
    fdlFromFields.right = new FormAttachment(middle, 0);
    fdlFromFields.top = new FormAttachment(lastFromControl, margin);
    wlFromFields.setLayoutData(fdlFromFields);
    final int fromPropsRows =
        (input.getFromNodeProps() != null ? input.getFromNodeProps().length : 10);
    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFields"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFieldsName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.PropType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphPropertyType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.PropPrimary"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              false),
        };
    wFromPropsGrid =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wFromComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fromPropsRows,
            null,
            props);
    props.setLook(wFromPropsGrid);

    Button wGetFromProps = new Button(wFromComp, SWT.PUSH);
    wGetFromProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button"));
    wGetFromProps.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            get(1);
          }
        });
    FormData fdGetFromProps = new FormData();
    fdGetFromProps.right = new FormAttachment(100, 0);
    fdGetFromProps.top = new FormAttachment(lastFromControl, margin);
    wGetFromProps.setLayoutData(fdGetFromProps);

    FormData fdFromPropsGrid = new FormData();
    fdFromPropsGrid.left = new FormAttachment(middle, margin);
    fdFromPropsGrid.right = new FormAttachment(wGetFromProps, 0);
    fdFromPropsGrid.top = new FormAttachment(lastFromControl, margin);
    fdFromPropsGrid.bottom = new FormAttachment(100, 0);
    wFromPropsGrid.setLayoutData(fdFromPropsGrid);
    lastFromControl = wFromPropsGrid;

    FormData fdFromComp = new FormData();
    fdFromComp.left = new FormAttachment(0, 0);
    fdFromComp.top = new FormAttachment(0, 0);
    fdFromComp.right = new FormAttachment(100, 0);
    fdFromComp.bottom = new FormAttachment(100, 0);
    wFromComp.setLayoutData(fdFromComp);

    wFromComp.layout();
    wFromTab.setControl(wFromComp);

    /*
     * STRING_TO
     */

    CTabItem wToTab = new CTabItem(wTabFolder, SWT.NONE);
    wToTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.ToTab"));

    FormLayout toLayout = new FormLayout();
    toLayout.marginWidth = 3;
    toLayout.marginHeight = 3;

    Composite wToComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wToComp);
    wToComp.setLayout(toLayout);

    // Read only "to" node?
    //
    wReadOnlyToNode = new Button(wToComp, SWT.CHECK);
    wReadOnlyToNode.setText(
        BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.ReadOnlyToNode"));
    props.setLook(wReadOnlyToNode);
    FormData fdReadOnlyToNode = new FormData();
    fdReadOnlyToNode.left = new FormAttachment(middle, margin);
    fdReadOnlyToNode.right = new FormAttachment(100, 0);
    fdReadOnlyToNode.top = new FormAttachment(0, margin * 3);
    wReadOnlyToNode.setLayoutData(fdReadOnlyToNode);
    Control lastToControl = wReadOnlyToNode;

    // Labels
    Label wlToLabel = new Label(wToComp, SWT.RIGHT);
    wlToLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.ToLabel"));
    props.setLook(wlToLabel);
    FormData fdlToLabels = new FormData();
    fdlToLabels.left = new FormAttachment(0, 0);
    fdlToLabels.right = new FormAttachment(middle, 0);
    fdlToLabels.top = new FormAttachment(lastToControl, margin);
    wlToLabel.setLayoutData(fdlToLabels);
    final int toLabelRows = (input.getToNodeLabels() != null ? input.getToNodeLabels().length : 10);
    ColumnInfo[] toLabelInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.ToLabelsTable.ToFields"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.ToLabelsTable.ToValues"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    toLabelInf[1].setUsingVariables(true);

    wToLabelGrid =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wToComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            toLabelInf,
            toLabelRows,
            null,
            PropsUi.getInstance());
    props.setLook(wToLabelGrid);

    Button wGetToLabel = new Button(wToComp, SWT.PUSH);
    wGetToLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button"));
    wGetToLabel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            get(2);
          }
        });
    FormData fdGetToLabel = new FormData();
    fdGetToLabel.right = new FormAttachment(100, 0);
    fdGetToLabel.top = new FormAttachment(lastToControl, margin);
    wGetToLabel.setLayoutData(fdGetToLabel);

    FormData fdToLabelGrid = new FormData();
    fdToLabelGrid.left = new FormAttachment(middle, margin);
    fdToLabelGrid.right = new FormAttachment(wGetToLabel, 0);
    fdToLabelGrid.top = new FormAttachment(lastToControl, margin);
    fdToLabelGrid.bottom = new FormAttachment(0, margin * 2 + (int) (props.getZoomFactor() * 150));
    fdToLabelGrid.bottom = new FormAttachment(0, margin * 2 + (int) (props.getZoomFactor() * 150));
    wToLabelGrid.setLayoutData(fdToLabelGrid);
    lastToControl = wToLabelGrid;

    // Node properties
    Label wlToFields = new Label(wToComp, SWT.RIGHT);
    wlToFields.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFields.Properties"));
    props.setLook(wlToFields);
    FormData fdlToFields = new FormData();
    fdlToFields.left = new FormAttachment(0, 0);
    fdlToFields.right = new FormAttachment(middle, 0);
    fdlToFields.top = new FormAttachment(lastToControl, margin);
    wlToFields.setLayoutData(fdlToFields);
    final int toPropsRows = (input.getToNodeProps() != null ? input.getToNodeProps().length : 10);
    ColumnInfo[] toColinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFieldsTable.ToFields"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFieldsTable.ToFieldsName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.PropType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphPropertyType.getNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.PropPrimary"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              false),
        };

    wToPropsGrid =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wToComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            toColinf,
            toPropsRows,
            null,
            PropsUi.getInstance());

    props.setLook(wToPropsGrid);

    Button wGetToProps = new Button(wToComp, SWT.PUSH);
    wGetToProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button"));
    wGetToProps.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            get(3);
          }
        });
    FormData fdGetToProps = new FormData();
    fdGetToProps.right = new FormAttachment(100, 0);
    fdGetToProps.top = new FormAttachment(lastToControl, margin);
    wGetToProps.setLayoutData(fdGetToProps);

    FormData fdToPropsGrid = new FormData();
    fdToPropsGrid.left = new FormAttachment(middle, margin);
    fdToPropsGrid.right = new FormAttachment(wGetToProps, 0);
    fdToPropsGrid.top = new FormAttachment(lastToControl, margin);
    fdToPropsGrid.bottom = new FormAttachment(100, 0);
    wToPropsGrid.setLayoutData(fdToPropsGrid);

    FormData fdToComp = new FormData();
    fdToComp.left = new FormAttachment(0, 0);
    fdToComp.top = new FormAttachment(0, 0);
    fdToComp.right = new FormAttachment(100, 0);
    fdToComp.bottom = new FormAttachment(100, 0);
    wToComp.setLayoutData(fdToComp);

    wToComp.layout();
    wToTab.setControl(wToComp);

    /*
     * Relationships
     */
    CTabItem wRelationshipsTab = new CTabItem(wTabFolder, SWT.NONE);
    wRelationshipsTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.RelationshipsTab"));

    FormLayout relationshipsLayout = new FormLayout();
    relationshipsLayout.marginWidth = 3;
    relationshipsLayout.marginHeight = 3;

    Composite wRelationshipsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wRelationshipsComp);
    wRelationshipsComp.setLayout(relationshipsLayout);

    // Relationship field
    Label wlRel = new Label(wRelationshipsComp, SWT.RIGHT);
    wlRel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Relationship.Label"));
    props.setLook(wlRel);
    FormData fdlRel = new FormData();
    fdlRel.left = new FormAttachment(0, 0);
    fdlRel.top = new FormAttachment(0, 0);
    wlRel.setLayoutData(fdlRel);
    wRel = new Combo(wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRel.setItems(fieldNames);
    props.setLook(wRel);
    wRel.addModifyListener(lsMod);
    FormData fdRel = new FormData();
    fdRel.left = new FormAttachment(wlRel, margin);
    fdRel.right = new FormAttachment(100, 0);
    fdRel.top = new FormAttachment(wlRel, 0, SWT.CENTER);
    wRel.setLayoutData(fdRel);
    lastControl = wRel;

    // Relationship value field
    Label wlRelValue = new Label(wRelationshipsComp, SWT.RIGHT);
    wlRelValue.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.RelationshipValue.Label"));
    props.setLook(wlRelValue);
    FormData fdlRelValue = new FormData();
    fdlRelValue.left = new FormAttachment(0, 0);
    fdlRelValue.top = new FormAttachment(lastControl, margin * 2);
    wlRelValue.setLayoutData(fdlRelValue);
    wRelValue = new TextVar(variables, wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelValue);
    wRelValue.addModifyListener(lsMod);
    FormData fdRelValue = new FormData();
    fdRelValue.left = new FormAttachment(wlRelValue, margin);
    fdRelValue.right = new FormAttachment(100, 0);
    fdRelValue.top = new FormAttachment(wlRelValue, 0, SWT.CENTER);
    wRelValue.setLayoutData(fdRelValue);
    lastControl = wRelValue;

    // Relationship properties
    Label wlRelProps = new Label(wRelationshipsComp, SWT.RIGHT);
    wlRelProps.setText(
        BaseMessages.getString(PKG, "Neo4JOutputDialog.RelationshipProperties.Label"));
    props.setLook(wlRelProps);
    FormData fdlRelProps = new FormData();
    fdlRelProps.left = new FormAttachment(0, 0);
    fdlRelProps.top = new FormAttachment(lastControl, margin * 3);
    wlRelProps.setLayoutData(fdlRelProps);

    final int relPropsRows = (input.getRelProps() != null ? input.getRelProps().length : 10);
    ColumnInfo[] relPropsInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              fieldNames,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesFieldName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4JOutputDialog.PropType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphPropertyType.getNames(),
              false),
        };
    wRelPropsGrid =
        new TableView(
            Variables.getADefaultVariableSpace(),
            wRelationshipsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            relPropsInf,
            relPropsRows,
            null,
            PropsUi.getInstance());
    props.setLook(wRelPropsGrid);

    Button wbRelProps = new Button(wRelationshipsComp, SWT.PUSH);
    wbRelProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button"));
    wbRelProps.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            get(4);
          }
        });
    FormData fdRelProps = new FormData();
    fdRelProps.right = new FormAttachment(100, 0);
    fdRelProps.top = new FormAttachment(lastControl, margin * 3);
    wbRelProps.setLayoutData(fdRelProps);

    FormData fdRelPropsGrid = new FormData();
    fdRelPropsGrid.left = new FormAttachment(wlRelProps, margin);
    fdRelPropsGrid.right = new FormAttachment(wbRelProps, -margin);
    fdRelPropsGrid.top = new FormAttachment(lastControl, margin * 3);
    fdRelPropsGrid.bottom = new FormAttachment(100, 0);
    wRelPropsGrid.setLayoutData(fdRelPropsGrid);

    FormData fdRelationshipsComp = new FormData();
    fdRelationshipsComp.left = new FormAttachment(0, 0);
    fdRelationshipsComp.top = new FormAttachment(0, 0);
    fdRelationshipsComp.right = new FormAttachment(100, 0);
    fdRelationshipsComp.bottom = new FormAttachment(100, 0);
    wRelationshipsComp.setLayoutData(fdRelationshipsComp);

    wRelationshipsComp.layout();
    wRelationshipsTab.setControl(wRelationshipsComp);

    wTabFolder.setSelection(0);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void enableFields() {

    boolean toNeo = !wReturnGraph.getSelection();

    wConnection.setEnabled(toNeo);
    wlBatchSize.setEnabled(toNeo);
    wBatchSize.setEnabled(toNeo);
    wlCreateIndexes.setEnabled(toNeo);
    wCreateIndexes.setEnabled(toNeo);
    wlUseCreate.setEnabled(toNeo);
    wUseCreate.setEnabled(toNeo);
    wlOnlyCreateRelationships.setEnabled(toNeo);
    wOnlyCreateRelationships.setEnabled(toNeo);

    wlReturnGraphField.setEnabled(!toNeo);
    wReturnGraphField.setEnabled(!toNeo);

    boolean onlyCreateRelationships = wOnlyCreateRelationships.getSelection();

    wReadOnlyFromNode.setEnabled(!onlyCreateRelationships);
    wReadOnlyToNode.setEnabled(!onlyCreateRelationships);
  }

  private void getData() {
    wTransformName.setText(transformName);
    wTransformName.selectAll();
    wConnection.setText(Const.NVL(input.getConnection(), ""));
    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wCreateIndexes.setSelection(input.isCreatingIndexes());
    wUseCreate.setSelection(input.isUsingCreate());
    wOnlyCreateRelationships.setSelection(input.isOnlyCreatingRelationships());
    wReturnGraph.setSelection(input.isReturningGraph());
    wReturnGraphField.setText(Const.NVL(input.getReturnGraphField(), ""));

    wReadOnlyFromNode.setSelection(input.isReadOnlyFromNode());
    wReadOnlyToNode.setSelection(input.isReadOnlyToNode());

    if (input.getFromNodeLabels() != null) {
      String[] fromNodeLabels = input.getFromNodeLabels();
      String[] fromNodeLabelValues = input.getFromNodeLabelValues();

      for (int i = 0; i < fromNodeLabels.length; i++) {
        TableItem item = wFromLabelGrid.table.getItem(i);
        item.setText(1, Const.NVL(fromNodeLabels[i], ""));
        item.setText(2, Const.NVL(fromNodeLabelValues[i], ""));
      }
    }

    if (input.getFromNodeProps() != null) {

      for (int i = 0; i < input.getFromNodeProps().length; i++) {
        TableItem item = wFromPropsGrid.table.getItem(i);
        item.setText(1, Const.NVL(input.getFromNodeProps()[i], ""));
        item.setText(2, Const.NVL(input.getFromNodePropNames()[i], ""));
        item.setText(3, Const.NVL(input.getFromNodePropTypes()[i], ""));
        item.setText(4, input.getFromNodePropPrimary()[i] ? "Y" : "N");
      }
    }

    if (input.getToNodeLabels() != null) {
      String[] toNodeLabels = input.getToNodeLabels();
      String[] toNodeLabelValues = input.getToNodeLabelValues();

      for (int i = 0; i < toNodeLabels.length; i++) {
        TableItem item = wToLabelGrid.table.getItem(i);
        item.setText(1, Const.NVL(toNodeLabels[i], ""));
        item.setText(2, Const.NVL(toNodeLabelValues[i], ""));
      }
    }

    if (input.getToNodeProps() != null) {
      for (int i = 0; i < input.getToNodeProps().length; i++) {
        TableItem item = wToPropsGrid.table.getItem(i);
        item.setText(1, Const.NVL(input.getToNodeProps()[i], ""));
        item.setText(2, Const.NVL(input.getToNodePropNames()[i], ""));
        item.setText(3, Const.NVL(input.getToNodePropTypes()[i], ""));
        item.setText(4, input.getToNodePropPrimary()[i] ? "Y" : "N");
      }
    }

    wRel.setText(Const.NVL(input.getRelationship(), ""));
    wRelValue.setText(Const.NVL(input.getRelationshipValue(), ""));

    if (input.getRelProps() != null) {
      for (int i = 0; i < input.getRelProps().length; i++) {
        TableItem item = wRelPropsGrid.table.getItem(i);
        item.setText(1, Const.NVL(input.getRelProps()[i], ""));
        item.setText(2, Const.NVL(input.getRelPropNames()[i], ""));
        item.setText(3, Const.NVL(input.getRelPropTypes()[i], ""));
      }
    }

    enableFields();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {

    transformName = wTransformName.getText();
    input.setConnection(wConnection.getText());
    input.setBatchSize(wBatchSize.getText());
    input.setCreatingIndexes(wCreateIndexes.getSelection());
    input.setUsingCreate(wUseCreate.getSelection());
    input.setOnlyCreatingRelationships(wOnlyCreateRelationships.getSelection());
    input.setReturningGraph(wReturnGraph.getSelection());
    input.setReturnGraphField(wReturnGraphField.getText());

    input.setReadOnlyFromNode(wReadOnlyFromNode.getSelection());
    input.setReadOnlyToNode(wReadOnlyToNode.getSelection());

    String[] fromNodeLabels = new String[wFromLabelGrid.nrNonEmpty()];
    String[] fromNodeLabelValues = new String[wFromLabelGrid.nrNonEmpty()];
    for (int i = 0; i < fromNodeLabels.length; i++) {
      TableItem item = wFromLabelGrid.table.getItem(i);
      fromNodeLabels[i] = item.getText(1);
      fromNodeLabelValues[i] = item.getText(2);
    }
    input.setFromNodeLabels(fromNodeLabels);
    input.setFromNodeLabelValues(fromNodeLabelValues);

    String[] toNodeLabels = new String[wToLabelGrid.nrNonEmpty()];
    String[] toNodeLabelValues = new String[wToLabelGrid.nrNonEmpty()];
    for (int i = 0; i < toNodeLabels.length; i++) {
      TableItem item = wToLabelGrid.table.getItem(i);
      toNodeLabels[i] = item.getText(1);
      toNodeLabelValues[i] = item.getText(2);
    }
    input.setToNodeLabels(toNodeLabels);
    input.setToNodeLabelValues(toNodeLabelValues);

    int nbFromPropLines = wFromPropsGrid.nrNonEmpty();
    String[] fromNodeProps = new String[nbFromPropLines];
    String[] fromNodePropNames = new String[nbFromPropLines];
    String[] fromNodePropTypes = new String[nbFromPropLines];
    boolean[] fromNodePropPrimary = new boolean[nbFromPropLines];

    for (int i = 0; i < fromNodeProps.length; i++) {
      TableItem item = wFromPropsGrid.table.getItem(i);
      fromNodeProps[i] = item.getText(1);
      fromNodePropNames[i] = item.getText(2);
      fromNodePropTypes[i] = item.getText(3);
      fromNodePropPrimary[i] = "Y".equalsIgnoreCase(item.getText(4));
    }
    input.setFromNodeProps(fromNodeProps);
    input.setFromNodePropNames(fromNodePropNames);
    input.setFromNodePropTypes(fromNodePropTypes);
    input.setFromNodePropPrimary(fromNodePropPrimary);

    int nbToPropLines = wToPropsGrid.nrNonEmpty();
    String[] toNodeProps = new String[nbToPropLines];
    String[] toNodePropNames = new String[nbToPropLines];
    String[] toNodePropTypes = new String[nbToPropLines];
    boolean[] toNodePropPrimary = new boolean[nbToPropLines];

    for (int i = 0; i < toNodeProps.length; i++) {
      TableItem item = wToPropsGrid.table.getItem(i);
      toNodeProps[i] = item.getText(1);
      toNodePropNames[i] = item.getText(2);
      toNodePropTypes[i] = item.getText(3);
      toNodePropPrimary[i] = "Y".equalsIgnoreCase(item.getText(4));
    }
    input.setToNodeProps(toNodeProps);
    input.setToNodePropNames(toNodePropNames);
    input.setToNodePropTypes(toNodePropTypes);
    input.setToNodePropPrimary(toNodePropPrimary);

    input.setRelationship(wRel.getText());
    input.setRelationshipValue(wRelValue.getText());

    int nbRelProps = wRelPropsGrid.nrNonEmpty();
    String[] relProps = new String[nbRelProps];
    String[] relPropNames = new String[nbRelProps];
    String[] relPropTypes = new String[nbRelProps];
    for (int i = 0; i < relProps.length; i++) {
      TableItem item = wRelPropsGrid.table.getItem(i);
      relProps[i] = item.getText(1);
      relPropNames[i] = item.getText(2);
      relPropTypes[i] = item.getText(3);
    }
    input.setRelProps(relProps);
    input.setRelPropNames(relPropNames);
    input.setRelPropTypes(relPropTypes);

    // Mark transform as changed
    transformMeta.setChanged();

    // Show some warnings if needed...
    //
    validateAndWarn(input);

    dispose();
  }

  public static final String STRING_DYNAMIC_LABELS_WARNING =
      "NEO4J_OUTPUT_SHOW_DYNAMIC_LABELS_WARNING";

  private void validateAndWarn(Neo4JOutputMeta input) {
    StringBuffer message = new StringBuffer();

    // Warn about running too many small UNWIND statements when using dynamic labels
    //
    boolean dynamicFrom = input.dynamicFromLabels() && input.isUsingCreate();
    if (dynamicFrom) {
      message.append(Const.CR);
      message.append(
          BaseMessages.getString(PKG, "Neo4JOutputDialog.Warning.SortDynamicFromLabels", Const.CR));
    }
    boolean dynamicTo = input.dynamicToLabels() && input.isUsingCreate();
    if (dynamicTo) {
      message.append(Const.CR);
      message.append(
          BaseMessages.getString(PKG, "Neo4JOutputDialog.Warning.SortDynamicToLabels", Const.CR));
    }
    if (input.isOnlyCreatingRelationships()
        && input.isCreatingRelationships()
        && (input.dynamicFromLabels() || input.dynamicToLabels())) {
      message.append(Const.CR);
      message.append(
          BaseMessages.getString(
              PKG, "Neo4JOutputDialog.Warning.SortDynamicRelationshipLabel", Const.CR));
    }

    // Verify that the defined connection is available
    //
    try {
      IHopMetadataSerializer<NeoConnection> connectionSerializer =
          metadataProvider.getSerializer(NeoConnection.class);
      NeoConnection connection = connectionSerializer.load(input.getConnection());
      if (connection == null) {
        message.append(Const.CR);
        message.append(
            BaseMessages.getString(
                PKG,
                "Neo4JOutputDialog.Warning.ReferencedNeo4jConnectionDoesntExist",
                input.getConnection(),
                Const.CR));
      }
    } catch (Exception e) {
      message
          .append("There was an error verifying the existence of the used Neo4j connection")
          .append(Const.CR)
          .append(Const.getStackTracker(e))
          .append(Const.CR);
    }

    // Warn people about the "Create indexes" button
    //
    if (input.isCreatingIndexes() && (input.dynamicFromLabels() || input.dynamicToLabels())) {
      message.append(Const.CR);
      message.append(
          BaseMessages.getString(
              PKG, "Neo4JOutputDialog.Warning.CreateIndexesIsLimited", Const.CR));
    }

    if (message.length() > 0
        && "Y".equalsIgnoreCase(props.getCustomParameter(STRING_DYNAMIC_LABELS_WARNING, "Y"))) {

      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "Neo4JOutputDialog.DynamicLabelsWarning.DialogTitle"),
              message + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "Neo4JOutputDialog.DynamicLabelsWarning.Understood")
              },
              BaseMessages.getString(PKG, "Neo4JOutputDialog.DynamicLabelsWarning.HideNextTime"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_DYNAMIC_LABELS_WARNING, "Y")));
      md.open();
      props.setCustomParameter(STRING_DYNAMIC_LABELS_WARNING, md.getToggleState() ? "N" : "Y");
    }
  }

  private void get(int button) {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        switch (button) {
            /* 0: from labels grid
             * 1: from properties grid
             * 2: to labels grid
             * 3: to properties grid
             * 4: relationship properties grid
             */
          case 0:
            BaseTransformDialog.getFieldsFromPrevious(
                r, wFromLabelGrid, 1, new int[] {1}, new int[] {}, -1, -1, null);
            break;
          case 1:
            BaseTransformDialog.getFieldsFromPrevious(
                r,
                wFromPropsGrid,
                1,
                new int[] {1, 2},
                new int[] {},
                -1,
                -1,
                (item, valueMeta) ->
                    getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4));
            break;
          case 2:
            BaseTransformDialog.getFieldsFromPrevious(
                r, wToLabelGrid, 1, new int[] {1}, new int[] {}, -1, -1, null);
            break;
          case 3:
            BaseTransformDialog.getFieldsFromPrevious(
                r,
                wToPropsGrid,
                1,
                new int[] {1, 2},
                new int[] {},
                -1,
                -1,
                (item, valueMeta) ->
                    getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4));
            break;
          case 4:
            BaseTransformDialog.getFieldsFromPrevious(
                r,
                wRelPropsGrid,
                1,
                new int[] {1, 2},
                new int[] {},
                -1,
                -1,
                (item, valueMeta) ->
                    getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4));
            break;
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage"),
          ke); //$NON-NLS-2$
    }
  }

  public static boolean getPropertyNameTypePrimary(
      TableItem item,
      IValueMeta valueMeta,
      int[] nameColumns,
      int[] typeColumns,
      int primaryColumn) {

    for (int nameColumn : nameColumns) {
      // Initcap the names in there, remove spaces and weird characters, lowercase first character
      // Issue #13
      //   Text Area 1 --> textArea1
      //   My_Silly_Column --> mySillyColumn
      //
      String propertyName = Neo4jUtil.standardizePropertyName(valueMeta);

      item.setText(nameColumn, propertyName);
    }

    for (int typeColumn : typeColumns) {
      GraphPropertyType type = GraphPropertyType.getTypeFromHop(valueMeta);
      item.setText(typeColumn, type.name());
    }

    if (primaryColumn > 0) {
      item.setText(primaryColumn, "N");
    }

    return true;
  }
}
