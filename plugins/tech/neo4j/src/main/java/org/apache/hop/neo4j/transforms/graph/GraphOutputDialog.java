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

package org.apache.hop.neo4j.transforms.graph;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = GraphOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;

  private MetaSelectionLine<NeoConnection> wConnection;
  private MetaSelectionLine<GraphModel> wModel;

  private Label wlBatchSize;
  private TextVar wBatchSize;
  private Label wlCreateIndexes;
  private Button wCreateIndexes;
  private Button wReturnGraph;
  private Label wlReturnGraphField;
  private TextVar wReturnGraphField;
  private Button wValidateAgainstModel;
  private Button wOutOfOrderAllowed;

  private CTabFolder wTabFolder;
  private TableView wFieldMappings;
  private TableView wRelMappings;
  private TableView wNodeMappings;

  private GraphOutputMeta input;

  private GraphModel activeModel;

  public GraphOutputDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, pipelineMeta, transformName);
    input = (GraphOutputMeta) inputMetadata;

    metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Neo4j GraphOutput");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
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

    wModel =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            GraphModel.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Graph model",
            "The name of the Neo4j logical Graph Model to use");
    props.setLook(wModel);
    FormData fdModel = new FormData();
    fdModel.left = new FormAttachment(0, 0);
    fdModel.right = new FormAttachment(100, 0);
    fdModel.top = new FormAttachment(lastControl, margin);
    wModel.setLayoutData(fdModel);
    try {
      wModel.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of models", e);
    }
    lastControl = wModel;

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
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    wlCreateIndexes = new Label(shell, SWT.RIGHT);
    wlCreateIndexes.setText("Create indexes? ");
    wlCreateIndexes.setToolTipText(
        "Create index on first row using label field and primary key properties.");
    props.setLook(wlCreateIndexes);
    FormData fdlCreateIndexes = new FormData();
    fdlCreateIndexes.left = new FormAttachment(0, 0);
    fdlCreateIndexes.right = new FormAttachment(middle, -margin);
    fdlCreateIndexes.top = new FormAttachment(lastControl, 2 * margin);
    wlCreateIndexes.setLayoutData(fdlCreateIndexes);
    wCreateIndexes = new Button(shell, SWT.CHECK | SWT.BORDER);
    wCreateIndexes.setToolTipText(
        "Create index on first row using label field and primary key properties.");
    props.setLook(wCreateIndexes);
    FormData fdCreateIndexes = new FormData();
    fdCreateIndexes.left = new FormAttachment(middle, 0);
    fdCreateIndexes.right = new FormAttachment(100, 0);
    fdCreateIndexes.top = new FormAttachment(wlCreateIndexes, 0, SWT.CENTER);
    wCreateIndexes.setLayoutData(fdCreateIndexes);
    lastControl = wCreateIndexes;

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
    FormData fdReturnGraphField = new FormData();
    fdReturnGraphField.left = new FormAttachment(middle, 0);
    fdReturnGraphField.right = new FormAttachment(100, 0);
    fdReturnGraphField.top = new FormAttachment(wlReturnGraphField, 0, SWT.CENTER);
    wReturnGraphField.setLayoutData(fdReturnGraphField);
    lastControl = wReturnGraphField;

    Label wlValidateAgainstModel = new Label(shell, SWT.RIGHT);
    wlValidateAgainstModel.setText("Validate against model?");
    wlValidateAgainstModel.setToolTipText(
        "This validates indexes, constraints and properties as specified in the model");
    props.setLook(wlValidateAgainstModel);
    FormData fdlValidateAgainstModel = new FormData();
    fdlValidateAgainstModel.left = new FormAttachment(0, 0);
    fdlValidateAgainstModel.right = new FormAttachment(middle, -margin);
    fdlValidateAgainstModel.top = new FormAttachment(lastControl, 2 * margin);
    wlValidateAgainstModel.setLayoutData(fdlValidateAgainstModel);
    wValidateAgainstModel = new Button(shell, SWT.CHECK | SWT.BORDER);
    wValidateAgainstModel.setToolTipText(returnGraphTooltipText);
    props.setLook(wValidateAgainstModel);
    FormData fdValidateAgainstModel = new FormData();
    fdValidateAgainstModel.left = new FormAttachment(middle, 0);
    fdValidateAgainstModel.right = new FormAttachment(100, 0);
    fdValidateAgainstModel.top = new FormAttachment(wlValidateAgainstModel, 0, SWT.CENTER);
    wValidateAgainstModel.setLayoutData(fdValidateAgainstModel);
    wValidateAgainstModel.addListener(SWT.Selection, e -> enableFields());
    lastControl = wlValidateAgainstModel;

    Label wlOutOfOrderAllowed = new Label(shell, SWT.RIGHT);
    wlOutOfOrderAllowed.setText("Allow out of order updates?");
    wlOutOfOrderAllowed.setToolTipText(
        "The transform can group similar cypher statements to increase performance.");
    props.setLook(wlOutOfOrderAllowed);
    FormData fdlOutOfOrderAllowed = new FormData();
    fdlOutOfOrderAllowed.left = new FormAttachment(0, 0);
    fdlOutOfOrderAllowed.right = new FormAttachment(middle, -margin);
    fdlOutOfOrderAllowed.top = new FormAttachment(lastControl, 2 * margin);
    wlOutOfOrderAllowed.setLayoutData(fdlOutOfOrderAllowed);
    wOutOfOrderAllowed = new Button(shell, SWT.CHECK | SWT.BORDER);
    wOutOfOrderAllowed.setToolTipText(returnGraphTooltipText);
    props.setLook(wOutOfOrderAllowed);
    FormData fdOutOfOrderAllowed = new FormData();
    fdOutOfOrderAllowed.left = new FormAttachment(middle, 0);
    fdOutOfOrderAllowed.right = new FormAttachment(100, 0);
    fdOutOfOrderAllowed.top = new FormAttachment(wlOutOfOrderAllowed, 0, SWT.CENTER);
    wOutOfOrderAllowed.setLayoutData(fdOutOfOrderAllowed);
    lastControl = wlOutOfOrderAllowed;

    // Some buttons at the bottom...
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // The tab folder goes between the last control and the OK button:
    //
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(lastControl, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    String[] fieldNames = getInputRowMeta().getFieldNames();

    addFieldMappingsTab(margin, fieldNames);
    addRelMappingsTab(margin, fieldNames);
    addNodeMappingsTab(fieldNames);

    // Select the first tab
    //
    wTabFolder.setSelection(0);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addFieldMappingsTab(int margin, String[] fieldNames) {
    CTabItem wPropertiesTab = new CTabItem(wTabFolder, SWT.NONE);
    wPropertiesTab.setText("Field to properties mappings  ");

    Composite wPropertiesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wPropertiesComp);
    wPropertiesComp.setLayout(new FormLayout());

    Button wMapping = new Button(wPropertiesComp, SWT.PUSH);
    wMapping.setText("Map fields");
    wMapping.addListener(SWT.Selection, e -> enterMapping());
    positionBottomButtons(wPropertiesComp, new Button[] {wMapping}, margin, null);

    // Table: field to model mapping
    //
    ColumnInfo[] parameterColumns =
        new ColumnInfo[] {
          new ColumnInfo("Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false),
          new ColumnInfo(
              "Target type", ColumnInfo.COLUMN_TYPE_CCOMBO, ModelTargetType.getNames(), false),
          new ColumnInfo("Target", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
          new ColumnInfo("Property", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
          new ColumnInfo(
              "Target hint",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ModelTargetHint.getDescriptions(),
              false),
        };

    wFieldMappings =
        new TableView(
            variables,
            wPropertiesComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            parameterColumns,
            input.getFieldModelMappings().size(),
            null,
            props);
    props.setLook(wFieldMappings);
    FormData fdFieldMappings = new FormData();
    fdFieldMappings.left = new FormAttachment(0, 0);
    fdFieldMappings.right = new FormAttachment(100, 0);
    fdFieldMappings.top = new FormAttachment(0, 0);
    fdFieldMappings.bottom = new FormAttachment(wMapping, -margin, SWT.TOP);
    wFieldMappings.setLayoutData(fdFieldMappings);

    FormData fdPropertiesComp = new FormData();
    fdPropertiesComp.left = new FormAttachment(0, 0);
    fdPropertiesComp.top = new FormAttachment(0, 0);
    fdPropertiesComp.right = new FormAttachment(100, 0);
    fdPropertiesComp.bottom = new FormAttachment(100, 0);
    wPropertiesComp.setLayoutData(fdPropertiesComp);

    wPropertiesComp.layout();
    wPropertiesTab.setControl(wPropertiesComp);
  }

  private void addRelMappingsTab(int margin, String[] fieldNames) {
    CTabItem wRelationshipsTab = new CTabItem(wTabFolder, SWT.NONE);
    wRelationshipsTab.setText("Field to relationship mappings  ");

    Composite wRelationshipsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wRelationshipsComp);
    wRelationshipsComp.setLayout(new FormLayout());

    Button wGetFields = new Button(wRelationshipsComp, SWT.PUSH);
    wGetFields.setText("Get fields");
    wGetFields.addListener(SWT.Selection, e -> enterRelationshipsMapping());
    positionBottomButtons(wRelationshipsComp, new Button[] {wGetFields}, margin, null);

    // Table: field to model mapping
    //
    ColumnInfo[] mappingColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              "Mapping type",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              RelationshipMappingType.getDescriptions(),
              false),
          new ColumnInfo("Field name", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false),
          new ColumnInfo("Field value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo(
              "Target relationship", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
          new ColumnInfo("Source node", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
          new ColumnInfo("Target node", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
        };

    wRelMappings =
        new TableView(
            variables,
            wRelationshipsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            mappingColumns,
            input.getRelationshipMappings().size(),
            null,
            props);
    props.setLook(wFieldMappings);
    wRelMappings.addModifyListener(null);
    FormData fdRelMappings = new FormData();
    fdRelMappings.left = new FormAttachment(0, 0);
    fdRelMappings.right = new FormAttachment(100, 0);
    fdRelMappings.top = new FormAttachment(0, 0);
    fdRelMappings.bottom = new FormAttachment(wGetFields, -margin, SWT.TOP);
    wRelMappings.setLayoutData(fdRelMappings);

    FormData fdRelationshipsComp = new FormData();
    fdRelationshipsComp.left = new FormAttachment(0, 0);
    fdRelationshipsComp.top = new FormAttachment(0, 0);
    fdRelationshipsComp.right = new FormAttachment(100, 0);
    fdRelationshipsComp.bottom = new FormAttachment(100, 0);
    wRelationshipsComp.setLayoutData(fdRelationshipsComp);

    wRelationshipsComp.layout();
    wRelationshipsTab.setControl(wRelationshipsComp);
  }

  private void addNodeMappingsTab(String[] fieldNames) {
    CTabItem wNodesTab = new CTabItem(wTabFolder, SWT.NONE);
    wNodesTab.setText("Node label mappings  ");

    Composite wNodesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wNodesComp);
    wNodesComp.setLayout(new FormLayout());

    // Table: field to model mapping
    //
    ColumnInfo[] mappingColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              "Mapping type",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              NodeMappingType.getDescriptions(),
              false),
          new ColumnInfo("Target node name", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
          new ColumnInfo("Field name", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false),
          new ColumnInfo("Field value", ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo("Target label", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false),
        };

    wNodeMappings =
        new TableView(
            variables,
            wNodesComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            mappingColumns,
            input.getNodeMappings().size(),
            null,
            props);
    props.setLook(wNodeMappings);
    wNodeMappings.addModifyListener(null);
    FormData fdNodeMappings = new FormData();
    fdNodeMappings.left = new FormAttachment(0, 0);
    fdNodeMappings.right = new FormAttachment(100, 0);
    fdNodeMappings.top = new FormAttachment(0, 0);
    fdNodeMappings.bottom = new FormAttachment(100, 0);
    wNodeMappings.setLayoutData(fdNodeMappings);

    FormData fdNodesComp = new FormData();
    fdNodesComp.left = new FormAttachment(0, 0);
    fdNodesComp.top = new FormAttachment(0, 0);
    fdNodesComp.right = new FormAttachment(100, 0);
    fdNodesComp.bottom = new FormAttachment(100, 0);
    wNodesComp.setLayoutData(fdNodesComp);

    wNodesComp.layout();
    wNodesTab.setControl(wNodesComp);
  }

  private void enableFields() {

    boolean toNeo = !wReturnGraph.getSelection();

    wConnection.setEnabled(toNeo);
    wlBatchSize.setEnabled(toNeo);
    wBatchSize.setEnabled(toNeo);
    wlCreateIndexes.setEnabled(toNeo);
    wCreateIndexes.setEnabled(toNeo);

    wlReturnGraphField.setEnabled(!toNeo);
    wReturnGraphField.setEnabled(!toNeo);
  }

  private void enterMapping() {
    // Map input field names to Node/Property values
    //
    try {
      if (activeModel == null) {
        if (!loadActiveModel()) {
          return;
        }
      }

      // Input fields
      //
      IRowMeta inputRowMeta = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      String[] inputFields = inputRowMeta.getFieldNames();

      // Node properties
      //
      String separator = " . ";
      List<String> targetPropertiesList = new ArrayList<>();
      for (GraphNode node : activeModel.getNodes()) {
        for (GraphProperty property : node.getProperties()) {
          String combo = node.getName() + " . " + property.getName();
          targetPropertiesList.add(combo);
        }
      }
      for (GraphRelationship relationship : activeModel.getRelationships()) {
        for (GraphProperty property : relationship.getProperties()) {
          String combo = relationship.getName() + " . " + property.getName();
          targetPropertiesList.add(combo);
        }
      }

      String[] targetProperties = targetPropertiesList.toArray(new String[0]);

      // Preserve mappings...
      //
      List<SourceToTargetMapping> mappings = new ArrayList<>();
      for (int i = 0; i < wFieldMappings.nrNonEmpty(); i++) {
        TableItem item = wFieldMappings.getNonEmpty(i);
        int sourceIndex = Const.indexOfString(item.getText(1), inputFields);
        int targetIndex =
            Const.indexOfString(item.getText(3) + separator + item.getText(4), targetProperties);
        if (sourceIndex >= 0 && targetIndex >= 0) {
          mappings.add(new SourceToTargetMapping(sourceIndex, targetIndex));
        }
      }

      EnterMappingDialog dialog =
          new EnterMappingDialog(shell, inputFields, targetProperties, mappings);
      mappings = dialog.open();
      if (mappings != null) {
        wFieldMappings.clearAll();
        for (SourceToTargetMapping mapping : mappings) {
          String field = mapping.getSourceString(inputFields);
          String target = mapping.getTargetString(targetProperties);
          int index = target.indexOf(separator);
          String targetName = target.substring(0, index);
          String property = target.substring(index + separator.length());

          String targetType = null;
          if (activeModel.findNode(targetName) != null) {
            targetType = "Node";
          } else if (activeModel.findRelationship(targetName) != null) {
            targetType = "Relationship";
          } else {
            throw new HopException(
                "Neither node nor pipeline found for target '" + targetName + ": internal error");
          }

          wFieldMappings.add(field, targetType, targetName, property);
        }
        wFieldMappings.removeEmptyRows();
        wFieldMappings.setRowNums();
        wFieldMappings.optWidth(true);
      }

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error mapping input fields to node properties", e);
    }
  }

  private boolean loadActiveModel() throws HopException {
    try {
      if (StringUtils.isEmpty(wModel.getText())) {
        return false;
      }
      IHopMetadataSerializer<GraphModel> modelSerializer =
          metadataProvider.getSerializer(GraphModel.class);
      activeModel = modelSerializer.load(wModel.getText());

      // Update the combo values in the table views..
      //
      List<String> nodeNamesList = new ArrayList(Arrays.asList(activeModel.getNodeNames()));
      String[] nodeNames = nodeNamesList.toArray(new String[0]);
      List<String> relNamesList = new ArrayList(Arrays.asList(activeModel.getRelationshipNames()));
      String[] relNames = relNamesList.toArray(new String[0]);
      List<String> allNamesList = new ArrayList<>(nodeNamesList);
      allNamesList.addAll(relNamesList);
      String[] allNames = allNamesList.toArray(new String[0]);

      Set<String> allLabelsSet = new HashSet<>();
      for (GraphNode node : activeModel.getNodes()) {
        allLabelsSet.addAll(node.getLabels());
      }
      List<String> allLabelsList = new ArrayList<>(allLabelsSet);
      Collections.sort(allLabelsList);
      String[] allLabels = allLabelsList.toArray(new String[0]);
      wFieldMappings.getColumns()[2].setComboValues(allNames);
      wRelMappings.getColumns()[3].setComboValues(relNames);
      wRelMappings.getColumns()[4].setComboValues(nodeNames);
      wRelMappings.getColumns()[5].setComboValues(nodeNames);
      wNodeMappings.getColumns()[1].setComboValues(nodeNames);
      wNodeMappings.getColumns()[4].setComboValues(allLabels);

      return true;
    } catch (Exception e) {
      throw new HopException("Error loading graph model", e);
    }
  }

  private void enterRelationshipsMapping() {
    // Populate rows and set the column values in the table view for the relationships found in the
    // model
    //
    try {
      if (!loadActiveModel()) {
        return;
      }

      String[] relationshipNames = activeModel.getRelationshipNames();
      wRelMappings.getColumns()[2].setComboValues(relationshipNames);

      // Input fields
      //
      IRowMeta inputRowMeta = pipelineMeta.getPrevTransformFields(variables, transformMeta);

      for (IValueMeta valueMeta : inputRowMeta.getValueMetaList()) {
        wRelMappings.add(valueMeta.getName());
      }

      wRelMappings.optimizeTableView();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error listing fields or loading graph model", e);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {

    wTransformName.setText(Const.NVL(transformName, ""));
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    try {
      wConnection.fillItems();
    } catch (HopException e) {
      log.logError("Error getting list of Neo4j Connection names", e);
    }

    wModel.setText(Const.NVL(input.getModel(), ""));
    try {
      wModel.fillItems();
    } catch (HopException e) {
      log.logError("Error getting list of Neo4j Graph Model names", e);
    }

    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wCreateIndexes.setSelection(input.isCreatingIndexes());

    {
      for (int i = 0; i < input.getFieldModelMappings().size(); i++) {
        FieldModelMapping mapping = input.getFieldModelMappings().get(i);
        TableItem item = wFieldMappings.table.getItem(i);
        int idx = 1;
        item.setText(idx++, Const.NVL(mapping.getField(), ""));
        item.setText(idx++, ModelTargetType.getCode(mapping.getTargetType()));
        item.setText(idx++, Const.NVL(mapping.getTargetName(), ""));
        item.setText(idx++, Const.NVL(mapping.getTargetProperty(), ""));
        item.setText(
            idx++, mapping.getTargetHint() == null ? "" : mapping.getTargetHint().getDescription());
      }
      wFieldMappings.optimizeTableView();
    }

    {
      for (int i = 0; i < input.getRelationshipMappings().size(); i++) {
        RelationshipMapping mapping = input.getRelationshipMappings().get(i);
        TableItem item = wRelMappings.table.getItem(i);
        int idx = 1;
        item.setText(idx++, mapping.getType() == null ? "" : mapping.getType().getDescription());
        item.setText(idx++, Const.NVL(mapping.getFieldName(), ""));
        item.setText(idx++, Const.NVL(mapping.getFieldValue(), ""));
        item.setText(idx++, Const.NVL(mapping.getTargetRelationship(), ""));
        item.setText(idx++, Const.NVL(mapping.getSourceNode(), ""));
        item.setText(idx++, Const.NVL(mapping.getTargetNode(), ""));
      }
      wRelMappings.optimizeTableView();
    }

    {
      for (int i = 0; i < input.getNodeMappings().size(); i++) {
        NodeMapping mapping = input.getNodeMappings().get(i);
        TableItem item = wNodeMappings.table.getItem(i);
        int idx = 1;
        item.setText(idx++, mapping.getType() == null ? "" : mapping.getType().getDescription());
        item.setText(idx++, Const.NVL(mapping.getTargetNode(), ""));
        item.setText(idx++, Const.NVL(mapping.getFieldName(), ""));
        item.setText(idx++, Const.NVL(mapping.getFieldValue(), ""));
        item.setText(idx++, Const.NVL(mapping.getTargetLabel(), ""));
      }
      wNodeMappings.optimizeTableView();
    }

    wReturnGraph.setSelection(input.isReturningGraph());
    wReturnGraphField.setText(Const.NVL(input.getReturnGraphField(), ""));

    wValidateAgainstModel.setSelection(input.isValidatingAgainstModel());
    wOutOfOrderAllowed.setSelection(input.isOutOfOrderAllowed());

    try {
      loadActiveModel();
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error loading specified graph model", e);
    }

    enableFields();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    input.setConnectionName(wConnection.getText());
    input.setBatchSize(wBatchSize.getText());
    input.setCreatingIndexes(wCreateIndexes.getSelection());
    input.setModel(wModel.getText());

    input.setReturningGraph(wReturnGraph.getSelection());
    input.setReturnGraphField(wReturnGraphField.getText());

    input.setValidatingAgainstModel(wValidateAgainstModel.getSelection());
    input.setOutOfOrderAllowed(wOutOfOrderAllowed.getSelection());

    List<FieldModelMapping> mappings = new ArrayList<>();
    for (TableItem item : wFieldMappings.getNonEmptyItems()) {
      int idx = 1;
      String sourceField = item.getText(idx++);
      ModelTargetType targetType = ModelTargetType.parseCode(item.getText(idx++));
      String targetName = item.getText(idx++);
      String targetProperty = item.getText(idx++);
      ModelTargetHint targetHint = ModelTargetHint.getTypeFromDescription(item.getText(idx++));

      mappings.add(
          new FieldModelMapping(sourceField, targetType, targetName, targetProperty, targetHint));
    }
    input.setFieldModelMappings(mappings);

    {
      List<RelationshipMapping> relMappings = new ArrayList<>();
      for (TableItem item : wRelMappings.getNonEmptyItems()) {
        int idx = 1;
        RelationshipMappingType mappingType =
            RelationshipMappingType.getTypeFromDescription(item.getText(idx++));
        String sourceFieldName = item.getText(idx++);
        String sourceFieldValue = item.getText(idx++);
        String targetRelationship = item.getText(idx++);
        String sourceNode = item.getText(idx++);
        String targetNode = item.getText(idx++);

        relMappings.add(
            new RelationshipMapping(
                mappingType,
                sourceFieldName,
                sourceFieldValue,
                targetRelationship,
                sourceNode,
                targetNode));
      }
      input.setRelationshipMappings(relMappings);
    }

    {
      List<NodeMapping> nodeMappings = new ArrayList<>();
      for (TableItem item : wNodeMappings.getNonEmptyItems()) {
        int idx = 1;
        NodeMappingType mappingType = NodeMappingType.getTypeFromDescription(item.getText(idx++));
        String targetNodeName = item.getText(idx++);
        String sourceFieldName = item.getText(idx++);
        String sourceFieldValue = item.getText(idx++);
        String targetLabel = item.getText(idx++);

        nodeMappings.add(
            new NodeMapping(
                mappingType, targetNodeName, sourceFieldName, sourceFieldValue, targetLabel));
      }
      input.setNodeMappings(nodeMappings);
    }

    input.setChanged();

    dispose();
  }

  private IRowMeta getInputRowMeta() {
    IRowMeta inputRowMeta;
    try {
      inputRowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopTransformException e) {
      LogChannel.GENERAL.logError("Unable to find transform input field", e);
      inputRowMeta = new RowMeta();
    }
    return inputRowMeta;
  }
}
