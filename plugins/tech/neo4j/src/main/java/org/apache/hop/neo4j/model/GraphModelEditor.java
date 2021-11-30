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

package org.apache.hop.neo4j.model;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.actions.constraint.ConstraintType;
import org.apache.hop.neo4j.actions.constraint.ConstraintUpdate;
import org.apache.hop.neo4j.actions.constraint.Neo4jConstraint;
import org.apache.hop.neo4j.actions.index.IndexUpdate;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.actions.index.ObjectType;
import org.apache.hop.neo4j.actions.index.UpdateType;
import org.apache.hop.neo4j.core.Neo4jUtil;
import org.apache.hop.neo4j.model.arrows.ArrowsAppImporter;
import org.apache.hop.neo4j.model.sw.SolutionsWorkbenchImporter;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterListDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.*;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.*;

import java.awt.geom.Line2D;
import java.util.*;

public class GraphModelEditor extends MetadataEditor<GraphModel> {

  private static Class<?> PKG =
      GraphModelEditor.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabs;

  private PropsUi props;

  // Model fields
  private Text wModelDescription;
  private Text wModelName;

  // Node fields
  private List wNodesList;
  private Text wNodeName;
  private Text wNodeDescription;
  private TableView wNodeLabels;
  private boolean monitorLabels = true;
  private TableView wNodeProperties;
  private boolean monitorNodeProperties = true;

  // Relationship fields
  private List wRelationshipsList;
  private Text wRelName;
  private Text wRelDescription;
  private Text wRelLabel;
  private CCombo wRelSource;
  private CCombo wRelTarget;
  private TableView wRelProperties;
  private boolean monitorRelProperties = true;

  private GraphModel graphModel;
  private GraphNode activeNode;
  private GraphRelationship activeRelationship;
  private Button wImportNode;

  private Point mouseDownPoint;
  private Canvas wCanvas;
  private Label wlNodeName;
  private Label wlNodeDescription;
  private Label wlNodeProperties;
  private Label wlRelName;
  private Label wlRelDescription;
  private Label wlRelLabel;
  private Label wlRelSource;
  private Label wlRelTarget;
  private Label wlRelProperties;
  private int middle;
  private int margin;

  public GraphModelEditor(HopGui hopGui, MetadataManager<GraphModel> manager, GraphModel metadata) {
    super(hopGui, manager, metadata);

    props = PropsUi.getInstance();

    // We will change graphModel, copy it first
    //
    this.graphModel = new GraphModel(metadata);

    mouseDownPoint = new Point(-1, -1);
  }

  @Override
  public void createControl(Composite composite) {
    // Add a tab folder
    //
    wTabs = new CTabFolder(composite, SWT.BORDER);
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment(0, 0);
    fdTabs.right = new FormAttachment(100, 0);
    fdTabs.top = new FormAttachment(0, 0);
    fdTabs.bottom = new FormAttachment(100, 0);
    wTabs.setLayoutData(fdTabs);

    addModelTab();
    addNodesTab();
    addRelationshipsTab();
    addGraphTab();

    setWidgetsContent();

    clearChanged();

    // Select the model tab
    //
    wTabs.setSelection(0);
  }

  @Override
  public void setWidgetsContent() {
    // Model tab
    wModelName.setText(Const.NVL(graphModel.getName(), ""));
    wModelDescription.setText(Const.NVL(graphModel.getDescription(), ""));

    refreshNodesList();
    if (graphModel.getNodes().size() > 0) {
      String activeName = graphModel.getNodeNames()[0];
      setActiveNode(activeName);
      wNodesList.setSelection(new String[] {activeName});
      refreshNodeFields();
    }
    refreshRelationshipsList();
    if (graphModel.getRelationships().size() > 0) {
      String activeRelationshipName = graphModel.getRelationshipNames()[0];
      setActiveRelationship(activeRelationshipName);
      wRelationshipsList.setSelection(new String[] {activeRelationshipName});
      refreshRelationshipsFields();
    }

    enableFields();
  }

  @Override
  public void getWidgetsContent(GraphModel gm) {
    gm.replace(graphModel);
  }

  private void enableFields() {
    boolean nodeSelected = activeNode != null;

    wlNodeName.setEnabled(nodeSelected);
    wNodeName.setEnabled(nodeSelected);
    wlNodeDescription.setEnabled(nodeSelected);
    wNodeDescription.setEnabled(nodeSelected);
    wNodeLabels.setEnabled(nodeSelected);
    wlNodeProperties.setEnabled(nodeSelected);
    wNodeProperties.setEnabled(nodeSelected);

    boolean relationshipSelected = activeRelationship != null;

    wlRelName.setEnabled(relationshipSelected);
    wRelName.setEnabled(relationshipSelected);
    wlRelDescription.setEnabled(relationshipSelected);
    wRelDescription.setEnabled(relationshipSelected);
    wlRelLabel.setEnabled(relationshipSelected);
    wRelLabel.setEnabled(relationshipSelected);
    wlRelSource.setEnabled(relationshipSelected);
    wRelSource.setEnabled(relationshipSelected);
    wlRelTarget.setEnabled(relationshipSelected);
    wRelTarget.setEnabled(relationshipSelected);
    wlRelProperties.setEnabled(relationshipSelected);
    wRelProperties.setEnabled(relationshipSelected);
  }

  private void setActiveNode(String nodeName) {
    activeNode = graphModel.findNode(nodeName);

    enableFields();
  }

  private void setActiveRelationship(String relationshipName) {
    activeRelationship = graphModel.findRelationship(relationshipName);

    enableFields();
  }

  private void refreshNodeFields() {
    if (activeNode == null) {
      return;
    }

    wNodeName.setText(Const.NVL(activeNode.getName(), ""));
    wNodeDescription.setText(Const.NVL(activeNode.getDescription(), ""));

    // Refresh the Labels
    //
    monitorLabels = false;
    wNodeLabels.clearAll();
    for (int i = 0; i < activeNode.getLabels().size(); i++) {
      TableItem item = new TableItem(wNodeLabels.table, SWT.NONE);
      item.setText(1, activeNode.getLabels().get(i));
    }
    wNodeLabels.removeEmptyRows(1);
    wNodeLabels.setRowNums();
    wNodeLabels.optWidth(true);
    monitorLabels = true;

    // Refresh the Properties
    //
    monitorNodeProperties = false;
    // System.out.println( "Adding " + activeNode.getProperties().size() + " properties to the node
    // view" );
    wNodeProperties.clearAll();
    for (int i = 0; i < activeNode.getProperties().size(); i++) {
      GraphProperty property = activeNode.getProperties().get(i);
      TableItem item = new TableItem(wNodeProperties.table, SWT.NONE);
      int col = 1;
      item.setText(col++, Const.NVL(property.getName(), ""));
      item.setText(col++, GraphPropertyType.getCode(property.getType()));
      item.setText(col++, Const.NVL(property.getDescription(), ""));
      item.setText(col++, property.isPrimary() ? "Y" : "N");
      item.setText(col++, property.isMandatory() ? "Y" : "N");
      item.setText(col++, property.isUnique() ? "Y" : "N");
      item.setText(col++, property.isIndexed() ? "Y" : "N");
    }
    wNodeProperties.removeEmptyRows(1);
    wNodeProperties.setRowNums();
    wNodeProperties.optWidth(true);

    wNodeName.setFocus();
    monitorNodeProperties = true;
  }

  private void refreshRelationshipsFields() {
    if (activeRelationship == null) {
      return;
    }

    wRelName.setText(Const.NVL(activeRelationship.getName(), ""));
    wRelDescription.setText(Const.NVL(activeRelationship.getDescription(), ""));
    wRelLabel.setText(Const.NVL(activeRelationship.getLabel(), ""));
    wRelSource.setText(Const.NVL(activeRelationship.getNodeSource(), ""));
    wRelTarget.setText(Const.NVL(activeRelationship.getNodeTarget(), ""));

    // Refresh the Properties
    //
    monitorRelProperties = false;
    // System.out.println( "Adding " + activeRelationship.getProperties().size() + " properties to
    // the relationship view" );
    wRelProperties.clearAll();
    for (int i = 0; i < activeRelationship.getProperties().size(); i++) {
      GraphProperty property = activeRelationship.getProperties().get(i);
      TableItem item = new TableItem(wRelProperties.table, SWT.NONE);
      int col = 1;
      item.setText(col++, Const.NVL(property.getName(), ""));
      item.setText(col++, GraphPropertyType.getCode(property.getType()));
      item.setText(col++, Const.NVL(property.getDescription(), ""));
      item.setText(col++, property.isPrimary() ? "Y" : "N");
      item.setText(col++, property.isMandatory() ? "Y" : "N");
      item.setText(col++, property.isUnique() ? "Y" : "N");
      item.setText(col++, property.isIndexed() ? "Y" : "N");
    }
    wRelProperties.removeEmptyRows(1);
    wRelProperties.setRowNums();
    wRelProperties.optWidth(true);

    wRelName.setFocus();

    monitorRelProperties = true;
  }

  private void refreshNodesList() {
    String[] nodeNames = graphModel.getNodeNames();
    wNodesList.setItems(nodeNames);
    if (activeNode != null) {
      wNodesList.setSelection(
          new String[] {
            activeNode.getName(),
          });
    }
    wRelSource.setItems(nodeNames);
    wRelTarget.setItems(nodeNames);
  }

  private void refreshRelationshipsList() {
    // Relationships tab
    //
    wRelationshipsList.setItems(graphModel.getRelationshipNames());
  }

  private void addModelTab() {

    CTabItem wModelTab = new CTabItem(wTabs, SWT.NONE);
    wModelTab.setText("Model");

    ScrolledComposite wModelSComp = new ScrolledComposite(wTabs, SWT.V_SCROLL | SWT.H_SCROLL);
    wModelSComp.setLayout(new FillLayout());

    Composite wModelComp = new Composite(wModelSComp, SWT.NONE);
    props.setLook(wModelComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wModelComp.setLayout(formLayout);

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Model properties
    //  - Name
    //  - Description
    //
    Label wlName = new Label(wModelComp, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "GraphModelDialog.Name.Label"));
    props.setLook(wlName);
    FormData fdlModelName = new FormData();
    fdlModelName.left = new FormAttachment(0, 0);
    fdlModelName.right = new FormAttachment(middle, 0);
    fdlModelName.top = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlModelName);
    wModelName = new Text(wModelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wModelName);
    FormData fdModelName = new FormData();
    fdModelName.left = new FormAttachment(middle, margin);
    fdModelName.right = new FormAttachment(100, 0);
    fdModelName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    wModelName.setLayoutData(fdModelName);
    wModelName.addModifyListener(
        e -> {
          setChanged();
          graphModel.setName(wModelName.getText());
        });
    Control lastControl = wModelName;

    Label wlModelDescription = new Label(wModelComp, SWT.RIGHT);
    wlModelDescription.setText(BaseMessages.getString(PKG, "GraphModelDialog.Description.Label"));
    props.setLook(wlModelDescription);
    FormData fdlModelDescription = new FormData();
    fdlModelDescription.left = new FormAttachment(0, 0);
    fdlModelDescription.right = new FormAttachment(middle, 0);
    fdlModelDescription.top = new FormAttachment(lastControl, margin);
    wlModelDescription.setLayoutData(fdlModelDescription);
    wModelDescription = new Text(wModelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wModelDescription);
    FormData fdModelDescription = new FormData();
    fdModelDescription.left = new FormAttachment(middle, margin);
    fdModelDescription.right = new FormAttachment(100, 0);
    fdModelDescription.top = new FormAttachment(wlModelDescription, 0, SWT.CENTER);
    wModelDescription.setLayoutData(fdModelDescription);
    wModelDescription.addModifyListener(
        e -> {
          setChanged();
          graphModel.setDescription(wModelDescription.getText());
        });
    lastControl = wModelDescription;

    Button wImportGraph = new Button(wModelComp, SWT.PUSH);
    wImportGraph.setText(BaseMessages.getString(PKG, "GraphModelDialog.ImportGraph.Button"));
    props.setLook(wImportGraph);
    FormData fdImportGraph = new FormData();
    fdImportGraph.left = new FormAttachment(middle, 0);
    fdImportGraph.right = new FormAttachment(75, 0);
    fdImportGraph.top = new FormAttachment(lastControl, 50);
    wImportGraph.setLayoutData(fdImportGraph);
    wImportGraph.addListener(SWT.Selection, (e) -> importGraphFromFile());
    lastControl = wImportGraph;

    Button wExportGraph = new Button(wModelComp, SWT.PUSH);
    wExportGraph.setText(BaseMessages.getString(PKG, "GraphModelDialog.ExportGraph.Button"));
    props.setLook(wExportGraph);
    FormData fdExportGraph = new FormData();
    fdExportGraph.left = new FormAttachment(middle, 0);
    fdExportGraph.right = new FormAttachment(75, 0);
    fdExportGraph.top = new FormAttachment(lastControl, margin);
    wExportGraph.setLayoutData(fdExportGraph);
    wExportGraph.addListener(SWT.Selection, (e) -> exportGraphToFile());
    lastControl = wExportGraph;

    Button wSolutionsWorkbenchImportGraph = new Button(wModelComp, SWT.PUSH);
    wSolutionsWorkbenchImportGraph.setText(
        BaseMessages.getString(PKG, "GraphModelDialog.ImportGraphSW.Button"));
    props.setLook(wSolutionsWorkbenchImportGraph);
    FormData fdSolutionsWorkbenchImportGraph = new FormData();
    fdSolutionsWorkbenchImportGraph.left = new FormAttachment(middle, 0);
    fdSolutionsWorkbenchImportGraph.right = new FormAttachment(75, 0);
    fdSolutionsWorkbenchImportGraph.top = new FormAttachment(lastControl, 50);
    wSolutionsWorkbenchImportGraph.setLayoutData(fdSolutionsWorkbenchImportGraph);
    wSolutionsWorkbenchImportGraph.addListener(
        SWT.Selection, (e) -> importGraphFromSolutionsWorkbench());
    lastControl = wSolutionsWorkbenchImportGraph;

    Button wArrowsAppImportGraph = new Button(wModelComp, SWT.PUSH);
    wArrowsAppImportGraph.setText(
        BaseMessages.getString(PKG, "GraphModelDialog.ImportArrowsApp.Button"));
    props.setLook(wArrowsAppImportGraph);
    FormData fdArrowsAppImportGraph = new FormData();
    fdArrowsAppImportGraph.left = new FormAttachment(middle, 0);
    fdArrowsAppImportGraph.right = new FormAttachment(75, 0);
    fdArrowsAppImportGraph.top = new FormAttachment(lastControl, margin);
    wArrowsAppImportGraph.setLayoutData(fdArrowsAppImportGraph);
    wArrowsAppImportGraph.addListener(SWT.Selection, (e) -> importGraphFromArrowsApp());
    lastControl = wArrowsAppImportGraph;

    Button wCreateIndexAction = new Button(wModelComp, SWT.PUSH);
    wCreateIndexAction.setText(
        BaseMessages.getString(PKG, "GraphModelDialog.CreateIndexAction.Button"));
    props.setLook(wCreateIndexAction);
    FormData fdCreateIndexAction = new FormData();
    fdCreateIndexAction.left = new FormAttachment(middle, 0);
    fdCreateIndexAction.right = new FormAttachment(75, 0);
    fdCreateIndexAction.top = new FormAttachment(lastControl, 50);
    wCreateIndexAction.setLayoutData(fdCreateIndexAction);
    wCreateIndexAction.addListener(SWT.Selection, (e) -> copyIndexActionToClipboard());

    FormData fdModelComp = new FormData();
    fdModelComp.left = new FormAttachment(0, 0);
    fdModelComp.top = new FormAttachment(0, 0);
    fdModelComp.right = new FormAttachment(100, 0);
    fdModelComp.bottom = new FormAttachment(100, 0);
    wModelComp.setLayoutData(fdModelComp);

    wModelComp.pack();

    Rectangle bounds = wModelComp.getBounds();

    wModelSComp.setContent(wModelComp);
    wModelSComp.setExpandHorizontal(true);
    wModelSComp.setExpandVertical(true);
    wModelSComp.setMinWidth(bounds.width);
    wModelSComp.setMinHeight(bounds.height);

    wModelTab.setControl(wModelSComp);
  }

  private void addNodesTab() {

    CTabItem wNodesTab = new CTabItem(wTabs, SWT.NONE);
    wNodesTab.setText("Nodes");

    ScrolledComposite wNodesSComp = new ScrolledComposite(wTabs, SWT.V_SCROLL | SWT.H_SCROLL);
    wNodesSComp.setLayout(new FillLayout());

    Composite wNodesComp = new Composite(wNodesSComp, SWT.NONE);
    props.setLook(wNodesComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    wNodesComp.setLayout(formLayout);

    // Nodes properties
    //  - Nodes List
    //  - Node name
    //  - Node description
    //  - Node labels
    //  - Node properties list
    //

    // buttons for New/Edit/Delete Node
    //
    Button wNewNode = new Button(wNodesComp, SWT.PUSH);
    wNewNode.setText("New node");
    wNewNode.addListener(SWT.Selection, (e) -> newNode());

    Button wDeleteNode = new Button(wNodesComp, SWT.PUSH);
    wDeleteNode.setText("Delete node");
    wDeleteNode.addListener(SWT.Selection, (e) -> deleteNode());

    Button wCopyNode = new Button(wNodesComp, SWT.PUSH);
    wCopyNode.setText("Copy node");
    wCopyNode.addListener(SWT.Selection, (e) -> copyNode());

    wImportNode = new Button(wNodesComp, SWT.PUSH);
    wImportNode.setText("Import properties");
    wImportNode.addListener(SWT.Selection, (e) -> importNodeProperties());

    Button wNewRelationshipNode = new Button(wNodesComp, SWT.PUSH);
    wNewRelationshipNode.setText("New relationship");
    wNewRelationshipNode.addListener(SWT.Selection, (e) -> newRelationshipFromNode());

    BaseTransformDialog.positionBottomButtons(
        wNodesComp,
        new Button[] {wNewNode, wDeleteNode, wCopyNode, wImportNode, wNewRelationshipNode},
        margin,
        null);

    Label wlNodesList = new Label(wNodesComp, SWT.LEFT);
    wlNodesList.setText("Nodes list");
    props.setLook(wlNodesList);
    FormData fdlNodesList = new FormData();
    fdlNodesList.left = new FormAttachment(0, 0);
    fdlNodesList.right = new FormAttachment(middle, 0);
    fdlNodesList.top = new FormAttachment(0, 0);
    wlNodesList.setLayoutData(fdlNodesList);
    wNodesList = new List(wNodesComp, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER);
    props.setLook(wNodesList);
    FormData fdNodesList = new FormData();
    fdNodesList.left = new FormAttachment(0, 0);
    fdNodesList.right = new FormAttachment(middle, 0);
    fdNodesList.top = new FormAttachment(wlNodesList, margin);
    fdNodesList.bottom = new FormAttachment(wNewNode, -margin * 2);
    wNodesList.setLayoutData(fdNodesList);
    wNodesList.addListener(
        SWT.Selection,
        event -> {
          getNodeLabelsFromView();
          setActiveNode(wNodesList.getSelection()[0]);
          refreshNodeFields();
        });

    wlNodeName = new Label(wNodesComp, SWT.RIGHT);
    wlNodeName.setText("Name");
    props.setLook(wlNodeName);
    FormData fdlNodeName = new FormData();
    fdlNodeName.left = new FormAttachment(middle, margin);
    fdlNodeName.top = new FormAttachment(wlNodesList, margin * 2);
    wlNodeName.setLayoutData(fdlNodeName);
    wNodeName = new Text(wNodesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNodeName.addListener(
        SWT.Modify,
        event -> {
          if (activeNode != null) {
            String nodeName = wNodeName.getText();
            activeNode.setName(nodeName);
            String[] nodeNames = graphModel.getNodeNames();
            wNodesList.setItems(nodeNames);
            wNodesList.setSelection(Const.indexOfString(nodeName, nodeNames));
            setChanged();
          }
        });
    props.setLook(wNodeName);
    FormData fdNodeName = new FormData();
    fdNodeName.left = new FormAttachment(wlNodeName, margin * 2);
    fdNodeName.right = new FormAttachment(100, 0);
    fdNodeName.top = new FormAttachment(wlNodeName, 0, SWT.CENTER);
    wNodeName.setLayoutData(fdNodeName);

    wlNodeDescription = new Label(wNodesComp, SWT.RIGHT);
    wlNodeDescription.setText("Description");
    props.setLook(wlNodeDescription);
    FormData fdlNodeDescription = new FormData();
    fdlNodeDescription.left = new FormAttachment(middle, margin);
    fdlNodeDescription.top = new FormAttachment(wNodeName, margin);
    wlNodeDescription.setLayoutData(fdlNodeDescription);
    wNodeDescription = new Text(wNodesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNodeDescription.addListener(
        SWT.Modify,
        event -> {
          if (activeNode != null) {
            activeNode.setDescription(wNodeDescription.getText());
            setChanged();
          }
        });
    props.setLook(wNodeDescription);
    FormData fdNodeDescription = new FormData();
    fdNodeDescription.left = new FormAttachment(wlNodeDescription, margin * 2);
    fdNodeDescription.right = new FormAttachment(100, 0);
    fdNodeDescription.top = new FormAttachment(wlNodeDescription, 0, SWT.CENTER);
    wNodeDescription.setLayoutData(fdNodeDescription);

    // Labels
    //
    ColumnInfo[] labelColumns =
        new ColumnInfo[] {
          new ColumnInfo("Labels", ColumnInfo.COLUMN_TYPE_TEXT, false),
        };
    ModifyListener labelModifyListener = modifyEvent -> getNodeLabelsFromView();
    wNodeLabels =
        new TableView(
            new Variables(),
            wNodesComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            labelColumns,
            1,
            labelModifyListener,
            props);
    props.setLook(wNodeLabels);
    FormData fdNodeLabels = new FormData();
    fdNodeLabels.left = new FormAttachment(middle, margin);
    fdNodeLabels.right = new FormAttachment(100, 0);
    fdNodeLabels.top = new FormAttachment(wNodeDescription, margin);
    fdNodeLabels.bottom = new FormAttachment(wNodeDescription, 250 + margin);
    wNodeLabels.setLayoutData(fdNodeLabels);

    // Properties
    //
    wlNodeProperties = new Label(wNodesComp, SWT.LEFT);
    wlNodeProperties.setText("Properties:");
    props.setLook(wlNodeProperties);
    FormData fdlNodeProperties = new FormData();
    fdlNodeProperties.left = new FormAttachment(middle, margin);
    fdlNodeProperties.top = new FormAttachment(wNodeLabels, margin);
    wlNodeProperties.setLayoutData(fdlNodeProperties);

    ColumnInfo[] propertyColumns =
        new ColumnInfo[] {
          new ColumnInfo("Property key", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              "Property Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false),
          new ColumnInfo("Description", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Primary?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo(
              "Mandatory?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo("Unique?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo("Indexed?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
        };
    ModifyListener propertyModifyListener = modifyEvent -> getNodePropertiesFromView();
    wNodeProperties =
        new TableView(
            new Variables(),
            wNodesComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            propertyColumns,
            1,
            propertyModifyListener,
            props);
    wNodeProperties.table.addListener(SWT.FocusOut, event -> getNodePropertiesFromView());
    props.setLook(wNodeProperties);
    FormData fdNodeProperties = new FormData();
    fdNodeProperties.left = new FormAttachment(middle, margin);
    fdNodeProperties.right = new FormAttachment(100, 0);
    fdNodeProperties.top = new FormAttachment(wlNodeProperties, margin);
    fdNodeProperties.bottom = new FormAttachment(wNewNode, -2 * margin);
    wNodeProperties.setLayoutData(fdNodeProperties);

    FormData fdNodesComp = new FormData();
    fdNodesComp.left = new FormAttachment(0, 0);
    fdNodesComp.top = new FormAttachment(0, 0);
    fdNodesComp.right = new FormAttachment(100, 0);
    fdNodesComp.bottom = new FormAttachment(100, 0);
    wNodesComp.setLayoutData(fdNodesComp);

    wNodesComp.pack();

    Rectangle bounds = wNodesComp.getBounds();

    wNodesSComp.setContent(wNodesComp);
    wNodesSComp.setExpandHorizontal(true);
    wNodesSComp.setExpandVertical(true);
    wNodesSComp.setMinWidth(bounds.width);
    wNodesSComp.setMinHeight(bounds.height);

    wNodesTab.setControl(wNodesSComp);
  }

  /** Use the selected node as the source of the relationship */
  private void newRelationshipFromNode() {
    // You need to have a selected node
    //
    if (activeNode == null) {
      return;
    }

    // You need more than 1 node
    //
    if (graphModel.getNodes().size() < 2) {
      return;
    }

    String[] nodeNames = new String[graphModel.getNodes().size() - 1];
    int index = 0;
    // Ignore the active node in the list
    for (GraphNode graphNode : graphModel.getNodes()) {
      if (!graphNode.equals(activeNode)) {
        nodeNames[index++] = graphNode.getName();
      }
    }
    Arrays.sort(nodeNames);

    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            hopGui.getShell(),
            nodeNames,
            "Select 2nd node",
            "Select the second node for the new relationship");
    dialog.setMulti(false);
    String targetNodeName = dialog.open();
    if (targetNodeName == null) {
      return;
    }

    // Save current relationship view
    //
    getRelationshipPropertiesFromView();

    // New relationship
    //
    String relBaseName = activeNode.getName() + " - " + targetNodeName;

    String relName = relBaseName;
    int relNr = 2;
    while (graphModel.findRelationship(relName) != null) {
      relName = relBaseName + " " + relNr++;
    }
    GraphRelationship rel =
        new GraphRelationship(
            relName, "", "", new ArrayList<>(), activeNode.getName(), targetNodeName);

    graphModel.getRelationships().add(rel);

    setActiveRelationship(relName);
    refreshRelationshipsList();
    wRelationshipsList.setSelection(
        Const.indexOfString(relName, graphModel.getRelationshipNames()));
    refreshRelationshipsFields();
  }

  /** Someone changed something in the labels, update the active node */
  private void getNodeLabelsFromView() {

    if (activeNode != null) {
      setChanged();
      if (monitorLabels) {
        // System.out.println( "Labels changed! " + new Date().getTime() + " found " +
        // wNodeLabels.nrNonEmpty() + " labels" );

        java.util.List<String> labels = new ArrayList<>();
        for (int i = 0; i < wNodeLabels.nrNonEmpty(); i++) {
          labels.add(wNodeLabels.getNonEmpty(i).getText(1));
        }

        TableEditor editor = wNodeLabels.getEditor();
        if (editor != null && editor.getEditor() != null && (editor.getEditor() instanceof Text)) {
          Text text = (Text) editor.getEditor();
          if (!text.isDisposed()) {
            if (!labels.contains(text.getText())) {
              labels.add(text.getText());
            }
            // System.out.println( "editor content : " + text.getText() );
          }
        }

        activeNode.setLabels(labels);
        // System.out.println( "Set " + activeNode.getLabels().size() + " labels on active node" );
      }
    }
  }

  /** Someone changed something in the properties, update the active node */
  private void getNodePropertiesFromView() {

    if (activeNode != null) {
      setChanged();
      if (monitorNodeProperties) {
        // System.out.println( "Labels changed! " + new Date().getTime() + " found " +
        // wNodeProperties.nrNonEmpty() + " properties" );

        java.util.List<GraphProperty> properties = new ArrayList<>();
        for (int i = 0; i < wNodeProperties.nrNonEmpty(); i++) {
          TableItem item = wNodeProperties.getNonEmpty(i);
          int col = 1;
          String propertyKey = item.getText(col++);
          GraphPropertyType propertyType = GraphPropertyType.parseCode(item.getText(col++));
          String propertyDescription = item.getText(col++);
          boolean propertyPrimary = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyMandatory = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyUnique = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyIndexed = "Y".equalsIgnoreCase(item.getText(col++));
          properties.add(
              new GraphProperty(
                  propertyKey,
                  propertyDescription,
                  propertyType,
                  propertyPrimary,
                  propertyMandatory,
                  propertyUnique,
                  propertyIndexed));
        }

        activeNode.setProperties(properties);
        // System.out.println( "Set " + activeNode.getProperties().size() + " properties on active
        // node" );
      }
    }
  }

  private void importNodeProperties() {
    try {
      if (activeNode == null) {
        return;
      }

      HopGuiPipelineGraph activePipelineGraph = HopGui.getActivePipelineGraph();
      if (activePipelineGraph == null) {
        MessageBox messageBox = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
        messageBox.setText("Sorry");
        messageBox.setMessage(
            "Sorry, I couldn't find an active pipeline to use to import output fields from a transform");
        messageBox.open();
        return;
      }
      PipelineMeta pipelineMeta = activePipelineGraph.getPipelineMeta();
      String[] transformNames = pipelineMeta.getTransformNames();

      EnterSelectionDialog enterSelectionDialog =
          new EnterSelectionDialog(
              getShell(),
              transformNames,
              "Select transform",
              "Enter the transform to use for the fields to input");
      String transformName = enterSelectionDialog.open();
      if (transformName == null) {
        return;
      }
      IRowMeta inputRowMeta = pipelineMeta.getTransformFields(getVariables(), transformName);

      String[] fieldNames = inputRowMeta.getFieldNames();

      EnterListDialog dialog =
          new EnterListDialog(
              hopGui.getShell(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.CLOSE, fieldNames);
      String[] fields = dialog.open();
      if (fields != null) {

        for (String field : fields) {
          // add this field as a property...
          //
          IValueMeta valueMeta = inputRowMeta.searchValueMeta(field);
          GraphPropertyType propertyType;
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_INTEGER:
              propertyType = GraphPropertyType.Integer;
              break;
            case IValueMeta.TYPE_NUMBER:
              propertyType = GraphPropertyType.Float;
              break;
            case IValueMeta.TYPE_DATE:
              propertyType = GraphPropertyType.LocalDateTime;
              break;
            case IValueMeta.TYPE_BOOLEAN:
              propertyType = GraphPropertyType.Boolean;
              break;
            case IValueMeta.TYPE_TIMESTAMP:
              propertyType = GraphPropertyType.LocalDateTime;
              break;
            case IValueMeta.TYPE_BINARY:
              propertyType = GraphPropertyType.ByteArray;
              break;
            default:
              propertyType = GraphPropertyType.String;
              break;
          }

          String propertyName = Neo4jUtil.standardizePropertyName(valueMeta);
          activeNode
              .getProperties()
              .add(new GraphProperty(propertyName, "", propertyType, false, false, false, false));
        }
        refreshNodeFields();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error importing transform fields as properties", e);
    }
  }

  private void copyNode() {
    if (activeNode == null) {
      return;
    }
    GraphNode graphNode = new GraphNode(activeNode);
    graphNode.setName(activeNode.getName() + " (copy)");
    graphModel.getNodes().add(graphNode);
    activeNode = graphNode;
    refreshNodesList();
    wNodesList.setSelection(
        new String[] {
          graphNode.getName(),
        });
    refreshNodeFields();
    enableFields();
  }

  private void deleteNode() {
    if (activeNode == null) {
      return;
    }
    int selectionIndex = wNodesList.getSelectionIndex();
    graphModel.getNodes().remove(activeNode);
    refreshNodesList();
    if (selectionIndex >= 0) {
      if (selectionIndex >= wNodesList.getItemCount()) {
        // Select last item if last was deleted.
        //
        selectionIndex = wNodesList.getItemCount() - 1;
      }
      wNodesList.setSelection(selectionIndex);
      if (wNodesList.getSelection().length > 0) {
        setActiveNode(wNodesList.getSelection()[0]);
      } else {
        setActiveNode(null);
      }
      refreshNodeFields();
    }
    enableFields();
  }

  private void newNode() {
    GraphNode node = new GraphNode();
    node.setName("Node " + (graphModel.getNodes().size() + 1));
    graphModel.getNodes().add(node);
    setActiveNode(node.getName());
    refreshNodesList();
    refreshNodeFields();
  }

  private void addRelationshipsTab() {

    CTabItem wRelTab = new CTabItem(wTabs, SWT.NONE);
    wRelTab.setText("Relationships");

    ScrolledComposite wRelSComp = new ScrolledComposite(wTabs, SWT.V_SCROLL | SWT.H_SCROLL);
    wRelSComp.setLayout(new FillLayout());

    Composite wRelComp = new Composite(wRelSComp, SWT.NONE);
    props.setLook(wRelComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wRelComp.setLayout(formLayout);

    // Relationships properties
    //  - Relationships List
    //  - Relationship name
    //  - Relationship description
    //  - Relationship labels
    //  - Relationship properties list
    //

    // buttons for New/Edit/Delete Relationship
    //
    Button wNewRelationship = new Button(wRelComp, SWT.PUSH);
    wNewRelationship.setText("New relationship");
    wNewRelationship.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            newRelationship();
          }
        });

    Button wDeleteRelationship = new Button(wRelComp, SWT.PUSH);
    wDeleteRelationship.setText("Delete relationship");
    wDeleteRelationship.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            deleteRelationship();
          }
        });

    Button wCopyRelationship = new Button(wRelComp, SWT.PUSH);
    wCopyRelationship.setText("Copy relationship");
    wCopyRelationship.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            copyRelationship();
          }
        });
    BaseTransformDialog.positionBottomButtons(
        wRelComp,
        new Button[] {wNewRelationship, wDeleteRelationship, wCopyRelationship},
        margin,
        null);

    Label wlRelationshipsList = new Label(wRelComp, SWT.LEFT);
    wlRelationshipsList.setText("Relationships list");
    props.setLook(wlRelationshipsList);
    FormData fdlRelationshipsList = new FormData();
    fdlRelationshipsList.left = new FormAttachment(0, 0);
    fdlRelationshipsList.right = new FormAttachment(middle, 0);
    fdlRelationshipsList.top = new FormAttachment(0, margin);
    wlRelationshipsList.setLayoutData(fdlRelationshipsList);
    wRelationshipsList = new List(wRelComp, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER);
    props.setLook(wRelationshipsList);
    FormData fdRelationshipsList = new FormData();
    fdRelationshipsList.left = new FormAttachment(0, 0);
    fdRelationshipsList.right = new FormAttachment(middle, 0);
    fdRelationshipsList.top = new FormAttachment(wlRelationshipsList, margin);
    fdRelationshipsList.bottom = new FormAttachment(wNewRelationship, -margin * 2);
    wRelationshipsList.setLayoutData(fdRelationshipsList);
    wRelationshipsList.addListener(
        SWT.Selection,
        event -> {
          setActiveRelationship(wRelationshipsList.getSelection()[0]);
          refreshRelationshipsFields();
        });

    wlRelName = new Label(wRelComp, SWT.LEFT);
    wlRelName.setText("Name");
    props.setLook(wlRelName);
    FormData fdlRelName = new FormData();
    fdlRelName.left = new FormAttachment(middle, margin);
    fdlRelName.top = new FormAttachment(wlRelationshipsList, 2 * margin);
    wlRelName.setLayoutData(fdlRelName);
    wRelName = new Text(wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelName);
    FormData fdRelName = new FormData();
    fdRelName.left = new FormAttachment(wlRelName, margin * 2);
    fdRelName.right = new FormAttachment(100, 0);
    fdRelName.top = new FormAttachment(wlRelName, 0, SWT.CENTER);
    wRelName.setLayoutData(fdRelName);
    wRelName.addListener(
        SWT.Modify,
        event -> {
          if (activeRelationship != null) {
            String relationshipName = wRelName.getText();
            activeRelationship.setName(relationshipName);
            String[] relationshipNames = graphModel.getRelationshipNames();
            wRelationshipsList.setItems(relationshipNames);
            wRelationshipsList.setSelection(
                Const.indexOfString(relationshipName, relationshipNames));
            setChanged();
          }
        });

    wlRelDescription = new Label(wRelComp, SWT.LEFT);
    wlRelDescription.setText("Description");
    props.setLook(wlRelDescription);
    FormData fdlRelDescription = new FormData();
    fdlRelDescription.left = new FormAttachment(middle, margin);
    fdlRelDescription.top = new FormAttachment(wRelName, margin);
    wlRelDescription.setLayoutData(fdlRelDescription);
    wRelDescription = new Text(wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelDescription);
    FormData fdRelDescription = new FormData();
    fdRelDescription.left = new FormAttachment(wlRelDescription, margin * 2);
    fdRelDescription.right = new FormAttachment(100, 0);
    fdRelDescription.top = new FormAttachment(wlRelDescription, 0, SWT.CENTER);
    wRelDescription.setLayoutData(fdRelDescription);
    wRelDescription.addListener(
        SWT.Modify,
        event -> {
          if (activeRelationship != null) {
            activeRelationship.setDescription(wRelDescription.getText());
            setChanged();
          }
        });

    wlRelLabel = new Label(wRelComp, SWT.LEFT);
    wlRelLabel.setText("Label");
    props.setLook(wlRelLabel);
    FormData fdlRelLabel = new FormData();
    fdlRelLabel.left = new FormAttachment(middle, margin);
    fdlRelLabel.top = new FormAttachment(wRelDescription, margin);
    wlRelLabel.setLayoutData(fdlRelLabel);
    wRelLabel = new Text(wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelLabel);
    FormData fdRelLabel = new FormData();
    fdRelLabel.left = new FormAttachment(wlRelLabel, margin * 2);
    fdRelLabel.right = new FormAttachment(100, 0);
    fdRelLabel.top = new FormAttachment(wlRelLabel, 0, SWT.CENTER);
    wRelLabel.setLayoutData(fdRelLabel);
    wRelLabel.addListener(
        SWT.Modify,
        event -> {
          if (activeRelationship != null) {
            activeRelationship.setLabel(wRelLabel.getText());
            setChanged();
          }
        });

    wlRelSource = new Label(wRelComp, SWT.LEFT);
    wlRelSource.setText("Source");
    props.setLook(wlRelSource);
    FormData fdlRelSource = new FormData();
    fdlRelSource.left = new FormAttachment(middle, margin);
    fdlRelSource.top = new FormAttachment(wRelLabel, margin);
    wlRelSource.setLayoutData(fdlRelSource);
    wRelSource = new CCombo(wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelSource);
    FormData fdRelSource = new FormData();
    fdRelSource.left = new FormAttachment(wlRelSource, margin * 2);
    fdRelSource.right = new FormAttachment(100, 0);
    fdRelSource.top = new FormAttachment(wlRelSource, 0, SWT.CENTER);
    wRelSource.setLayoutData(fdRelSource);
    wRelSource.addListener(
        SWT.Modify,
        event -> {
          if (activeRelationship != null) {
            activeRelationship.setNodeSource(wRelSource.getText());
            setChanged();
          }
        });

    wlRelTarget = new Label(wRelComp, SWT.LEFT);
    wlRelTarget.setText("Target");
    props.setLook(wlRelTarget);
    FormData fdlRelTarget = new FormData();
    fdlRelTarget.left = new FormAttachment(middle, margin);
    fdlRelTarget.top = new FormAttachment(wRelSource, margin);
    wlRelTarget.setLayoutData(fdlRelTarget);
    wRelTarget = new CCombo(wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wRelTarget);
    FormData fdRelTarget = new FormData();
    fdRelTarget.left = new FormAttachment(wlRelTarget, margin * 2);
    fdRelTarget.right = new FormAttachment(100, 0);
    fdRelTarget.top = new FormAttachment(wlRelTarget, 0, SWT.CENTER);
    wRelTarget.setLayoutData(fdRelTarget);
    wRelTarget.addListener(
        SWT.Modify,
        event -> {
          if (activeRelationship != null) {
            activeRelationship.setNodeTarget(wRelTarget.getText());
            setChanged();
          }
        });

    // Properties
    //
    wlRelProperties = new Label(wRelComp, SWT.LEFT);
    wlRelProperties.setText("Properties:");
    props.setLook(wlRelProperties);
    FormData fdlRelProperties = new FormData();
    fdlRelProperties.left = new FormAttachment(middle, margin);
    fdlRelProperties.top = new FormAttachment(wRelTarget, margin);
    wlRelProperties.setLayoutData(fdlRelProperties);

    ColumnInfo[] propertyColumns =
        new ColumnInfo[] {
          new ColumnInfo("Property key", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              "Property Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false),
          new ColumnInfo("Description", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Primary?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo(
              "Mandatory?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo("Unique?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
          new ColumnInfo("Indexed?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {"Y", "N"}, false),
        };
    ModifyListener propertyModifyListener = modifyEvent -> getRelationshipPropertiesFromView();
    wRelProperties =
        new TableView(
            new Variables(),
            wRelComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            propertyColumns,
            1,
            propertyModifyListener,
            props);
    wRelProperties.table.addListener(SWT.FocusOut, event -> getRelationshipPropertiesFromView());
    props.setLook(wRelProperties);
    FormData fdRelProperties = new FormData();
    fdRelProperties.left = new FormAttachment(middle, margin);
    fdRelProperties.right = new FormAttachment(100, 0);
    fdRelProperties.top = new FormAttachment(wlRelProperties, margin);
    fdRelProperties.bottom = new FormAttachment(wNewRelationship, -2 * margin);
    wRelProperties.setLayoutData(fdRelProperties);

    FormData fdRelComp = new FormData();
    fdRelComp.left = new FormAttachment(0, 0);
    fdRelComp.top = new FormAttachment(0, 0);
    fdRelComp.right = new FormAttachment(100, 0);
    fdRelComp.bottom = new FormAttachment(100, 0);
    wRelComp.setLayoutData(fdRelComp);

    wRelComp.pack();

    Rectangle bounds = wRelComp.getBounds();

    wRelSComp.setContent(wRelComp);
    wRelSComp.setExpandHorizontal(true);
    wRelSComp.setExpandVertical(true);
    wRelSComp.setMinWidth(bounds.width);
    wRelSComp.setMinHeight(bounds.height);

    wRelTab.setControl(wRelSComp);
  }

  /** Someone changed something in the relationship properties, update the active node */
  private void getRelationshipPropertiesFromView() {

    if (activeRelationship != null) {
      setChanged();
      if (monitorRelProperties) {
        // System.out.println( "Relationship properties changed! " + new Date().getTime() + " found
        // " + wRelProperties.nrNonEmpty() + " properties" );

        java.util.List<GraphProperty> properties = new ArrayList<>();
        for (int i = 0; i < wRelProperties.nrNonEmpty(); i++) {
          TableItem item = wRelProperties.getNonEmpty(i);
          int col = 1;
          String propertyKey = item.getText(col++);
          GraphPropertyType propertyType = GraphPropertyType.parseCode(item.getText(col++));
          String propertyDescription = item.getText(col++);
          boolean propertyPrimary = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyMandatory = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyUnique = "Y".equalsIgnoreCase(item.getText(col++));
          boolean propertyIndexed = "Y".equalsIgnoreCase(item.getText(col++));

          properties.add(
              new GraphProperty(
                  propertyKey,
                  propertyDescription,
                  propertyType,
                  propertyPrimary,
                  propertyMandatory,
                  propertyUnique,
                  propertyIndexed));
        }

        activeRelationship.setProperties(properties);
        // System.out.println( "Set " + activeRelationship.getProperties().size() + " properties on
        // active relationship" );
      }
    }
  }

  private void copyRelationship() {
    if (activeRelationship == null) {
      return;
    }
    GraphRelationship graphRelationship = new GraphRelationship(activeRelationship);
    graphRelationship.setName(activeRelationship.getName() + " (copy)");
    graphModel.getRelationships().add(graphRelationship);
    setActiveRelationship(graphRelationship.getName());
    refreshRelationshipsList();
    wRelationshipsList.setSelection(
        new String[] {
          graphRelationship.getName(),
        });
    refreshRelationshipsFields();
  }

  private void deleteRelationship() {
    if (activeRelationship == null) {
      return;
    }
    int selectionIndex = wRelationshipsList.getSelectionIndex();
    graphModel.getRelationships().remove(activeRelationship);
    refreshRelationshipsList();
    if (selectionIndex >= 0) {
      if (selectionIndex >= wRelationshipsList.getItemCount()) {
        // Select last item if last was deleted.
        //
        selectionIndex = wRelationshipsList.getItemCount() - 1;
      }
      wRelationshipsList.setSelection(selectionIndex);
      if (wRelationshipsList.getSelection().length > 0) {
        setActiveRelationship(wRelationshipsList.getSelection()[0]);
      } else {
        setActiveRelationship(null);
      }
      refreshRelationshipsFields();
    }
  }

  private void newRelationship() {
    GraphRelationship relationship = new GraphRelationship();
    relationship.setName("Relationship " + (graphModel.getRelationships().size() + 1));
    graphModel.getRelationships().add(relationship);
    setActiveRelationship(relationship.getName());
    refreshRelationshipsList();
    refreshRelationshipsFields();
  }

  private void addGraphTab() {

    CTabItem wGraphTab = new CTabItem(wTabs, SWT.NONE);
    wGraphTab.setText("Graph");

    ScrolledComposite wGraphSComp = new ScrolledComposite(wTabs, SWT.V_SCROLL | SWT.H_SCROLL);
    wGraphSComp.setLayout(new FillLayout());

    Composite wGraphComp = new Composite(wGraphSComp, SWT.NONE);
    props.setLook(wGraphComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wGraphComp.setLayout(formLayout);

    Button wAuto = new Button(wGraphComp, SWT.PUSH);
    wAuto.setText("Auto");
    FormData fdAuto = new FormData();
    fdAuto.right = new FormAttachment(100, 0);
    fdAuto.bottom = new FormAttachment(100, 0);
    wAuto.setLayoutData(fdAuto);
    wAuto.addListener(SWT.Selection, this::autoModelLayout);

    // This is the canvas on which we draw the graph
    //
    wCanvas = new Canvas(wGraphComp, SWT.NONE);
    props.setLook(wCanvas);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    wCanvas.setLayoutData(fdCanvas);
    wCanvas.addPaintListener(this::paintCanvas);
    wCanvas.addListener(SWT.MouseDown, this::graphMouseDown);
    wCanvas.addListener(SWT.MouseUp, this::graphMouseUp);
    wCanvas.addListener(SWT.MouseMove, this::moveGraphObject);
    wCanvas.addListener(SWT.MouseDoubleClick, this::editGraphObject);

    FormData fdGraphComp = new FormData();
    fdGraphComp.left = new FormAttachment(0, 0);
    fdGraphComp.top = new FormAttachment(0, 0);
    fdGraphComp.right = new FormAttachment(100, 0);
    fdGraphComp.bottom = new FormAttachment(wAuto, -margin);
    wGraphComp.setLayoutData(fdGraphComp);

    wGraphComp.pack();

    Rectangle bounds = wGraphComp.getBounds();

    wGraphSComp.setContent(wGraphComp);
    wGraphSComp.setExpandHorizontal(true);
    wGraphSComp.setExpandVertical(true);
    wGraphSComp.setMinWidth(bounds.width);
    wGraphSComp.setMinHeight(bounds.height);

    wGraphTab.setControl(wGraphSComp);
  }

  private void autoModelLayout(Event event) {

    // Height is usually the limiting factor
    // Take half of it
    //
    Rectangle bounds = wCanvas.getBounds();
    nodeCache = new HashMap<>();

    int optDistance =
        (int) ((bounds.width + bounds.height) * 1.5 / (graphModel.getNodes().size() + 1));
    int nrNodes = graphModel.getNodes().size();

    java.util.List<Point> nodesizes = getNodeSizes();

    // Generate list of random points in the bounding box
    //
    java.util.List<Point> bestCoordinates = generateRandomPoints(bounds, nrNodes);
    ;
    Scoring bestScore = calculateGraphScore(bestCoordinates, bounds, optDistance, nodesizes);

    System.out.println(
        ">>>>>>>>>>> optDistance="
            + optDistance
            + ", nrNodes="
            + nrNodes
            + ", startscore="
            + bestScore);
    for (int iteration = 0; iteration < 10000; iteration++) {

      // Change one random point
      //
      java.util.List<Point> testCoordinates = modifyRandomPoints(bestCoordinates, bounds, 2);
      Scoring testScore = calculateGraphScore(testCoordinates, bounds, optDistance, nodesizes);
      if (testScore.score < bestScore.score) {
        bestScore = testScore;
        bestCoordinates = testCoordinates;
      }
    }

    // Modify the graph model after iterating
    //
    for (int n = 0; n < nrNodes; n++) {
      Point point = bestCoordinates.get(n);
      GraphPresentation presentation = graphModel.getNodes().get(n).getPresentation();
      presentation.setX(point.x);
      presentation.setY(point.y);
    }

    System.out.println("<<<<<<<<<<<<< Best score found : " + bestScore);

    wCanvas.redraw();
  }

  private java.util.List<Point> getNodeSizes() {
    java.util.List<Point> sizes = new ArrayList<>();

    Image image = new Image(getShell().getDisplay(), 100, 100);
    GC gc = new GC(image);
    gc.setFont(GuiResource.getInstance().getFontMediumBold());

    for (GraphNode node : graphModel.getNodes()) {
      Point textExtent = gc.textExtent(node.getName());

      int width = textExtent.x + 2 * 10; // 10 : margin
      int height = textExtent.y + 2 * 10;

      sizes.add(new Point(width, height));
    }
    gc.dispose();
    image.dispose();

    return sizes;
  }

  private Map<String, Integer> nodeCache;

  private int lookupNode(String nodeName) {
    Integer index = nodeCache.get(nodeName);
    if (index == null) {
      GraphNode node = graphModel.findNode(nodeName);
      index = graphModel.getNodes().indexOf(node);
      nodeCache.put(nodeName, index);
    }
    return index;
  }

  private class Scoring {
    double score;
    double distanceToOthers;
    double distanceToCenter;
    double vertexLength;
    double crossedVertices;
    double overlappingLabels;

    public Scoring() {
      score = 0.0;
      distanceToOthers = 0.0;
      distanceToCenter = 0.0;
      vertexLength = 0.0;
      crossedVertices = 0.0;
      overlappingLabels = 0.0;
    }

    public void calculateTotal() {
      score =
          distanceToOthers + distanceToCenter + vertexLength + crossedVertices + overlappingLabels;
    }

    @Override
    public String toString() {
      return "Score: "
          + (long) score
          + " [distanceToOthers="
          + (long) distanceToOthers
          + ", distanceToCenter"
          + (long) distanceToCenter
          + ", vertexLength="
          + (long) vertexLength
          + ", crossedVertices="
          + (long) crossedVertices
          + ", overlappingLabels="
          + overlappingLabels
          + "]";
    }
  }

  private Scoring calculateGraphScore(
      java.util.List<Point> coordinates,
      Rectangle bounds,
      int optDistance,
      java.util.List<Point> nodeSizes) {
    Scoring scoring = new Scoring();

    Point center = new Point(bounds.width / 2, bounds.height / 2);

    for (int n = 0; n < graphModel.getNodes().size(); n++) {
      GraphNode node = graphModel.getNodes().get(n);
      Point nodePoint = coordinates.get(n);

      // Calculate distance score to other nodes in radius.
      //
      java.util.List<GraphNode> otherNodes = graphModel.getNodes();
      for (int o = 0; o < otherNodes.size(); o++) {
        GraphNode otherNode = otherNodes.get(o);
        Point otherNodePoint = coordinates.get(o);

        if (!node.equals(otherNode)) {
          double distanceToOtherNode = calculateDistance(nodePoint, otherNodePoint);

          // We score difference with optimal distance
          //
          scoring.distanceToOthers +=
              (distanceToOtherNode - optDistance) * (distanceToOtherNode - optDistance);
        }
      }

      // Add penalty for being far from the center!
      //
      scoring.distanceToCenter += 25 * calculateDistance(center, nodePoint);
    }

    // Penalties for crossing vertices
    //
    for (GraphRelationship vertex : graphModel.getRelationships()) {
      int fromIndex = lookupNode(vertex.getNodeSource());
      Point vFrom = coordinates.get(fromIndex);
      int toIndex = lookupNode(vertex.getNodeTarget());
      Point vTo = coordinates.get(toIndex);

      // Penalty for longer vertices is even higher
      //
      scoring.vertexLength += calculateDistance(vFrom, vTo);

      // score += 200 * Math.sqrt( ( vertexLength - optDistance ) * ( vertexLength - optDistance )
      // );

      // Penalties for crossing vertices
      //
      for (GraphRelationship otherVertex : graphModel.getRelationships()) {
        if (!vertex.equals(otherVertex)) {
          int fromOtherIndex = lookupNode(otherVertex.getNodeSource());
          Point oFrom = coordinates.get(fromOtherIndex);
          int toOtherIndex = lookupNode(otherVertex.getNodeTarget());
          Point oTo = coordinates.get(toOtherIndex);

          Line2D one = new Line2D.Double(vFrom.x, vFrom.y, vTo.x, vTo.y);
          Line2D two = new Line2D.Double(oFrom.x, oFrom.y, oTo.x, oTo.y);

          if (one.intersectsLine(two)) {
            scoring.crossedVertices += 20000;
          }
        }
      }
    }

    // Build label rectangles
    //
    java.util.List<Rectangle> labels = new ArrayList<>();
    for (int s = 0; s < nodeSizes.size(); s++) {
      Point nodePoint = coordinates.get(s);
      Point size = nodeSizes.get(s);
      labels.add(new Rectangle(nodePoint.x, nodePoint.y, size.x, size.y));
    }

    // Pentalties for overlapping labels
    //
    for (int a = 0; a < labels.size(); a++) {
      Rectangle labelA = labels.get(a);
      // Intersection with other label?
      //
      for (int b = 0; b < labels.size(); b++) {
        if (a != b) {
          Rectangle labelB = labels.get(b);
          if (labelA.intersects(labelB)) {
            scoring.overlappingLabels += 30000;
          }
        }
      }
      // Label outside of bounds?
      //
      if (labelA.x + labelA.width > bounds.width) {
        scoring.overlappingLabels += 50000;
      }
      if (labelA.y + labelA.height > bounds.height) {
        scoring.overlappingLabels += 50000;
      }
      // Intersection with any vertices?
      //

    }

    scoring.calculateTotal();

    return scoring;
  }

  private double calculateDistance(Point a, Point b) {
    return calculateDistance(a.x, a.y, b.x, b.y);
  }

  private Point getNodePoint(GraphNode node) {
    return new Point(node.getPresentation().getX(), node.getPresentation().getY());
  }

  private Random random = new Random();

  private java.util.List<Point> generateRandomPoints(Rectangle bounds, int size) {
    java.util.List<Point> points = new ArrayList<>();
    for (int i = 0; i < size; i++) {

      Point point = generateRandomPoint(bounds);
      points.add(point);
    }
    return points;
  }

  private Point generateRandomPoint(Rectangle bounds) {
    int x = (int) ((random.nextDouble() * bounds.width * 0.7) + (0.15 * bounds.width));
    int y = (int) ((random.nextDouble() * bounds.height * 0.7) + (0.15 * bounds.height));
    return new Point(x, y);
  }

  private java.util.List<Point> modifyRandomPoints(
      java.util.List<Point> original, Rectangle bounds, int count) {
    int size = original.size();

    java.util.List<Point> points = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Point point = original.get(i);
      points.add(new Point(point.x, point.y));
    }

    // Modify a few random points in the list
    //
    for (int i = 0; i < count; i++) {
      int index = (int) (random.nextDouble() * size);
      Point point = generateRandomPoint(bounds);
      points.set(index, point);
    }
    return points;
  }

  /**
   * So we're doing one scan over all the nodes. We're calculating all the nodes in a certain
   * radius.
   *
   * <p>So looking in [x-radius, y-radius] to [x+radius, y+radius]
   *
   * @param centerNode The node at the center of circle
   * @param radius The radius of the circle
   * @return The list of nodes
   */
  protected java.util.List<GraphNode> findNodesInCircle(GraphNode centerNode, int radius) {
    int xc = centerNode.getPresentation().getX();
    int yc = centerNode.getPresentation().getY();

    java.util.List<GraphNode> nodes = new ArrayList<>();

    for (GraphNode node : graphModel.getNodes()) {
      // Don't add same node
      //
      if (!node.equals(centerNode)) {

        int xn = node.getPresentation().getX();
        int yn = node.getPresentation().getY();

        double distance = calculateDistance(xc, yc, xn, yn);

        // Check if the node is within the circle
        //
        if (distance < radius) {
          nodes.add(node);
        }
      }
    }

    return nodes;
  }

  private double calculateDistance(int xc, int yc, int xn, int yn) {
    return Math.sqrt((xn - xc) * (xn - xc) + (yn - yc) * (yn - yc));
  }

  private void graphMouseUp(Event e) {
    mouseDownPoint.x = -1;
    mouseDownPoint.y = -1;
    // System.out.println("Up: ("+e.x+", "+e.y+")");
  }

  private void graphMouseDown(Event e) {
    mouseDownPoint.x = e.x;
    mouseDownPoint.y = e.y;
    // System.out.println("Down: ("+e.x+", "+e.y+")");
  }

  private void moveGraphObject(Event e) {
    if (mouseDownPoint.x > 0 && mouseDownPoint.y > 0) {
      // System.out.println("Move: ("+e.x+", "+e.y+")");
      // Mouse drag
      //
      AreaOwner areaOwner = AreaOwner.findArea(areaOwners, mouseDownPoint.x, mouseDownPoint.y);
      if (areaOwner != null) {
        int offsetX = mouseDownPoint.x - areaOwner.getX();
        int offsetY = mouseDownPoint.y - areaOwner.getY();
        // System.out.println("Offset: (+"+offsetX+", "+offsetY+")");

        switch (areaOwner.getAreaType()) {
          case NODE:
            GraphNode graphNode = (GraphNode) areaOwner.getSubject();
            GraphPresentation p = new GraphPresentation(e.x - offsetX, e.y - offsetY);
            graphNode.setPresentation(p);
            // System.out.println("Node move: (+"+p.getX()+", "+p.getY()+")  -->
            // "+graphNode.getName());
            mouseDownPoint.x = e.x;
            mouseDownPoint.y = e.y;
            wCanvas.redraw();
            setChanged();
            break;
          default:
            break;
        }
      } else {
        // Move all the objects around
        //
        int offsetX = mouseDownPoint.x - e.x;
        int offsetY = mouseDownPoint.y - e.y;

        for (GraphNode graphNode : graphModel.getNodes()) {
          GraphPresentation p = graphNode.getPresentation();
          p.setX(p.getX() - offsetX);
          p.setY(p.getY() - offsetY);
        }
        mouseDownPoint.x = e.x;
        mouseDownPoint.y = e.y;

        wCanvas.redraw();
      }
    }
  }

  private void editGraphObject(Event e) {

    mouseDownPoint.x = -1;
    mouseDownPoint.y = -1;

    AreaOwner areaOwner = AreaOwner.findArea(areaOwners, e.x, e.y);
    if (areaOwner == null) {
      return;
    }

    switch (areaOwner.getAreaType()) {
      case NODE:
        // Select the nodes tab
        //
        wTabs.setSelection(1);

        // Which node?
        //
        GraphNode graphNode = (GraphNode) areaOwner.getSubject();

        // Select the right node name
        //
        int nodeIndex = Const.indexOfString(graphNode.getName(), wNodesList.getItems());
        if (nodeIndex >= 0) {
          getNodeLabelsFromView();
          setActiveNode(graphNode.getName());
          wNodesList.setSelection(nodeIndex);
          refreshNodeFields();
        }
        break;
      case RELATIONSHIP_LABEL:
        // Select the relationships tab
        //
        wTabs.setSelection(2);

        // Which relationship?
        //
        GraphRelationship rel = (GraphRelationship) areaOwner.getSubject();

        // select the relationship...
        //
        int relIndex = Const.indexOfString(rel.getName(), wRelationshipsList.getItems());
        if (relIndex >= 0) {
          setActiveRelationship(rel.getName());
          wRelationshipsList.setSelection(relIndex);
          refreshRelationshipsFields();
        }
        break;
      default:
        break;
    }
  }

  private java.util.List<AreaOwner> areaOwners;

  private synchronized void paintCanvas(PaintEvent e) {

    areaOwners = new ArrayList<>();
    GC gc = e.gc;

    gc.setForeground(GuiResource.getInstance().getColorLightGray());
    gc.fillRectangle(0, 0, e.width, e.height);

    int margin = 10;

    // Draw the relationships
    //
    for (GraphRelationship relationship : graphModel.getRelationships()) {
      GraphNode sourceNode = graphModel.findNode(relationship.getNodeSource());
      GraphNode targetNode = graphModel.findNode(relationship.getNodeTarget());
      if (sourceNode != null && targetNode != null) {
        gc.setFont(GuiResource.getInstance().getFontMedium());
        Point sourceExtent = gc.textExtent(sourceNode.getName());
        Point targetExtent = gc.textExtent(targetNode.getName());

        int fromX = sourceNode.getPresentation().getX() + margin + sourceExtent.x / 2;
        int fromY = sourceNode.getPresentation().getY() + margin + sourceExtent.y / 2;

        int toX = targetNode.getPresentation().getX() + margin + targetExtent.x / 2;
        int toY = targetNode.getPresentation().getY() + margin + targetExtent.y / 2;

        gc.setForeground(GuiResource.getInstance().getColorLightBlue());
        gc.drawLine(fromX, fromY, toX, toY);

        gc.setFont(GuiResource.getInstance().getFontMedium());
        Point relExtent = gc.textExtent(relationship.getName());

        int middleX = fromX + (toX - fromX) / 2 - relExtent.x / 2;
        int middleY = fromY + (toY - fromY) / 2 - relExtent.y / 2;

        gc.setForeground(GuiResource.getInstance().getColorBlack());
        gc.drawText(relationship.getName(), middleX, middleY);
        areaOwners.add(
            new AreaOwner(
                middleX,
                middleY,
                relExtent.x,
                relExtent.y,
                AreaType.RELATIONSHIP_LABEL,
                relationship));
      }
    }

    // Draw all the nodes
    //
    gc.setFont(GuiResource.getInstance().getFontMediumBold());
    for (GraphNode graphNode : graphModel.getNodes()) {
      GraphPresentation presentation = graphNode.getPresentation();
      Point textExtent = gc.textExtent(graphNode.getName());
      int x = presentation.getX();
      int y = presentation.getY();
      int width = textExtent.x + 2 * margin;
      int height = textExtent.y + 2 * margin;

      gc.setForeground(GuiResource.getInstance().getColorBackground());
      gc.fillRoundRectangle(x, y, width, height, margin, margin);

      gc.setForeground(GuiResource.getInstance().getColorBlue());
      gc.drawRoundRectangle(x, y, width, height, height / 3, height / 3);
      areaOwners.add(new AreaOwner(x, y, width, height, AreaType.NODE, graphNode));

      gc.setForeground(GuiResource.getInstance().getColorBlack());
      gc.drawText(graphNode.getName(), x + margin, y + margin);
    }
  }

  private void importGraphFromFile() {
    try {
      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "Model JSON",
              "This is the JSON of the graph model",
              graphModel.getJSONString(),
              true);
      String jsonModelString = dialog.open();
      if (jsonModelString == null) {
        return;
      }

      // The graph model is loaded, replace the one in memory
      //
      graphModel = new GraphModel(jsonModelString);

      // Refresh the dialog.
      //
      setWidgetsContent();
      setChanged();

    } catch (Exception e) {
      new ErrorDialog(getShell(), "ERROR", "Error importing JSON", e);
    }
  }

  private void exportGraphToFile() {
    try {
      String prettyJsonString = getModelJson();

      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "Model JSON",
              "This is the JSON of the graph model",
              prettyJsonString,
              true);
      dialog.open();
    } catch (Exception e) {
      new ErrorDialog(getShell(), "ERROR", "Error serializing to JSON", e);
    }
  }

  private void importGraphFromSolutionsWorkbench() {
    try {
      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "Solutions Workbench Export",
              "Paste the Solutions Workbench model export (JSON) below",
              "{}",
              true);
      String jsonModelString = dialog.open();
      if (jsonModelString == null) {
        return;
      }

      // The graph model is loaded, replace the one in memory
      //
      GraphModel importedModel = SolutionsWorkbenchImporter.importFromCwJson(jsonModelString);
      graphModel = SolutionsWorkbenchImporter.changeNamesToLabels(importedModel);

      // Refresh the dialog.
      //
      setWidgetsContent();
      setChanged();

    } catch (Exception e) {
      new ErrorDialog(getShell(), "ERROR", "Error importing JSON", e);
    }
  }

  private void importGraphFromArrowsApp() {
    try {
      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "Arrows JSON",
              "Paste the Arrows application export in JSON format below",
              "{}",
              true);
      String jsonModelString = dialog.open();
      if (jsonModelString == null) {
        return;
      }

      // The graph model is loaded, replace the one in memory
      //
      graphModel = ArrowsAppImporter.importFromArrowsJson(jsonModelString);

      // Refresh the dialog.
      //
      setWidgetsContent();
      setChanged();

      MessageBox box = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText("Import successful");
      box.setMessage(
          "The import from the Arrows JSON was successful.  Please make sure to give this model a name and indicate the primary key fields of nodes.");
      box.open();

    } catch (Exception e) {
      new ErrorDialog(getShell(), "ERROR", "Error importing JSON", e);
    }
  }

  private String getModelJson() throws HopException {
    return graphModel.getJSONString();
  }

  private void copyIndexActionToClipboard() {
    try {

      // Select the index name...
      //
      IHopMetadataSerializer<NeoConnection> connectionSerializer =
          getMetadataManager().getMetadataProvider().getSerializer(NeoConnection.class);
      java.util.List<String> connectionNames = connectionSerializer.listObjectNames();
      EnterSelectionDialog enterSelectionDialog =
          new EnterSelectionDialog(
              getShell(),
              connectionNames.toArray(new String[0]),
              "Select connection",
              "Select the Neo4j Connection to create indexes and constraints on in the generated actions:");
      String connectionName = enterSelectionDialog.open();
      if (connectionName == null) {
        return;
      }
      NeoConnection connection = connectionSerializer.load(connectionName);

      Neo4jIndex neo4jIndex = new Neo4jIndex();
      neo4jIndex.setConnection(connection);

      // We need indexes for all the indexed or primary key fields (one only) in the model nodes...
      //
      for (GraphNode node : graphModel.getNodes()) {
        // Loop over all the labels
        //
        for (String label : node.getLabels()) {
          for (GraphProperty property : node.getProperties()) {
            // If the field is flagged indexed we index it,
            // but not if the field is flagged as unique
            // Note: Unique indexes are handled by a constraint below
            //
            if ((property.isIndexed() || property.isPrimary()) && !property.isUnique()) {
              neo4jIndex
                  .getIndexUpdates()
                  .add(
                      new IndexUpdate(
                          UpdateType.CREATE,
                          ObjectType.NODE,
                          "IDX_" + label.toUpperCase() + "_" + property.getName().toUpperCase(),
                          label,
                          property.getName()));
            }
          }
        }
      }

      // Wrap this in an action
      //
      ActionMeta indexMeta = new ActionMeta(neo4jIndex);
      indexMeta.setName("Create indexes for graph model " + graphModel.getName());
      indexMeta.setLocation(50, 50);
      String xmlIndex = indexMeta.getXml();

      Neo4jConstraint neo4jConstraint = new Neo4jConstraint();
      neo4jConstraint.setConnection(connection);

      // We need indexes on all the primary keys in the model nodes...
      //
      for (GraphNode node : graphModel.getNodes()) {
        // Loop over all the labels
        //
        for (String label : node.getLabels()) {
          String constraintName = label.toUpperCase();
          String properties = "";
          for (GraphProperty property : node.getProperties()) {
            // If the field is flagged unique we create a constraint for it...
            //
            if (property.isUnique()) {

              neo4jConstraint
                  .getConstraintUpdates()
                  .add(
                      new ConstraintUpdate(
                          org.apache.hop.neo4j.actions.constraint.UpdateType.CREATE,
                          org.apache.hop.neo4j.actions.constraint.ObjectType.NODE,
                          ConstraintType.UNIQUE,
                          "COU_" + label.toUpperCase() + "_" + property.getName().toUpperCase(),
                          label,
                          property.getName()));
            }
          }
        }
      }

      // Wrap this in an action
      //
      ActionMeta constraintMeta = new ActionMeta(neo4jConstraint);
      constraintMeta.setName("Create constraints for graph model " + graphModel.getName());
      constraintMeta.setLocation(100, 50);
      String xmlConstraint = constraintMeta.getXml();

      GuiResource.getInstance()
          .toClipboard(
              "<workflow-actions><actions>"
                  + xmlIndex
                  + xmlConstraint
                  + "</actions></workflow-actions>");

      MessageBox messageBox = new MessageBox(getShell(), SWT.OK | SWT.ICON_INFORMATION);
      messageBox.setText("Copied to clipboard");
      messageBox.setMessage(
          "An Neo4j Index/Constraint actions were copied to the clipboard to create the required indexes and constraints for this model.  You can paste these actions in a workflow.");
      messageBox.open();

    } catch (Exception e) {
      new ErrorDialog(getShell(), "ERROR", "Error serializing to JSON", e);
    }
  }

  @Override
  public void setChanged() {
    this.isChanged = true;
    MetadataPerspective.getInstance().updateEditor(this);
  }

  public void clearChanged() {
    this.isChanged = false;
    MetadataPerspective.getInstance().updateEditor(this);
  }
}
