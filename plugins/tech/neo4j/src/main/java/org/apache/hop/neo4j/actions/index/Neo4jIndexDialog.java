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

package org.apache.hop.neo4j.actions.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionSelectionLine;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class Neo4jIndexDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = Neo4jIndexDialog.class;

  private Neo4jIndex meta;

  private boolean changed;

  private NeoConnectionSelectionLine wConnection;
  private TableView wUpdates;

  public Neo4jIndexDialog(
      Shell parent, IAction iAction, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.meta = (Neo4jIndex) iAction;

    if (this.meta.getName() == null) {
      this.meta.setName(BaseMessages.getString(PKG, "Neo4jIndexDialog.Action.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "Neo4jIndexDialog.Dialog.Title"), meta);
    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    int middle = this.middle;
    int margin = this.margin;

    wConnection =
        new NeoConnectionSelectionLine(
            variables,
            getMetadataProvider(),
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "Neo4jIndexDialog.NeoConnection.Label"),
            BaseMessages.getString(PKG, "Neo4jIndexDialog.NeoConnection.Tooltip"),
            true);
    PropsUi.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(wSpacer, margin);
    wConnection.setLayoutData(fdConnection);
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }

    Label wlUpdates = new Label(shell, SWT.LEFT);
    wlUpdates.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Label"));
    PropsUi.setLook(wlUpdates);
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment(0, 0);
    fdlCypher.right = new FormAttachment(100, 0);
    fdlCypher.top = new FormAttachment(wConnection, margin);
    wlUpdates.setLayoutData(fdlCypher);

    // The columns
    //
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.UpdateType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              UpdateType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ObjectType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.IndexName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectName"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.ObjectProperties"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.IndexType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              IndexType.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.VectorDimensions"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.VectorSimilarity"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              GraphVectorSimilarity.getNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.VectorCapacity"),
              ColumnInfo.COLUMN_TYPE_TEXT),
        };
    columns[5].setToolTip(
        BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.IndexType.Tooltip"));
    columns[6].setUsingVariables(true);
    columns[8].setUsingVariables(true);
    columns[8].setToolTip(
        BaseMessages.getString(PKG, "Neo4jIndexDialog.IndexUpdates.Column.VectorCapacity.Tooltip"));

    // Only offer what the database of the selected connection supports
    //
    columns[1].setComboValuesSelectionListener(
        (tableItem, rowNr, colNr) -> {
          IGraphDialect dialect = getSelectedDialect();
          List<String> objectTypes = new ArrayList<>();
          if (dialect.isSupportingNodeIndexes()) {
            objectTypes.add(ObjectType.NODE.name());
          }
          if (dialect.isSupportingRelationshipIndexes()) {
            objectTypes.add(ObjectType.RELATIONSHIP.name());
          }
          return objectTypes.toArray(new String[0]);
        });

    wUpdates =
        new TableView(
            variables, shell, SWT.NONE, columns, meta.getIndexUpdates().size(), false, null, props);
    PropsUi.setLook(wUpdates);
    wUpdates.addModifyListener(lsMod);
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment(0, 0);
    fdCypher.right = new FormAttachment(100, 0);
    fdCypher.top = new FormAttachment(wlUpdates, margin);
    wUpdates.setLayoutData(fdCypher);

    buildButtonBar()
        .custom(
            BaseMessages.getString(PKG, "Neo4jIndexDialog.Button.ShowCypher"),
            e -> showCypherPreview())
        .ok(e -> ok())
        .cancel(e -> cancel())
        .build(wUpdates);

    getData();
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return meta;
  }

  private void showCypherPreview() {
    // Get current data from dialog
    List<TableItem> items = wUpdates.getNonEmptyItems();
    if (items.isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.NoIndexes.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "Neo4jIndexDialog.NoIndexes.Message"));
      mb.open();
      return;
    }

    // Build Cypher preview for all index updates
    StringBuilder cypherPreview = new StringBuilder();
    cypherPreview
        .append("-- Generated Cypher statements for index operations")
        .append(Const.CR)
        .append(Const.CR);

    for (int i = 0; i < items.size(); i++) {
      TableItem item = items.get(i);
      try {
        IndexUpdate indexUpdate = toIndexUpdate(item);
        UpdateType type = indexUpdate.getType();
        indexUpdate.setVectorDimensions(variables.resolve(indexUpdate.getVectorDimensions()));
        indexUpdate.setVectorCapacity(variables.resolve(indexUpdate.getVectorCapacity()));

        String cypher;
        if (type == UpdateType.CREATE) {
          cypher = Neo4jIndex.generateCreateIndexCypher(indexUpdate, getSelectedDialect());
        } else {
          cypher = Neo4jIndex.generateDropIndexCypher(indexUpdate, getSelectedDialect());
        }

        cypherPreview.append("-- Index ").append(i + 1).append(Const.CR);
        cypherPreview.append(cypher).append(Const.CR).append(Const.CR);
      } catch (Exception e) {
        cypherPreview
            .append("-- Error generating Cypher for index ")
            .append(i + 1)
            .append(": ")
            .append(e.getMessage())
            .append(Const.CR)
            .append(Const.CR);
      }
    }

    // Show in read-only dialog
    EnterTextDialog dialog =
        new EnterTextDialog(
            shell,
            BaseMessages.getString(PKG, "Neo4jIndexDialog.ShowCypher.Title"),
            BaseMessages.getString(PKG, "Neo4jIndexDialog.ShowCypher.Message"),
            cypherPreview.toString(),
            true);
    dialog.setReadOnly();
    dialog.open();
  }

  @Override
  protected void onActionNameModified() {
    meta.setChanged();
  }

  private void cancel() {
    meta.setChanged(changed);
    meta = null;
    dispose();
  }

  private void getData() {
    wName.setText(Const.NVL(meta.getName(), ""));
    wConnection.setText(Const.NVL(meta.getConnectionName(), ""));
    for (int i = 0; i < meta.getIndexUpdates().size(); i++) {
      TableItem item = wUpdates.table.getItem(i);
      IndexUpdate indexUpdate = meta.getIndexUpdates().get(i);

      if (indexUpdate.getType() != null) {
        item.setText(1, indexUpdate.getType().name());
      }
      if (indexUpdate.getObjectType() != null) {
        item.setText(2, indexUpdate.getObjectType().name());
      }
      item.setText(3, Const.NVL(indexUpdate.getIndexName(), ""));
      item.setText(4, Const.NVL(indexUpdate.getObjectName(), ""));
      item.setText(5, Const.NVL(indexUpdate.getObjectProperties(), ""));
      item.setText(
          6,
          indexUpdate.getIndexType() == null
              ? IndexType.RANGE.name()
              : indexUpdate.getIndexType().name());
      item.setText(7, Const.NVL(indexUpdate.getVectorDimensions(), ""));
      if (indexUpdate.getVectorSimilarity() != null) {
        item.setText(8, indexUpdate.getVectorSimilarity().name());
      }
      item.setText(9, Const.NVL(indexUpdate.getVectorCapacity(), ""));
    }
    wUpdates.optimizeTableView();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "Neo4jIndexDialog.MissingName.Warning.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "Neo4jIndexDialog.MissingName.Warning.Message"));
      mb.open();
      return;
    }
    meta.setName(wName.getText());

    // Grab the connection
    //

    meta.setConnectionName(wConnection.getText());

    List<TableItem> items = wUpdates.getNonEmptyItems();
    meta.getIndexUpdates().clear();
    for (TableItem item : items) {
      meta.getIndexUpdates().add(toIndexUpdate(item));
    }

    dispose();
  }

  /** The index update in a row of the table. The vector settings are only kept for VECTOR. */
  private static IndexUpdate toIndexUpdate(TableItem item) {
    IndexUpdate indexUpdate =
        new IndexUpdate(
            UpdateType.getType(item.getText(1)),
            ObjectType.getType(item.getText(2)),
            item.getText(3),
            item.getText(4),
            item.getText(5));
    IndexType indexType = IndexType.getType(item.getText(6));
    if (indexType == IndexType.VECTOR) {
      indexUpdate.setIndexType(IndexType.VECTOR);
      indexUpdate.setVectorDimensions(item.getText(7));
      indexUpdate.setVectorSimilarity(GraphVectorSimilarity.getType(item.getText(8)));
      indexUpdate.setVectorCapacity(item.getText(9));
    }
    return indexUpdate;
  }

  /** The dialect of the selected connection, Neo4j when it can't be determined. */
  private IGraphDialect getSelectedDialect() {
    try {
      NamedGraphConnection connection =
          NeoConnectionUtils.findGraphConnection(
              getMetadataProvider(), variables.resolve(wConnection.getText()));
      if (connection != null) {
        return connection.getDialect();
      }
    } catch (Exception e) {
      // Fall back to Neo4j
    }
    return Neo4jGraphDialect.INSTANCE;
  }
}
