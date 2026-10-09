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

package org.apache.hop.neo4j.actions.propertygraph;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/** The dialog of the Create property graph action, built from its annotated fields. */
public class ActionCreatePropertyGraphDialog extends ActionDialog {
  private static final Class<?> PKG = ActionCreatePropertyGraph.class;

  private final ActionCreatePropertyGraph action;
  private GuiCompositeWidgets widgets;

  public ActionCreatePropertyGraphDialog(
      Shell parent, IAction action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = (ActionCreatePropertyGraph) action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionCreatePropertyGraph.Name"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionCreatePropertyGraph.Name"), action);
    buildButtonBar()
        .ok(e -> ok())
        .custom(
            BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.GetFromModel.Button"),
            e -> getFromModel())
        .custom(
            BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.ShowSql.Button"),
            e -> showSql())
        .cancel(e -> cancel())
        .build();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wSpacer,
            wOk,
            ActionCreatePropertyGraph.GUI_PLUGIN_ELEMENT_PARENT_ID,
            action);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              action.setChanged();
            }
          }
        });

    wName.setText(Const.NVL(action.getName(), ""));
    loading = false;
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return action;
  }

  /** Fill in the node and edge tables of all model elements which have no mapping yet. */
  private void getFromModel() {
    widgets.getWidgetsContents(action, ActionCreatePropertyGraph.GUI_PLUGIN_ELEMENT_PARENT_ID);
    try {
      GraphModel model = action.loadGraphModel(getMetadataProvider());
      List<NodeTableMapping> nodeTables = new ArrayList<>(action.getNodeTables());
      for (NodeTableMapping mapping : PropertyGraphGenerator.getDefaultNodeMappings(model)) {
        if (nodeTables.stream().noneMatch(m -> mapping.getNodeName().equals(m.getNodeName()))) {
          nodeTables.add(mapping);
        }
      }
      List<EdgeTableMapping> edgeTables = new ArrayList<>(action.getEdgeTables());
      for (EdgeTableMapping mapping : PropertyGraphGenerator.getDefaultEdgeMappings(model)) {
        if (edgeTables.stream()
            .noneMatch(m -> mapping.getRelationshipName().equals(m.getRelationshipName()))) {
          edgeTables.add(mapping);
        }
      }
      action.setNodeTables(nodeTables);
      action.setEdgeTables(edgeTables);
      widgets.setWidgetsContents(
          action, shell, ActionCreatePropertyGraph.GUI_PLUGIN_ELEMENT_PARENT_ID);
      action.setChanged();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.Error.Title"),
          BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.GetFromModel.Error"),
          e);
    }
  }

  /** Show the statements the action would execute right now. */
  private void showSql() {
    ActionCreatePropertyGraph copy = action.clone();
    widgets.getWidgetsContents(copy, ActionCreatePropertyGraph.GUI_PLUGIN_ELEMENT_PARENT_ID);
    copy.setMetadataProvider(getMetadataProvider());
    copy.copyFrom(variables);
    try {
      DatabaseMeta databaseMeta = copy.loadDatabaseMeta(getMetadataProvider());
      GraphModel model = copy.loadGraphModel(getMetadataProvider());
      StringBuilder sql = new StringBuilder();
      try (Database database = new Database(copy, copy, databaseMeta)) {
        database.connect();
        for (String statement : copy.getStatements(database, model)) {
          sql.append(statement).append(";").append(Const.CR).append(Const.CR);
        }
      }
      new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.ShowSql.Title"),
              BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.ShowSql.Message"),
              sql.toString(),
              true)
          .open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.Error.Title"),
          BaseMessages.getString(PKG, "ActionCreatePropertyGraphDialog.ShowSql.Error"),
          e);
    }
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    widgets.getWidgetsContents(action, ActionCreatePropertyGraph.GUI_PLUGIN_ELEMENT_PARENT_ID);
    action.setName(wName.getText());
    action.setChanged();
    dispose();
  }

  private void cancel() {
    dispose();
  }
}
