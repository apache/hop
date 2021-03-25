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
 *
 */

package org.apache.hop.databases.cassandra.metadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.metadata.IMetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.actions.execcql.ExecCql;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

public class CassandraConnectionEditor extends MetadataEditor<CassandraConnection>
    implements IMetadataEditor<CassandraConnection> {

  private static final Class<?> PKG = CassandraConnectionEditor.class; // For Translator

  public static final String PARENT_WIDGET_ID = "CassandraConnectionEditor.Widgets.ParentId";

  private Composite parent;
  private Text wName;
  private GuiCompositeWidgets widgets;

  public CassandraConnectionEditor(
      HopGui hopGui, MetadataManager<CassandraConnection> manager, CassandraConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    this.parent = parent;

    PropsUi props = PropsUi.getInstance();
    int margin = props.getMargin();
    int middle = props.getMiddlePct();

    // Name...
    //
    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText("Cassandra connection name");
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // Rest of the widgets...
    //
    widgets = new GuiCompositeWidgets(manager.getVariables(), 18);
    widgets.createCompositeWidgets(getMetadata(), null, parent, PARENT_WIDGET_ID, wName);

    // Set content on the widgets...
    //
    setWidgetsContent();

    // Add changed listeners
    wName.addListener(SWT.Modify, e -> setChanged());
    for (Control control : widgets.getWidgetsMap().values()) {
      control.addListener(SWT.Modify, e -> setChanged());
    }
  }

  @Override
  public void setWidgetsContent() {
    CassandraConnection meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    widgets.setWidgetsContents(meta, parent, PARENT_WIDGET_ID);
  }

  @Override
  public void getWidgetsContent(CassandraConnection meta) {
    meta.setName(wName.getText());
    widgets.getWidgetsContents(meta, PARENT_WIDGET_ID);
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    Button wbSelectKeyspace = new Button(parent, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSelectKeyspace);
    wbSelectKeyspace.setText("Select keyspace");
    wbSelectKeyspace.addListener(SWT.Selection, e -> selectKeyspace());

    Button wbTest = new Button(parent, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTest);
    wbTest.setText("Test");
    wbTest.addListener(SWT.Selection, e -> test());

    Button wbCql = new Button(parent, SWT.PUSH | SWT.CENTER);
    props.setLook(wbCql);
    wbCql.setText("Execute CQL");
    wbCql.addListener(SWT.Selection, e -> execCql());

    return new Button[] {
      wbCql, wbSelectKeyspace, wbTest,
    };
  }

  public void test() {
    try {
      CassandraConnection meta = new CassandraConnection();
      getWidgetsContent(meta);

      Connection connection = meta.createConnection(manager.getVariables(), false);
      try {
        connection.openConnection();
        connection.getKeyspaceNames();
      } finally {
        connection.closeConnection();
      }

      MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText("Success!");
      box.setMessage("It's possible to connect to Cassandra with this metadata!");
      box.open();

    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "We couldn't connect using this information", e);
    }
  }

  public void selectKeyspace() {
    try {
      CassandraConnection meta = new CassandraConnection();
      getWidgetsContent(meta);

      Connection connection = meta.createConnection(manager.getVariables(), false);
      try {
        connection.openConnection();
        String[] keyspaceNames = connection.getKeyspaceNames();
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                getShell(), keyspaceNames, "Select keyspace", "Select the keyspace to use:");
        String keyspaceName = dialog.open();
        if (keyspaceName != null) {
          meta.setKeyspace(keyspaceName);
          setMetadata(meta);
          setWidgetsContent();
        }
      } finally {
        connection.closeConnection();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error selecting keyspace", e);
    }
  }

  public void execCql() {
    try {

      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "Enter CQL",
              "Enter the CQL statements to execute on this Cassandra connection."
                  + Const.CR
                  + "The statements are split by a ; on a separate line."
                  + Const.CR
                  + "Please note that results of queries are not shown at this time.",
              "",
              true);
      String cql = dialog.open();
      if (cql != null) {

        CassandraConnection meta = new CassandraConnection();
        getWidgetsContent(meta);

        int executed =
            ExecCql.executeCqlStatements(
                manager.getVariables(), LogChannel.UI, new Result(), meta, cql);
        MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
        box.setText("Success!");
        box.setMessage(+executed + " CQL statements were executed.");
        box.open();
      }
    } catch (Exception e) {
      new ErrorDialog(
          parent.getShell(),
          "Error",
          "There was an error executing CQL on this Cassandra cluster",
          e);
    }
  }
}
