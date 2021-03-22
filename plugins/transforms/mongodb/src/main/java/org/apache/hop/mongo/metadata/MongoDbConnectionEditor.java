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

package org.apache.hop.mongo.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.metadata.IMetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class MongoDbConnectionEditor extends MetadataEditor<MongoDbConnection>
    implements IMetadataEditor<MongoDbConnection> {

  private static final Class<?> PKG = MongoDbConnectionEditor.class; // For Translator

  public static final String PARENT_WIDGET_ID = "MongoDbConnectionEditor.Widgets.ParentId";

  private Composite parent;
  private Text wName;
  private GuiCompositeWidgets widgets;

  public MongoDbConnectionEditor(
      HopGui hopGui, MetadataManager<MongoDbConnection> manager, MongoDbConnection metadata) {
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
    wlName.setText("MongoDB Connection name");
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
    MongoDbConnection meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    widgets.setWidgetsContents(meta, parent, PARENT_WIDGET_ID);
  }

  @Override
  public void getWidgetsContent(MongoDbConnection meta) {
    meta.setName(wName.getText());
    widgets.getWidgetsContents(meta, PARENT_WIDGET_ID);
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    Button wbGetDbs = new Button(parent, SWT.PUSH | SWT.CENTER);
    props.setLook(wbGetDbs);
    wbGetDbs.setText("Get databases");
    wbGetDbs.addListener(SWT.Selection, e -> getDbNames());

    Button wbTest = new Button(parent, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTest);
    wbTest.setText("Test");
    wbTest.addListener(SWT.Selection, e -> test());

    return new Button[] {
      wbTest, wbGetDbs,
    };
  }

  public void test() {
    try {
      MongoDbConnection meta = new MongoDbConnection();
      getWidgetsContent(meta);
      meta.test(manager.getVariables(), LogChannel.UI);
      MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText("Success!");
      box.setMessage("Connected successfully!");
      box.open();
    } catch (Exception e) {
      new ErrorDialog(parent.getShell(), "Error", "We couldn't connect using this information", e);
    }
  }

  public void getDbNames() {
    ComboVar wDbName = (ComboVar) widgets.getWidgetsMap().get(MongoDbConnection.WIDGET_ID_DB_NAME);
    IVariables variables = manager.getVariables();
    ILogChannel log = LogChannel.UI;

    String current = wDbName.getText();
    wDbName.removeAll();

    MongoDbConnection meta = new MongoDbConnection();
    getWidgetsContent(meta);

    String hostname = variables.resolve(meta.getHostname());

    if (!StringUtils.isEmpty(hostname)) {

      try {
        MongoClientWrapper wrapper = meta.createWrapper(variables, log);
        List<String> dbNames;
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }

        for (String s : dbNames) {
          wDbName.add(s);
        }
      } catch (Exception e) {
        log.logError("Error connecting to MongoDB connection " + getName(), e);
        new ErrorDialog(
            parent.getShell(), "Error", "Error connecting to MongoDB connection " + getName(), e);
      }
    } else {
      // popup some feedback
      ShowMessageDialog smd =
          new ShowMessageDialog(
              parent.getShell(),
              SWT.ICON_WARNING | SWT.OK,
              "Warning",
              "Please provide a hostname to connect to");
      smd.open();
    }

    if (!StringUtils.isEmpty(current)) {
      wDbName.setText(current);
    }
  }
}
