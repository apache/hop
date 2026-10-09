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

package org.apache.hop.ui.core.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphDatabaseCapabilities;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePluginType;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaEntry;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDatabase;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

/**
 * The editor of a graph database connection: a name, the graph database type and the widgets of
 * that type. Found through the name of {@link GraphDatabaseMeta}: don't move it.
 */
@GuiPlugin(description = "Editor for graph database connection metadata")
public class GraphDatabaseMetaEditor extends MetadataEditor<GraphDatabaseMeta> {
  private static final Class<?> PKG = GraphDatabaseMetaEditor.class;

  private final GraphDatabaseMeta workingMeta;

  /** The settings per type name, so switching back and forth between types loses nothing. */
  private final Map<String, IGraphDatabase> typeMap = new HashMap<>();

  private TextVar wName;
  private CCombo wType;
  private Label wCapabilities;
  private Composite wTypeComp;
  private GuiCompositeWidgets guiCompositeWidgets;
  private boolean changingType;

  public GraphDatabaseMetaEditor(
      HopGui hopGui, MetadataManager<GraphDatabaseMeta> manager, GraphDatabaseMeta metadata) {
    super(hopGui, manager, metadata);
    this.workingMeta = new GraphDatabaseMeta(metadata);
    if (workingMeta.getGraphDatabase() != null) {
      typeMap.put(workingMeta.getGraphDatabase().getPluginName(), workingMeta.getGraphDatabase());
    }
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    wName =
        createNameField(
            parent,
            BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Name.Label"),
            middle,
            margin);

    Label wlType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlType);
    wlType.setText(BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Type.Label"));
    FormData fdlType = new FormData();
    fdlType.top = new FormAttachment(wName, margin);
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    wlType.setLayoutData(fdlType);
    wType = new CCombo(parent, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wType);
    wType.setItems(getTypeNames());
    FormData fdType = new FormData();
    fdType.top = new FormAttachment(wlType, 0, SWT.CENTER);
    fdType.left = new FormAttachment(middle, 0);
    fdType.right = new FormAttachment(100, 0);
    wType.setLayoutData(fdType);

    // What the selected type supports, read-only. The same as hop-conf --graph-database-types.
    //
    Label wlCapabilities = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlCapabilities);
    wlCapabilities.setText(
        BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Capabilities.Label"));
    FormData fdlCapabilities = new FormData();
    fdlCapabilities.top = new FormAttachment(wType, margin);
    fdlCapabilities.left = new FormAttachment(0, 0);
    fdlCapabilities.right = new FormAttachment(middle, -margin);
    wlCapabilities.setLayoutData(fdlCapabilities);
    wCapabilities = new Label(parent, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wCapabilities);
    FormData fdCapabilities = new FormData();
    fdCapabilities.top = new FormAttachment(wType, margin);
    fdCapabilities.left = new FormAttachment(middle, 0);
    fdCapabilities.right = new FormAttachment(100, 0);
    wCapabilities.setLayoutData(fdCapabilities);

    // The widgets of the selected type go here. With groups on the type's fields the composite
    // widgets add their own scrolled area.
    //
    wTypeComp = new Composite(parent, SWT.NONE);
    PropsUi.setLook(wTypeComp);
    FormLayout typeLayout = new FormLayout();
    wTypeComp.setLayout(typeLayout);
    FormData fdTypeComp = new FormData();
    fdTypeComp.top = new FormAttachment(wCapabilities, margin);
    fdTypeComp.left = new FormAttachment(0, 0);
    fdTypeComp.right = new FormAttachment(100, 0);
    fdTypeComp.bottom = new FormAttachment(100, 0);
    wTypeComp.setLayoutData(fdTypeComp);

    addTypeWidgets();
    setWidgetsContent();
    resetChanged();

    wName.addListener(SWT.Modify, e -> setChanged());
    wType.addListener(SWT.Selection, e -> changeType());
  }

  private String[] getTypeNames() {
    List<String> names = new ArrayList<>();
    for (IPlugin plugin : PluginRegistry.getInstance().getPlugins(GraphDatabasePluginType.class)) {
      names.add(plugin.getName());
    }
    names.sort(String.CASE_INSENSITIVE_ORDER);
    return names.toArray(new String[0]);
  }

  private void addTypeWidgets() {
    for (Control child : wTypeComp.getChildren()) {
      child.dispose();
    }
    guiCompositeWidgets = null;
    IGraphDatabase graphDatabase = workingMeta.getGraphDatabase();
    if (graphDatabase == null && wType.getItemCount() == 0) {
      // Graph database types come from plugins, for example the Neo4j plugin.
      //
      Label wlNoTypes = new Label(wTypeComp, SWT.WRAP);
      PropsUi.setLook(wlNoTypes);
      wlNoTypes.setText(BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.NoTypes.Message"));
      FormData fdlNoTypes = new FormData();
      fdlNoTypes.top = new FormAttachment(0, PropsUi.getMargin());
      fdlNoTypes.left = new FormAttachment(0, 0);
      fdlNoTypes.right = new FormAttachment(100, 0);
      wlNoTypes.setLayoutData(fdlNoTypes);
    } else if (graphDatabase != null) {
      guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
      guiCompositeWidgets.createCompositeWidgets(
          graphDatabase, null, wTypeComp, GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      guiCompositeWidgets.setWidgetsListener(
          new GuiCompositeWidgetsAdapter() {
            @Override
            public void widgetModified(
                GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
              setChanged();
            }
          });
    }
    wTypeComp.layout(true, true);
  }

  /** Show what the selected graph database type supports. */
  private void showCapabilities() {
    if (wCapabilities == null || wCapabilities.isDisposed()) {
      return;
    }
    IGraphDatabase graphDatabase = workingMeta.getGraphDatabase();
    if (graphDatabase == null) {
      wCapabilities.setText("");
      wCapabilities.setToolTipText("");
      return;
    }
    GraphDatabaseCapabilities capabilities = GraphDatabaseCapabilities.of(graphDatabase);
    List<String> supported = new ArrayList<>();
    StringBuilder all = new StringBuilder();
    for (Map.Entry<String, Boolean> entry : capabilities.getCapabilities().entrySet()) {
      String label = GraphDatabaseCapabilities.getCapabilityLabel(entry.getKey());
      boolean value = Boolean.TRUE.equals(entry.getValue());
      if (value) {
        supported.add(label);
      }
      all.append(label)
          .append(" : ")
          .append(
              BaseMessages.getString(
                  PKG,
                  value
                      ? "GraphDatabaseMetaEditor.Capabilities.Yes"
                      : "GraphDatabaseMetaEditor.Capabilities.No"))
          .append(Const.CR);
    }
    String text =
        supported.isEmpty()
            ? BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Capabilities.None")
            : String.join(", ", supported);
    // A label shows an ampersand as a mnemonic
    wCapabilities.setText(text.replace("&", "&&"));
    wCapabilities.setToolTipText(
        BaseMessages.getString(
                PKG,
                "GraphDatabaseMetaEditor.Capabilities.Tooltip",
                Const.NVL(capabilities.getQueryLanguage(), ""))
            + Const.CR
            + Const.CR
            + all.toString().trim());
    wCapabilities.getParent().layout(true, true);
  }

  private void changeType() {
    if (changingType) {
      return;
    }
    changingType = true;
    try {
      getWidgetsContent(workingMeta);
      String typeName = wType.getText();
      IGraphDatabase graphDatabase = typeMap.get(typeName);
      if (graphDatabase == null) {
        IPlugin plugin =
            PluginRegistry.getInstance()
                .findPluginWithName(GraphDatabasePluginType.class, typeName);
        if (plugin == null) {
          return;
        }
        graphDatabase = GraphDatabaseMeta.createGraphDatabase(plugin.getIds()[0]);
        typeMap.put(typeName, graphDatabase);
      }
      workingMeta.setGraphDatabase(graphDatabase);
      addTypeWidgets();
      setWidgetsContent();
      setChanged();
    } catch (HopException e) {
      new ErrorDialog(getShell(), "Error", "Error changing the graph database type", e);
    } finally {
      changingType = false;
    }
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(workingMeta.getName(), ""));
    IGraphDatabase graphDatabase = workingMeta.getGraphDatabase();
    if (graphDatabase == null) {
      wType.setText("");
      showCapabilities();
      return;
    }
    wType.setText(Const.NVL(graphDatabase.getPluginName(), ""));
    showCapabilities();
    if (guiCompositeWidgets != null) {
      guiCompositeWidgets.setWidgetsContents(
          graphDatabase, wTypeComp, GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
  }

  @Override
  public void getWidgetsContent(GraphDatabaseMeta meta) {
    meta.setName(wName.getText());
    meta.setGraphDatabase(workingMeta.getGraphDatabase());
    if (meta.getGraphDatabase() != null && guiCompositeWidgets != null) {
      guiCompositeWidgets.getWidgetsContents(
          meta.getGraphDatabase(), GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());
    Button wSchema = new Button(composite, SWT.PUSH);
    wSchema.setText(BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Schema.Button"));
    wSchema.setToolTipText(BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Schema.Tooltip"));
    wSchema.addListener(SWT.Selection, e -> showSchema());
    return new Button[] {wTest, wSchema};
  }

  /** Read the labels, relationship types and properties of the database and show them. */
  private void showSchema() {
    GraphDatabaseMeta meta = new GraphDatabaseMeta(workingMeta);
    getWidgetsContent(meta);
    try {
      GraphSchema schema;
      try (IGraphConnection connection = meta.connect(LogChannel.UI, manager.getVariables())) {
        if (!connection.getGraphDialect().isSupportingSchemaIntrospection()) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "GraphDatabaseMetaEditor.Schema.NotSupported",
                  connection.getGraphDialect().getId()));
        }
        schema = connection.getSchema(GraphSchema.DEFAULT_SAMPLE_SIZE);
      }
      IRowMeta rowMeta = new RowMeta();
      for (String column :
          new String[] {"Type", "Name", "Property", "Types", "StartLabels", "EndLabels"}) {
        rowMeta.addValueMeta(
            new ValueMetaString(
                BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Schema.Column." + column)));
      }
      for (String column : new String[] {"Mandatory", "Indexed", "Unique"}) {
        rowMeta.addValueMeta(
            new ValueMetaBoolean(
                BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Schema.Column." + column)));
      }
      List<Object[]> rows = new ArrayList<>();
      for (GraphSchemaEntry entry : schema.entries()) {
        rows.add(
            new Object[] {
              entry.elementType().name(),
              entry.name(),
              entry.property(),
              String.join(",", entry.propertyTypes()),
              String.join(",", entry.startLabels()),
              String.join(",", entry.endLabels()),
              entry.mandatory(),
              schema.isIndexed(entry),
              schema.isUnique(entry)
            });
      }
      new PreviewRowsDialog(
              getShell(),
              manager.getVariables(),
              SWT.NONE,
              BaseMessages.getString(
                  PKG,
                  schema.sampled()
                      ? "GraphDatabaseMetaEditor.Schema.Title.Sampled"
                      : "GraphDatabaseMetaEditor.Schema.Title",
                  meta.getName(),
                  Integer.toString(GraphSchema.DEFAULT_SAMPLE_SIZE)),
              rowMeta,
              rows)
          .open();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Schema.Error.Title"),
          BaseMessages.getString(
              PKG, "GraphDatabaseMetaEditor.Schema.Error.Message", meta.getName()),
          e);
    }
  }

  private void test() {
    GraphDatabaseMeta meta = new GraphDatabaseMeta(workingMeta);
    getWidgetsContent(meta);
    try {
      String tested = meta.test(manager.getVariables());
      MessageBox box = new MessageBox(getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Test.Success.Title"));
      box.setMessage(
          BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Test.Success.Message", tested));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Test.Error.Title"),
          BaseMessages.getString(PKG, "GraphDatabaseMetaEditor.Test.Error.Message", meta.getName()),
          e);
    }
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
