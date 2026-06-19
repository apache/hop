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
 *
 */

package org.apache.hop.ui.datastream.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.datastream.plugin.DataStreamPluginType;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(description = "This is the editor for DataStream metadata")
public class DataStreamMetaEditor extends MetadataEditor<DataStreamMeta> {
  private static final Class<?> PKG = DataStreamMetaEditor.class;

  private final DataStreamMeta dataStreamMeta;
  private final DataStreamMeta workingDataStreamMeta;

  // Name, description and plugin type are fixed.
  private Text wName;
  private Text wDescription;
  private ComboVar wPluginType;

  // Then come the plugin specific fields from the data stream plugin
  //
  private Composite wPluginComp;
  private GuiCompositeWidgets guiPluginWidgets;
  private Map<String, IDataStream> metaMap;

  public DataStreamMetaEditor(
      HopGui hopGui, MetadataManager<DataStreamMeta> manager, DataStreamMeta dataStreamMeta) {
    super(hopGui, manager, dataStreamMeta);
    this.dataStreamMeta = dataStreamMeta;
    this.workingDataStreamMeta = new DataStreamMeta(dataStreamMeta);

    metaMap = populateMetaMap();
    if (workingDataStreamMeta.getDataStream() != null) {
      metaMap.put(
          workingDataStreamMeta.getDataStream().getPluginName(),
          workingDataStreamMeta.getDataStream());
    }
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public void createControl(Composite parent) {
    ScrolledComposite wsPluginComp;
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    // The name
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "DataStream.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "DataStream.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0); // To the right of the label
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // What's the type of engine?
    //
    Label wlPluginType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPluginType);
    wlPluginType.setText(BaseMessages.getString(PKG, "DataStream.PluginType.Label"));
    FormData fdlPluginType = new FormData();
    fdlPluginType.top = new FormAttachment(lastControl, margin);
    fdlPluginType.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPluginType.right = new FormAttachment(middle, -margin);
    wlPluginType.setLayoutData(fdlPluginType);
    wPluginType = new ComboVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPluginType);
    wPluginType.setItems(getPluginTypes());
    FormData fdPluginType = new FormData();
    fdPluginType.top = new FormAttachment(wlPluginType, 0, SWT.CENTER);
    fdPluginType.left = new FormAttachment(middle, 0); // To the right of the label
    fdPluginType.right = new FormAttachment(100, 0);
    wPluginType.setLayoutData(fdPluginType);
    lastControl = wPluginType;

    // Add a composite area
    //
    wsPluginComp = new ScrolledComposite(parent, SWT.V_SCROLL | SWT.H_SCROLL);
    wsPluginComp.setLayout(new FormLayout());
    FormData fdsPluginComp = new FormData();
    fdsPluginComp.left = new FormAttachment(0, 0);
    fdsPluginComp.top = new FormAttachment(lastControl, margin);
    fdsPluginComp.right = new FormAttachment(100, 0);
    fdsPluginComp.bottom = new FormAttachment(100, 0);
    wsPluginComp.setLayoutData(fdsPluginComp);

    wPluginComp = new Composite(wsPluginComp, SWT.BACKGROUND);
    PropsUi.setLook(wPluginComp);
    wPluginComp.setLayout(new FormLayout());
    FormData fdPluginComp = new FormData();
    fdPluginComp.left = new FormAttachment(0, 0);
    fdPluginComp.right = new FormAttachment(100, 0);
    fdPluginComp.top = new FormAttachment(lastControl, margin);
    fdPluginComp.bottom = new FormAttachment(100, 0);
    wPluginComp.setLayoutData(fdPluginComp);

    wsPluginComp.setContent(wPluginComp);

    // Add the plugin specific widgets
    //
    addGuiCompositeWidgets();

    wPluginComp.layout();
    wsPluginComp.setExpandHorizontal(true);
    wsPluginComp.setExpandVertical(true);
    Rectangle bounds = wPluginComp.getBounds();
    wsPluginComp.setMinWidth(bounds.width);
    wsPluginComp.setMinHeight(bounds.height);

    setWidgetsContent();

    // Some widget set changed
    resetChanged();

    // Add listeners...
    //
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wDescription.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, e -> changeDataStreamType());
  }

  private final AtomicBoolean busyChangingPluginType = new AtomicBoolean(false);

  private void changeDataStreamType() {
    if (busyChangingPluginType.get()) {
      return;
    }
    busyChangingPluginType.set(true);

    try {
      // Capture any information on the widgets
      //
      getWidgetsContent(workingDataStreamMeta);

      // Save the state of this type so we can switch back and forth
      if (workingDataStreamMeta.getDataStream() != null) {
        metaMap.put(
            workingDataStreamMeta.getDataStream().getPluginId(),
            workingDataStreamMeta.getDataStream());
      }

      changeWorkingDataStream(workingDataStreamMeta);

      // Add the plugin widgets
      //
      addGuiCompositeWidgets();

      // Put the data back
      //
      setWidgetsContent();
    } finally {
      busyChangingPluginType.set(false);
    }
  }

  @Override
  public void save() throws HopException {
    changeWorkingDataStream(dataStreamMeta);
    super.save();
  }

  private void changeWorkingDataStream(DataStreamMeta dataStreamMeta) {
    String pluginName = wPluginType.getText();
    IDataStream dataStream = metaMap.get(pluginName);
    dataStreamMeta.setDataStream(dataStream);
  }

  private void addGuiCompositeWidgets() {
    // Remove existing children
    //
    for (Control child : wPluginComp.getChildren()) {
      child.dispose();
    }

    if (workingDataStreamMeta.getDataStream() == null) {
      return;
    }
    guiPluginWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiPluginWidgets.createCompositeWidgets(
        workingDataStreamMeta.getDataStream(),
        null,
        wPluginComp,
        DataStreamMeta.GUI_WIDGETS_PARENT_ID,
        null);
    guiPluginWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(workingDataStreamMeta.getName(), ""));
    wDescription.setText(Const.NVL(workingDataStreamMeta.getDescription(), ""));
    if (workingDataStreamMeta.getDataStream() != null) {
      wPluginType.setText(Const.NVL(workingDataStreamMeta.getDataStream().getPluginName(), ""));
      guiPluginWidgets.setWidgetsContents(
          workingDataStreamMeta.getDataStream(), wPluginComp, DataStreamMeta.GUI_WIDGETS_PARENT_ID);
    } else {
      wPluginType.setText("");
    }
  }

  @Override
  public void getWidgetsContent(DataStreamMeta meta) {
    meta.setName(wName.getText());
    meta.setDescription(wDescription.getText());

    // Get the plugin specific information from the widgets on the screen
    //
    if (meta.getDataStream() != null
        && guiPluginWidgets != null
        && !guiPluginWidgets.getWidgetsMap().isEmpty()) {
      guiPluginWidgets.getWidgetsContents(
          meta.getDataStream(), DataStreamMeta.GUI_WIDGETS_PARENT_ID);
    }
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  private String[] getPluginTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(DataStreamPluginType.class);
    String[] types = new String[plugins.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = plugins.get(i).getName();
    }
    Arrays.sort(types, String.CASE_INSENSITIVE_ORDER);
    return types;
  }

  private Map<String, IDataStream> populateMetaMap() {
    metaMap = new HashMap<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(DataStreamPluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IDataStream dataStream = PluginRegistry.getInstance().loadClass(plugin, IDataStream.class);
        DataStreamPlugin annotation = dataStream.getClass().getAnnotation(DataStreamPlugin.class);
        dataStream.setPluginId(annotation.id());
        dataStream.setPluginName(annotation.name());
        metaMap.put(dataStream.getPluginName(), dataStream);
      } catch (Exception e) {
        HopGui.getInstance()
            .getLog()
            .logError("Error instantiating data stream plugin: " + plugin.getIds()[0], e);
      }
    }

    return metaMap;
  }
}
