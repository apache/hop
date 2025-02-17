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

package org.apache.hop.ui.execution;

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
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPluginType;
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
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(description = "Editor for execution information location metadata")
/**
 * The editor for metadata object {@link org.apache.hop.execution.ExecutionInfoLocation} Don't move
 * this class around as it's synchronized with the {@link
 * org.apache.hop.execution.ExecutionInfoLocation} package to find the dialog.
 */
public class ExecutionInfoLocationEditor extends MetadataEditor<ExecutionInfoLocation> {
  private static final Class<?> PKG = ExecutionInfoLocationEditor.class;

  private ExecutionInfoLocation executionInfoLocation;
  private ExecutionInfoLocation workingLocation;

  private Text wName;
  private Text wDescription;
  private Text wDataLoggingDelay;
  private Text wDataLoggingInterval;
  private ComboVar wPluginType;

  private Composite wPluginSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;

  private Map<String, IExecutionInfoLocation> metaMap;

  /**
   * @param hopGui
   * @param manager
   * @param location The execution info location to edit
   */
  public ExecutionInfoLocationEditor(
      HopGui hopGui,
      MetadataManager<ExecutionInfoLocation> manager,
      ExecutionInfoLocation location) {
    super(hopGui, manager, location);

    this.executionInfoLocation = location;
    this.workingLocation = new ExecutionInfoLocation(location);
    metaMap = populateMetaMap();
    if (workingLocation.getExecutionInfoLocation() != null) {
      metaMap.put(
          workingLocation.getExecutionInfoLocation().getPluginName(),
          workingLocation.getExecutionInfoLocation());
    }
  }

  private Map<String, IExecutionInfoLocation> populateMetaMap() {
    metaMap = new HashMap<>();
    List<IPlugin> plugins =
        PluginRegistry.getInstance().getPlugins(ExecutionInfoLocationPluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IExecutionInfoLocation location =
            PluginRegistry.getInstance().loadClass(plugin, IExecutionInfoLocation.class);

        location.setPluginId(plugin.getIds()[0]);
        location.setPluginName(plugin.getName());

        metaMap.put(plugin.getName(), location);
      } catch (Exception e) {
        HopGui.getInstance()
            .getLog()
            .logError("Error instantiating execution information location plugin", e);
      }
    }

    return metaMap;
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    // Create a tabbed interface instead of the confusing left-hand side options
    // This will make it more conforming the rest.
    //
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    ScrolledComposite wMainSComp = new ScrolledComposite(parent, SWT.V_SCROLL | SWT.H_SCROLL);
    wMainSComp.setLayout(new FillLayout());

    Composite wMainComp = new Composite(wMainSComp, SWT.NONE);
    PropsUi.setLook(wMainComp);

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;
    wMainComp.setLayout(mainLayout);

    // The generic widgets: name, description and pipeline engine type
    //
    // What's the name
    //
    Label wlName = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ExecutionInfoLocationEditor.label.name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin * 2);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin); // To the right of the label
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlDescription = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(
        BaseMessages.getString(PKG, "ExecutionInfoLocationEditor.label.description"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin * 2);
    fdlDescription.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDescription.right = new FormAttachment(middle, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, margin); // To the right of the label
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    Label wlDataLoggingDelay = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlDataLoggingDelay);
    wlDataLoggingDelay.setText(
        BaseMessages.getString(PKG, "ExecutionInfoLocationEditor.label.DataLoggingDelay"));
    FormData fdlDataLoggingDelay = new FormData();
    fdlDataLoggingDelay.top = new FormAttachment(lastControl, margin * 2);
    fdlDataLoggingDelay.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDataLoggingDelay.right = new FormAttachment(middle, 0);
    wlDataLoggingDelay.setLayoutData(fdlDataLoggingDelay);
    wDataLoggingDelay = new Text(wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataLoggingDelay);
    FormData fdDataLoggingDelay = new FormData();
    fdDataLoggingDelay.top = new FormAttachment(wlDataLoggingDelay, 0, SWT.CENTER);
    fdDataLoggingDelay.left = new FormAttachment(middle, margin); // To the right of the label
    fdDataLoggingDelay.right = new FormAttachment(100, 0);
    wDataLoggingDelay.setLayoutData(fdDataLoggingDelay);
    lastControl = wDataLoggingDelay;

    Label wlDataLoggingInterval = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlDataLoggingInterval);
    wlDataLoggingInterval.setText(
        BaseMessages.getString(PKG, "ExecutionInfoLocationEditor.label.DataLoggingInterval"));
    FormData fdlDataLoggingInterval = new FormData();
    fdlDataLoggingInterval.top = new FormAttachment(lastControl, margin * 2);
    fdlDataLoggingInterval.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDataLoggingInterval.right = new FormAttachment(middle, 0);
    wlDataLoggingInterval.setLayoutData(fdlDataLoggingInterval);
    wDataLoggingInterval = new Text(wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataLoggingInterval);
    FormData fdDataLoggingInterval = new FormData();
    fdDataLoggingInterval.top = new FormAttachment(wlDataLoggingInterval, 0, SWT.CENTER);
    fdDataLoggingInterval.left = new FormAttachment(middle, margin); // To the right of the label
    fdDataLoggingInterval.right = new FormAttachment(100, 0);
    wDataLoggingInterval.setLayoutData(fdDataLoggingInterval);
    lastControl = wDataLoggingInterval;

    // What's the type of location plugin?
    //
    Label wlPluginType = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlPluginType);
    wlPluginType.setText(
        BaseMessages.getString(PKG, "ExecutionInfoLocationEditor.label.locationType"));
    FormData fdlPluginType = new FormData();
    fdlPluginType.top = new FormAttachment(lastControl, margin * 2);
    fdlPluginType.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPluginType.right = new FormAttachment(middle, 0);
    wlPluginType.setLayoutData(fdlPluginType);
    wPluginType =
        new ComboVar(manager.getVariables(), wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPluginType);
    wPluginType.setItems(getPluginTypes());
    FormData fdPluginType = new FormData();
    fdPluginType.top = new FormAttachment(wlPluginType, 0, SWT.CENTER);
    fdPluginType.left = new FormAttachment(middle, margin); // To the right of the label
    fdPluginType.right = new FormAttachment(100, 0);
    wPluginType.setLayoutData(fdPluginType);
    lastControl = wPluginType;

    // Add a composite area
    //
    wPluginSpecificComp = new Composite(wMainComp, SWT.BACKGROUND);
    PropsUi.setLook(wPluginSpecificComp);
    wPluginSpecificComp.setLayout(new FormLayout());
    FormData fdPluginSpecificComp = new FormData();
    fdPluginSpecificComp.left = new FormAttachment(0, 0);
    fdPluginSpecificComp.right = new FormAttachment(100, 0);
    fdPluginSpecificComp.top = new FormAttachment(lastControl, margin);
    fdPluginSpecificComp.bottom = new FormAttachment(100, 0);
    wPluginSpecificComp.setLayoutData(fdPluginSpecificComp);

    // Add the plugin specific widgets
    //
    addGuiCompositeWidgets();

    FormData fdMainSComp = new FormData();
    fdMainSComp.top = new FormAttachment(0, 0);
    fdMainSComp.left = new FormAttachment(0, 0);
    fdMainSComp.right = new FormAttachment(95, 0);
    fdMainSComp.bottom = new FormAttachment(95, 0);
    wMainSComp.setLayoutData(fdMainSComp);

    FormData fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment(0, 0);
    fdMainComp.top = new FormAttachment(0, 0);
    fdMainComp.right = new FormAttachment(100, 0);
    fdMainComp.bottom = new FormAttachment(100, 0);
    wMainComp.setLayoutData(fdMainComp);

    wMainComp.pack();
    Rectangle mainBounds = wMainComp.getBounds();

    wMainSComp.setContent(wMainComp);
    wMainSComp.setExpandHorizontal(true);
    wMainSComp.setExpandVertical(true);
    wMainSComp.setMinWidth(mainBounds.width);
    wMainSComp.setMinHeight(mainBounds.height);

    setWidgetsContent();

    // Some widget set changed
    resetChanged();

    // Add listeners...
    //
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wDescription.addListener(SWT.Modify, modifyListener);
    wDataLoggingDelay.addListener(SWT.Modify, modifyListener);
    wDataLoggingInterval.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, e -> changeConnectionType());
  }

  private void addGuiCompositeWidgets() {

    // Remove existing children
    //
    for (Control child : wPluginSpecificComp.getChildren()) {
      child.dispose();
    }

    if (workingLocation.getExecutionInfoLocation() != null) {
      guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
      guiCompositeWidgets.createCompositeWidgets(
          workingLocation.getExecutionInfoLocation(),
          null,
          wPluginSpecificComp,
          ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
          wDescription);
      guiCompositeWidgets.setWidgetsListener(
          new GuiCompositeWidgetsAdapter() {
            @Override
            public void widgetModified(
                GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
              setChanged();
            }
          });
      guiCompositeWidgets.setCompositeButtonsListener(
          sourceObject ->
              guiCompositeWidgets.getWidgetsContents(
                  sourceObject, ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID));
    }

    wPluginSpecificComp.layout(true, true);
  }

  private final AtomicBoolean busyChangingPluginType = new AtomicBoolean(false);

  private void changeConnectionType() {

    if (busyChangingPluginType.get()) {
      return;
    }
    busyChangingPluginType.set(true);

    // Capture any information on the widgets
    //
    getWidgetsContent(workingLocation);

    // Save the state of this type, so we can switch back and forth
    if (workingLocation.getExecutionInfoLocation() != null) {
      metaMap.put(
          workingLocation.getExecutionInfoLocation().getPluginName(),
          workingLocation.getExecutionInfoLocation());
    }

    changeWorkingLocation(workingLocation);

    // Add the plugin widgets
    //
    addGuiCompositeWidgets();

    // Put the data back
    //
    setWidgetsContent();

    busyChangingPluginType.set(false);
  }

  @Override
  public void save() throws HopException {
    changeWorkingLocation(executionInfoLocation);

    super.save();
  }

  @Override
  public void setWidgetsContent() {

    wName.setText(Const.NVL(workingLocation.getName(), ""));
    wDescription.setText(Const.NVL(workingLocation.getDescription(), ""));
    wDataLoggingDelay.setText(Const.NVL(workingLocation.getDataLoggingDelay(), ""));
    wDataLoggingInterval.setText(Const.NVL(workingLocation.getDataLoggingInterval(), ""));

    if (workingLocation.getExecutionInfoLocation() != null) {
      wPluginType.setText(
          Const.NVL(workingLocation.getExecutionInfoLocation().getPluginName(), ""));
      guiCompositeWidgets.setWidgetsContents(
          workingLocation.getExecutionInfoLocation(),
          wPluginSpecificComp,
          ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID);
    } else {
      wPluginType.setText("");
    }
  }

  @Override
  public void getWidgetsContent(ExecutionInfoLocation location) {

    location.setName(wName.getText());
    location.setDescription(wDescription.getText());
    location.setDataLoggingDelay(wDataLoggingDelay.getText());
    location.setDataLoggingInterval(wDataLoggingInterval.getText());

    // Get the plugin specific information from the widgets on the screen
    //
    if (location.getExecutionInfoLocation() != null
        && guiCompositeWidgets != null
        && !guiCompositeWidgets.getWidgetsMap().isEmpty()) {
      guiCompositeWidgets.getWidgetsContents(
          location.getExecutionInfoLocation(), ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
  }

  private void changeWorkingLocation(ExecutionInfoLocation meta) {
    String pluginName = wPluginType.getText();
    IExecutionInfoLocation old = metaMap.get(pluginName);
    if (old != null) {
      // Switch to the right plugin type
      //
      meta.setExecutionInfoLocation(old);
    } else {
      meta.setExecutionInfoLocation(null);
    }
  }

  private String[] getPluginTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(ExecutionInfoLocationPluginType.class);
    String[] types = new String[plugins.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = plugins.get(i).getName();
    }
    Arrays.sort(types, String.CASE_INSENSITIVE_ORDER);
    return types;
  }
}
