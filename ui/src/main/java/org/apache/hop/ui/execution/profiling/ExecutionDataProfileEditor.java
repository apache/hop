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

package org.apache.hop.ui.execution.profiling;

import java.util.ArrayList;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerPluginType;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * The editor for metadata object {@link org.apache.hop.execution.profiling.ExecutionDataProfile}
 * Don't move this class around as it's synchronized with the {@link ExecutionDataProfile} package
 * to find this editor.
 */
@GuiPlugin(description = "Editor for execution data profile metadata")
public class ExecutionDataProfileEditor extends MetadataEditor<ExecutionDataProfile> {
  private static final Class<?> PKG = ExecutionDataProfileEditor.class;

  private ExecutionDataProfile executionDataProfile;
  private ExecutionDataProfile workingProfile;

  private Text wName;
  private Text wDescription;
  private org.eclipse.swt.widgets.List wSamplers;

  private Composite wPluginSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;

  private Map<String, IExecutionDataSampler> metaMap;

  /**
   * @param hopGui
   * @param manager
   * @param profile The execution info profile to edit
   */
  public ExecutionDataProfileEditor(
      HopGui hopGui, MetadataManager<ExecutionDataProfile> manager, ExecutionDataProfile profile) {
    super(hopGui, manager, profile);

    this.executionDataProfile = profile;
    this.workingProfile = new ExecutionDataProfile(profile);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    // Create a tabbed interface instead of the confusing left-hand side options
    // This will make it more conforming the rest.
    //
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // The generic widgets: name, description and pipeline engine type
    //
    // What's the name
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "ExecutionDataProfileEditor.label.name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin * 2);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin); // To the right of the label
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(
        BaseMessages.getString(PKG, "ExecutionDataProfileEditor.label.description"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin * 2);
    fdlDescription.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDescription.right = new FormAttachment(middle, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, margin); // To the right of the label
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    Label wlSamplers = new Label(parent, SWT.LEFT);
    PropsUi.setLook(wlSamplers);
    wlSamplers.setText(BaseMessages.getString(PKG, "ExecutionDataProfileEditor.label.Samplers"));
    FormData fdlSamplers = new FormData();
    fdlSamplers.top = new FormAttachment(lastControl, margin * 2);
    fdlSamplers.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSamplers.right = new FormAttachment(middle, 0);
    wlSamplers.setLayoutData(fdlSamplers);
    lastControl = wlSamplers;

    ToolBar wSamplersToolBar = new ToolBar(parent, SWT.NONE);
    ToolItem addSamplersItem = new ToolItem(wSamplersToolBar, SWT.PUSH);
    addSamplersItem.setImage(GuiResource.getInstance().getImageAdd());
    addSamplersItem.setToolTipText(
        BaseMessages.getString(PKG, "ExecutionDataProfileEditor.Button.AddSamplers"));
    addSamplersItem.addListener(SWT.Selection, e -> addSamplers());
    addSamplersItem.addListener(SWT.Selection, e -> setChanged());
    ToolItem deleteSamplerItem = new ToolItem(wSamplersToolBar, SWT.PUSH);
    deleteSamplerItem.setImage(GuiResource.getInstance().getImageDelete());
    deleteSamplerItem.setToolTipText(
        BaseMessages.getString(PKG, "ExecutionDataProfileEditor.Button.DeleteSampler"));
    deleteSamplerItem.addListener(SWT.Selection, e -> deleteSampler());
    deleteSamplerItem.addListener(SWT.Selection, e -> setChanged());
    FormData fdSamplersToolBar = new FormData();
    fdSamplersToolBar.left = new FormAttachment(0, 0);
    fdSamplersToolBar.right = new FormAttachment(middle, 0);
    fdSamplersToolBar.top = new FormAttachment(lastControl, margin);
    wSamplersToolBar.setLayoutData(fdSamplersToolBar);
    lastControl = wSamplersToolBar;

    wSamplers = new org.eclipse.swt.widgets.List(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSamplers);
    FormData fdSamplers = new FormData();
    fdSamplers.top = new FormAttachment(lastControl, margin);
    fdSamplers.left = new FormAttachment(0, 0); // To the right of the label
    fdSamplers.right = new FormAttachment(middle, 0);
    fdSamplers.bottom = new FormAttachment(lastControl, (int) (150 * props.getZoomFactor()));
    wSamplers.setLayoutData(fdSamplers);
    lastControl = wSamplers;

    // Add a composite area
    //
    wPluginSpecificComp = new Composite(parent, SWT.BACKGROUND);
    PropsUi.setLook(wPluginSpecificComp);
    wPluginSpecificComp.setLayout(new FormLayout());
    FormData fdPluginSpecificComp = new FormData();
    fdPluginSpecificComp.left = new FormAttachment(lastControl, margin);
    fdPluginSpecificComp.right = new FormAttachment(100, 0);
    fdPluginSpecificComp.top = new FormAttachment(wlSamplers, margin);
    fdPluginSpecificComp.bottom = new FormAttachment(100, 0);
    wPluginSpecificComp.setLayoutData(fdPluginSpecificComp);

    // Add the plugin specific widgets
    //
    addSamplerPluginWidgets();

    setWidgetsContent();

    // Some widget set changed
    resetChanged();

    // Add listeners...
    //
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wDescription.addListener(SWT.Modify, modifyListener);
    wSamplers.addListener(SWT.Selection, e -> addSamplerPluginWidgets());
  }

  private IExecutionDataSampler previousSampler;

  private void addSamplerPluginWidgets() {

    savePreviousSampler();

    removeSamplerWidgets();

    // Get the selected sampler
    //
    int selectionIndex = wSamplers.getSelectionIndex();
    if (selectionIndex < 0) {
      return;
    }

    IExecutionDataSampler sampler = workingProfile.getSamplers().get(selectionIndex);

    // Draw the sampler
    //
    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        sampler,
        null,
        wPluginSpecificComp,
        ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
        wDescription);

    // Set the content while we're at it.
    //
    guiCompositeWidgets.setWidgetsContents(
        sampler, wPluginSpecificComp, ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID);

    // Flag as changed if anything is modified
    //
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });

    previousSampler = sampler;
  }

  /** Remove existing children */
  private void removeSamplerWidgets() {
    for (Control child : wPluginSpecificComp.getChildren()) {
      child.dispose();
    }
  }

  private void savePreviousSampler() {
    if (previousSampler != null) {
      // Grab the settings before removing the widgets.
      //
      guiCompositeWidgets.getWidgetsContents(
          previousSampler, ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
  }

  @Override
  public void setWidgetsContent() {

    wName.setText(Const.NVL(workingProfile.getName(), ""));
    wDescription.setText(Const.NVL(workingProfile.getDescription(), ""));

    // Add all the sampler plugins in the profile
    //
    for (IExecutionDataSampler sampler : workingProfile.getSamplers()) {
      wSamplers.add(sampler.getPluginName());
    }

    // Select the first by default.  This will fill in content and widgets automatically
    //
    if (wSamplers.getItemCount() > 0) {
      wSamplers.setSelection(0);
    }

    // Get the sampler specific widgets
    //
    addSamplerPluginWidgets();
  }

  @Override
  public void getWidgetsContent(ExecutionDataProfile profile) {

    profile.setName(wName.getText());
    profile.setDescription(wDescription.getText());

    savePreviousSampler();

    // Copy the samplers from the working profile
    //
    profile.getSamplers().clear();
    profile.getSamplers().addAll(workingProfile.getSamplers());
  }

  private void addSamplers() {
    try {
      // Lets' get a list of all the samplers that aren't added yet.
      // We start with all the samplers and then remove the ones we alread have.
      //
      java.util.List<IExecutionDataSampler> samplers = new ArrayList<>();
      for (IPlugin plugin :
          PluginRegistry.getInstance().getPlugins(ExecutionDataSamplerPluginType.class)) {
        IExecutionDataSampler sampler =
            PluginRegistry.getInstance().loadClass(plugin, IExecutionDataSampler.class);
        sampler.setPluginId(plugin.getIds()[0]);
        sampler.setPluginName(plugin.getName());
        samplers.add(sampler);
      }
      samplers.removeAll(workingProfile.getSamplers());

      if (samplers.isEmpty()) {
        // Nothing to add, sorry
        return;
      }

      String[] samplerNames = new String[samplers.size()];
      for (int i = 0; i < samplerNames.length; i++) {
        samplerNames[i] = samplers.get(i).getPluginName();
      }

      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              getShell(),
              samplerNames,
              BaseMessages.getString(PKG, "ExecutionDataProfileEditor.AddSamplers.ShellText"),
              BaseMessages.getString(PKG, "ExecutionDataProfileEditor.AddSamplers.Message"));
      dialog.setMulti(true);
      String selection = dialog.open();
      if (selection != null) {
        for (int index : dialog.getSelectionIndeces()) {
          IExecutionDataSampler sampler = samplers.get(index);
          workingProfile.getSamplers().add(sampler);
          wSamplers.add(sampler.getPluginName());
        }
        // Select the last added sampler
        //
        wSamplers.setSelection(wSamplers.getItemCount() - 1);
        savePreviousSampler();
        addSamplerPluginWidgets();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error adding samplers", e);
    }
  }

  private void deleteSampler() {
    try {
      int index = wSamplers.getSelectionIndex();
      if (index < 0) {
        // Nothing selected to delete
        return;
      }
      IExecutionDataSampler sampler = workingProfile.getSamplers().get(index);
      workingProfile.getSamplers().remove(sampler);
      wSamplers.remove(sampler.getPluginName());
      previousSampler = null;
      removeSamplerWidgets();
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error deleting samplers", e);
    }
  }
}
