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

package org.apache.hop.ui.core.variables.resolver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolverPluginType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

@GuiPlugin(description = "This is the editor for variable resolver metadata")

/**
 * The metadata editor for VariableResolver. Don't move this class around as it's synchronized with
 * the VariableResolver package to find the dialog.
 */
public class VariableResolverEditor extends MetadataEditor<VariableResolver> {
  private static final Class<?> PKG = VariableResolverEditor.class;

  private Text wName;
  private Text wDescription;
  private Combo wResolverType;

  private Composite wResolverSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;
  private Map<String, IVariableResolver> metaMap;

  private final PropsUi props;

  /**
   * @param hopGui The hop GUI
   * @param manager The metadata
   * @param resolver The object to edit
   */
  public VariableResolverEditor(
      HopGui hopGui, MetadataManager<VariableResolver> manager, VariableResolver resolver) {
    super(hopGui, manager, resolver);
    props = PropsUi.getInstance();
    populateMetaMap();
  }

  @Override
  public void createControl(Composite parent) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    PropsUi.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "VariableResolverEditor.label.Name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, margin);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);

    // What's the description
    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "VariableResolverEditor.label.Description"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(wName, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, margin);
    fdDescription.left = new FormAttachment(0, 0);
    fdDescription.right = new FormAttachment(wIcon, -margin);
    wDescription.setLayoutData(fdDescription);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wDescription, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // What's the type of database access?
    //
    Label wlResolverType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlResolverType);
    wlResolverType.setText(
        BaseMessages.getString(PKG, "VariableResolverEditor.label.ResolverType"));
    FormData fdlConnectionType = new FormData();
    fdlConnectionType.top = new FormAttachment(spacer, margin);
    fdlConnectionType.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlConnectionType.right = new FormAttachment(middle, -margin);
    wlResolverType.setLayoutData(fdlConnectionType);

    ToolBar wToolBar = new ToolBar(parent, SWT.FLAT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.right = new FormAttachment(100, 0);
    fdToolBar.top = new FormAttachment(spacer, margin);
    wToolBar.setLayoutData(fdToolBar);
    PropsUi.setLook(wToolBar, Props.WIDGET_STYLE_DEFAULT);

    ToolItem item = new ToolItem(wToolBar, SWT.PUSH);
    item.setImage(GuiResource.getInstance().getImageHelp());
    item.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.Help"));
    item.addListener(SWT.Selection, e -> onHelpVariableResolverType());

    wResolverType = new Combo(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResolverType.setItems(getResolverTypes());
    PropsUi.setLook(wResolverType);
    FormData fdConnectionType = new FormData();
    fdConnectionType.top = new FormAttachment(wlResolverType, 0, SWT.CENTER);
    fdConnectionType.left = new FormAttachment(middle, 0); // To the right of the label
    fdConnectionType.right = new FormAttachment(wToolBar, -margin);
    wResolverType.setLayoutData(fdConnectionType);
    Control lastControl = wResolverType;

    // Add a composite area
    //
    wResolverSpecificComp = new Composite(parent, SWT.BACKGROUND);
    wResolverSpecificComp.setLayout(new FormLayout());
    FormData fdResolverSpecificComp = new FormData();
    fdResolverSpecificComp.left = new FormAttachment(0, 0);
    fdResolverSpecificComp.right = new FormAttachment(100, 0);
    fdResolverSpecificComp.top = new FormAttachment(lastControl, 0);
    fdResolverSpecificComp.bottom = new FormAttachment(100, 0);
    wResolverSpecificComp.setLayoutData(fdResolverSpecificComp);
    PropsUi.setLook(wResolverSpecificComp);
    // lastControl = wResolverSpecificComp;

    // Now add the database plugin specific widgets
    //
    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        getMetadata().getIResolver(),
        null,
        wResolverSpecificComp,
        VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
        null);

    // Add listener to detect change
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });

    setWidgetsContent();

    // Some widget set changed
    resetChanged();

    // Add listener to detect change after loading data
    Listener modifyListener =
        event -> {
          setChanged();
          MetadataPerspective.getInstance().updateEditor(this);
        };
    wName.addListener(SWT.Modify, modifyListener);
    wResolverType.addListener(SWT.Modify, modifyListener);
    wResolverType.addListener(SWT.Modify, event -> changeResolverType());
  }

  private AtomicBoolean busyChangingConnectionType = new AtomicBoolean(false);

  private void changeResolverType() {
    if (busyChangingConnectionType.get()) {
      return;
    }
    busyChangingConnectionType.set(true);

    // Now change the data type
    //
    String newTypeName = wResolverType.getText();
    wResolverType.setText(newTypeName);

    IVariableResolver iResolver = metaMap.get(newTypeName);
    getMetadata().setIResolver(iResolver);

    // Remove existing children
    //
    for (Control child : wResolverSpecificComp.getChildren()) {
      child.dispose();
    }

    // Re-add the widgets
    //
    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        iResolver,
        null,
        wResolverSpecificComp,
        VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
        null);
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });

    // Put the data back
    //
    setWidgetsContent();

    getShell().layout(true, true);

    busyChangingConnectionType.set(false);
  }

  private void enableFields() {
    // Perhaps later
  }

  @Override
  public void setWidgetsContent() {
    VariableResolver resolver = this.getMetadata();
    wName.setText(Const.NVL(resolver.getName(), ""));
    wDescription.setText(Const.NVL(resolver.getDescription(), ""));
    wResolverType.setText(Const.NVL(resolver.getPluginName(), ""));

    guiCompositeWidgets.setWidgetsContents(
        resolver.getIResolver(),
        wResolverSpecificComp,
        VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID);

    enableFields();
  }

  @Override
  public void getWidgetsContent(VariableResolver meta) {

    meta.setName(wName.getText());
    meta.setDescription(wDescription.getText());

    // TODO meta.setResolverType(wResolverType.getText());

    // Get the resolver specific information
    //
    guiCompositeWidgets.getWidgetsContents(
        meta.getIResolver(), VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID);
  }

  private String[] getResolverTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(VariableResolverPluginType.class);
    String[] types = new String[plugins.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = plugins.get(i).getName();
    }
    Arrays.sort(types, String.CASE_INSENSITIVE_ORDER);
    return types;
  }

  private void onHelpVariableResolverType() {
    PluginRegistry registry = PluginRegistry.getInstance();
    String name = wResolverType.getText();
    for (IPlugin plugin : registry.getPlugins(VariableResolverPluginType.class)) {
      if (plugin.getName().equals(name)) {
        HelpUtils.openHelp(getShell(), plugin);
        break;
      }
    }
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  private Map<String, IVariableResolver> populateMetaMap() {
    metaMap = new HashMap<>();
    List<IPlugin> plugins =
        PluginRegistry.getInstance().getPlugins(VariableResolverPluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IVariableResolver resolver =
            (IVariableResolver) PluginRegistry.getInstance().loadClass(plugin);

        metaMap.put(plugin.getName(), resolver);
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error instantiating variable resolver metadata", e);
      }
    }

    return metaMap;
  }
}
