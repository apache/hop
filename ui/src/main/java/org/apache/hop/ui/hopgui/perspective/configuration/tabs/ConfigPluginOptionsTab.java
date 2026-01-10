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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

@GuiPlugin
public class ConfigPluginOptionsTab {

  public static final String GUI_WIDGETS_PARENT_ID = "EnterOptionsDialog-GuiWidgetsParent";

  private static Map<String, Object> pluginDataMap = new HashMap<>();
  private Composite wPluginComposite;

  public ConfigPluginOptionsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addGeneralOptionsTab() method.
  }

  @GuiTab(
      id = "10200-config-perspective-plugins-options-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "Plugins options tab")
  public void addPluginOptionsTab(CTabFolder wTabFolder) {
    Shell shell = wTabFolder.getShell();

    CTabItem wPluginsTab = new CTabItem(wTabFolder, SWT.NONE);
    wPluginsTab.setFont(GuiResource.getInstance().getFontDefault());
    wPluginsTab.setText("Plugins");
    wPluginsTab.setImage(GuiResource.getInstance().getImagePlugin());

    Composite wPluginsTabComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wPluginsTabComp);

    FormLayout lookLayout = new FormLayout();
    lookLayout.marginWidth = PropsUi.getFormMargin();
    lookLayout.marginHeight = PropsUi.getFormMargin();
    wPluginsTabComp.setLayout(lookLayout);

    // A composite to show settings for a plugin - takes full width
    //
    wPluginComposite = new Composite(wPluginsTabComp, SWT.NO_BACKGROUND);
    PropsUi.setLook(wPluginComposite);
    FormData fdPluginComposite = new FormData();
    fdPluginComposite.left = new FormAttachment(0, 0);
    fdPluginComposite.right = new FormAttachment(100, 0);
    fdPluginComposite.top = new FormAttachment(0, 0);
    fdPluginComposite.bottom = new FormAttachment(100, 0);
    wPluginComposite.setLayoutData(fdPluginComposite);
    wPluginComposite.setLayout(new FormLayout());

    // Load all configuration plugins and store their data
    //
    pluginDataMap.clear();
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    java.util.List<IPlugin> configPlugins = pluginRegistry.getPlugins(ConfigPluginType.class);
    for (IPlugin configPlugin : configPlugins) {
      try {
        Object emptySourceData = pluginRegistry.loadClass(configPlugin);
        GuiPlugin annotation = emptySourceData.getClass().getAnnotation(GuiPlugin.class);
        if (annotation != null) {
          // Load the instance
          //
          Method method = emptySourceData.getClass().getMethod("getInstance");
          Object sourceData = method.invoke(null, (Object[]) null);

          // This config plugin is also a GUI plugin
          // Store the plugin data in the map
          //
          String name =
              Const.NVL(
                  TranslateUtil.translate(annotation.description(), emptySourceData.getClass()),
                  "");
          pluginDataMap.put(name, sourceData);
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            "Error",
            "Error handling configuration options for config / GUI plugin "
                + configPlugin.getIds()[0],
            e);
      }
    }

    // Show a helpful message when no plugin is selected
    showPluginInstructions(wPluginComposite);

    FormData fdPluginsTabComp = new FormData();
    fdPluginsTabComp.left = new FormAttachment(0, 0);
    fdPluginsTabComp.right = new FormAttachment(100, 0);
    fdPluginsTabComp.top = new FormAttachment(0, 0);
    fdPluginsTabComp.bottom = new FormAttachment(100, 100);
    wPluginsTabComp.setLayoutData(fdPluginsTabComp);

    wPluginsTab.setControl(wPluginsTabComp);
  }

  /**
   * Show settings for a specific plugin. This is called from the tree selection in
   * ConfigurationPerspective.
   */
  public static void showConfigPluginSettings(String pluginName, Composite targetComposite) {
    Object pluginSourceData = pluginDataMap.get(pluginName);
    if (pluginSourceData == null) {
      return;
    }

    // Delete everything in the target composite
    for (Control child : targetComposite.getChildren()) {
      child.dispose();
    }

    // Rebuild
    ScrolledComposite sPluginsComp =
        new ScrolledComposite(targetComposite, SWT.V_SCROLL | SWT.H_SCROLL);
    sPluginsComp.setLayout(new FormLayout());
    FormData fdsPluginsComp = new FormData();
    fdsPluginsComp.left = new FormAttachment(0, 0);
    fdsPluginsComp.right = new FormAttachment(100, 0);
    fdsPluginsComp.top = new FormAttachment(0, 0);
    fdsPluginsComp.bottom = new FormAttachment(100, 0);
    sPluginsComp.setLayoutData(fdsPluginsComp);

    Composite wPluginsComp = new Composite(sPluginsComp, SWT.NONE);
    PropsUi.setLook(wPluginsComp);
    wPluginsComp.setLayout(new FormLayout());

    targetComposite.layout();

    GuiCompositeWidgets compositeWidgets =
        new GuiCompositeWidgets(HopGui.getInstance().getVariables());
    compositeWidgets.createCompositeWidgets(
        pluginSourceData, null, wPluginsComp, GUI_WIDGETS_PARENT_ID, null);
    compositeWidgets.setWidgetsContents(pluginSourceData, wPluginsComp, GUI_WIDGETS_PARENT_ID);
    if (pluginSourceData
        instanceof IGuiPluginCompositeWidgetsListener iGuiPluginCompositeWidgetsListener) {
      // This listener saves the changed values immediately, so we don't have to worry about that.
      //
      compositeWidgets.setWidgetsListener(iGuiPluginCompositeWidgetsListener);
    }

    wPluginsComp.layout();
    wPluginsComp.pack();

    Rectangle bounds = wPluginsComp.getBounds();
    sPluginsComp.setContent(wPluginsComp);
    sPluginsComp.setExpandHorizontal(true);
    sPluginsComp.setExpandVertical(true);
    sPluginsComp.setMinWidth(bounds.width);
    sPluginsComp.setMinHeight(bounds.height);

    targetComposite.layout(true, true);
  }

  /** Get all available plugin names for tree population */
  public static java.util.Set<String> getPluginNames() {
    return pluginDataMap.keySet();
  }

  /** Show instruction message when no plugin is selected */
  public static void showPluginInstructions(Composite targetComposite) {
    // Clear the composite
    for (Control child : targetComposite.getChildren()) {
      child.dispose();
    }

    // Create a centered label with instructions
    Composite instructionsComp = new Composite(targetComposite, SWT.NONE);
    PropsUi.setLook(instructionsComp);
    instructionsComp.setLayout(new FormLayout());

    FormData fdInstructions = new FormData();
    fdInstructions.left = new FormAttachment(0, 0);
    fdInstructions.right = new FormAttachment(100, 0);
    fdInstructions.top = new FormAttachment(0, 0);
    fdInstructions.bottom = new FormAttachment(100, 0);
    instructionsComp.setLayoutData(fdInstructions);

    org.eclipse.swt.widgets.Label instructionLabel =
        new org.eclipse.swt.widgets.Label(instructionsComp, SWT.CENTER | SWT.WRAP);
    instructionLabel.setText(
        "Select a plugin from the tree on the left to configure its settings.");
    PropsUi.setLook(instructionLabel);
    instructionLabel.setForeground(
        instructionsComp.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(20, 0);
    fdLabel.right = new FormAttachment(80, 0);
    fdLabel.top = new FormAttachment(40, 0);
    instructionLabel.setLayoutData(fdLabel);

    targetComposite.layout(true, true);
  }

  /** Get the plugin composite for a tab item */
  public Composite getPluginComposite() {
    return wPluginComposite;
  }
}
