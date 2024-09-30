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
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;

@GuiPlugin
public class ConfigPluginOptionsTab {

  public static final String GUI_WIDGETS_PARENT_ID = "EnterOptionsDialog-GuiWidgetsParent";

  private List wPluginsList;
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
    int margin = PropsUi.getMargin();

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

    // Plugins list on the left
    //
    wPluginsList = new List(wPluginsTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPluginsList);
    FormData fdPluginsList = new FormData();
    fdPluginsList.left = new FormAttachment(0, 0);
    fdPluginsList.right = new FormAttachment(20, 0);
    fdPluginsList.top = new FormAttachment(0, 0);
    fdPluginsList.bottom = new FormAttachment(100, 0);
    wPluginsList.setLayoutData(fdPluginsList);

    // A composite to show settings for a plugin on the right.
    //
    wPluginComposite = new Composite(wPluginsTabComp, SWT.NO_BACKGROUND | SWT.BORDER);
    PropsUi.setLook(wPluginComposite);
    FormData fdPluginComposite = new FormData();
    fdPluginComposite.left = new FormAttachment(wPluginsList, margin);
    fdPluginComposite.right = new FormAttachment(100, 0);
    fdPluginComposite.top = new FormAttachment(0, 0);
    fdPluginComposite.bottom = new FormAttachment(100, 0);
    wPluginComposite.setLayoutData(fdPluginComposite);
    wPluginComposite.setLayout(new FormLayout());

    // Add the list of configuration plugins
    //
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
          // Add a tab
          //
          String name =
              Const.NVL(
                  TranslateUtil.translate(annotation.description(), emptySourceData.getClass()),
                  "");
          wPluginsList.add(name);
          wPluginsList.setData(name, sourceData);
          wPluginsList.addListener(SWT.Selection, e -> showConfigPluginSettings());
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

    wPluginsList.select(new int[] {});

    FormData fdPluginsTabComp = new FormData();
    fdPluginsTabComp.left = new FormAttachment(0, 0);
    fdPluginsTabComp.right = new FormAttachment(100, 0);
    fdPluginsTabComp.top = new FormAttachment(0, 0);
    fdPluginsTabComp.bottom = new FormAttachment(100, 100);
    wPluginsTabComp.setLayoutData(fdPluginsTabComp);

    wPluginsTab.setControl(wPluginsTabComp);
  }

  /** Someone selected the settings of a plugin to edit */
  private void showConfigPluginSettings() {
    int index = wPluginsList.getSelectionIndex();
    if (index < 0) {
      return;
    }
    String name = wPluginsList.getSelection()[0];
    Object pluginSourceData = wPluginsList.getData(name);

    // Delete everything
    for (Control child : wPluginComposite.getChildren()) {
      child.dispose();
    }

    // Rebuild
    ScrolledComposite sPluginsComp =
        new ScrolledComposite(wPluginComposite, SWT.V_SCROLL | SWT.H_SCROLL);
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

    wPluginComposite.layout();

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

    wPluginComposite.layout(true, true);
  }
}
