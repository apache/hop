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

package org.apache.hop.ui.hopgui.perspective.configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.tab.GuiTabItem;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

@HopPerspectivePlugin(
    id = "160-HopConfigurationPerspective",
    name = "i18n::ConfigurationPerspective.Name",
    description = "i18n::ConfigurationPerspective.Description",
    image = "ui/images/gear.svg",
    documentationUrl = "/hop-gui/perspective-configuration.html")
@GuiPlugin(description = "i18n::HopConfigurationPerspective.GuiPlugin.Description")
public class ConfigurationPerspective implements IHopPerspective {

  public static final String CONFIG_PERSPECTIVE_TABS = "ConfigurationPerspective.Tabs.ID";
  private HopGui hopGui;
  private Composite composite;
  public CTabFolder configTabs;
  private static ConfigurationPerspective instance;

  public static ConfigurationPerspective getInstance() {
    return instance;
  }

  public ConfigurationPerspective() {
    instance = this;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>();
  }

  @Override
  public String getId() {
    return "configuration";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'c')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'c')
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    // Do nothing
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    composite = new Composite(parent, SWT.NONE);
    composite.setLayout(new FillLayout());
    PropsUi.setLook(composite);

    configTabs = new CTabFolder(composite, SWT.BORDER);
    PropsUi.setLook(configTabs, Props.WIDGET_STYLE_TAB);
    configTabs.setMaximized(true);

    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    List<GuiTabItem> tabsList = guiRegistry.getGuiTabsMap().get(CONFIG_PERSPECTIVE_TABS);

    if (tabsList != null) {
      tabsList.sort(Comparator.comparing(GuiTabItem::getId));
      for (GuiTabItem tabItem : tabsList) {
        try {
          Object object = tabItem.getMethod().getDeclaringClass().getConstructor().newInstance();
          tabItem.getMethod().invoke(object, configTabs);
        } catch (Exception e) {
          new ErrorDialog(
              hopGui.getShell(),
              "Error",
              "Hop was unable to invoke @GuiTab method "
                  + tabItem.getMethod().getName()
                  + " with the parent composite as argument",
              e);
        }
      }
      if (configTabs.getItemCount() > 0) {
        configTabs.setSelection(0);
      }
    }

    configTabs.layout();
  }

  public void showSystemVariablesTab() {
    for (CTabItem tabItem : configTabs.getItems()) {
      // Do nothing
    }
  }

  @Override
  public Control getControl() {
    return composite;
  }

  @Override
  public List<ISearchable> getSearchables() {
    return new ArrayList<>();
  }
}
