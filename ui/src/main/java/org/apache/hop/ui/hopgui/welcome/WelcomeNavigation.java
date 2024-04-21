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

package org.apache.hop.ui.hopgui.welcome;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

@GuiPlugin
public class WelcomeNavigation {

  private static final Class<?> PKG = WelcomeNavigation.class; // i18n

  private static final String WELCOME_NAVIGATION_PARENT_ID = "WelcomeWelcome.Parent.ID";

  @GuiWidgetElement(
      type = GuiElementType.COMPOSITE,
      id = "11000-navigation",
      label = "Navigation",
      parentId = WelcomeDialog.PARENT_ID_WELCOME_WIDGETS)
  public void welcome(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    Composite parentComposite = new Composite(parent, SWT.NONE);
    parentComposite.setLayout(props.createFormLayout());

    FormData fdParentComposite = new FormData();
    fdParentComposite.left = new FormAttachment(0, 0);
    fdParentComposite.right = new FormAttachment(100, 0);
    fdParentComposite.top = new FormAttachment(0, 0);
    fdParentComposite.bottom = new FormAttachment(100, 0);
    parentComposite.setLayoutData(fdParentComposite);
    PropsUi.setLook(parentComposite);

    GuiCompositeWidgets compositeWidgets =
        new GuiCompositeWidgets(HopGui.getInstance().getVariables());
    compositeWidgets.createCompositeWidgets(
        this, null, parentComposite, WELCOME_NAVIGATION_PARENT_ID, null);
    // No data to set on these widgets
  }

  @GuiWidgetElement(
      id = "WelcomeNavigation.1000-how-to-navigate",
      parentId = WELCOME_NAVIGATION_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "Here are a few ways to navigate around large or zoomed pipelines and workflows:\n\n"
              + " - Use the arrow keys to move the view\n"
              + " - Use the HOME key to reset to top/left\n"
              + " - Drag with the middle mouse button on the background\n"
              + " - CTRL+drag with the left mouse button on the background\n"
              + " - Drag the darker rectangle on the bottom right of the screen\n")
  public void homepageLink(Event event) {
    // handleWebLinkEvent(event, WEB_NAME_HOP_APACHE_ORG, WEB_LINK_HOP_APACHE_ORG);
  }
}
