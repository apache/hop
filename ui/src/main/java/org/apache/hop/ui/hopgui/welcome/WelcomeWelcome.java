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
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

@GuiPlugin
public class WelcomeWelcome {

  private static final Class<?> PKG = WelcomeWelcome.class; // i18n

  private static final String WELCOME_WELCOME_PARENT_ID = "WelcomeWelcome.Parent.ID";

  @GuiWidgetElement(
      type = GuiElementType.COMPOSITE,
      id = "10000-welcome",
      label = "Welcome!",
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
        this, null, parentComposite, WELCOME_WELCOME_PARENT_ID, null);
    // No data to set on these widgets
  }

  public static final String WEB_NAME_HOP_APACHE_ORG = "hop.apache.org";
  public static final String WEB_LINK_HOP_APACHE_ORG = "https://hop.apache.org/";
  public static final String WEB_NAME_GITHUB_STAR = "starring";
  public static final String WEB_LINK_GITHUB_STAR = "https://github.com/apache/hop";

  @GuiWidgetElement(
      id = "WelcomeWelcome.0900-github-star",
      parentId = WELCOME_WELCOME_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "If you like Apache Hop, please consider <a>"
              + WEB_NAME_GITHUB_STAR
              + "</a> \u2B50 the project on github. \n")
  public void githubStarLink(Event event) {
    handleWebLinkEvent(event, WEB_NAME_GITHUB_STAR, WEB_LINK_GITHUB_STAR);
  }

  @GuiWidgetElement(
      id = "WelcomeWelcome.1000-homepage",
      parentId = WELCOME_WELCOME_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "Welcome to the Apache Hop project!\n\n"
              + "The Hop Orchestration Platform, or Apache Hop, aims to facilitate all aspects of data and metadata orchestration.\n\n"
              + "The Apache Hop website can be found at <a>"
              + WEB_NAME_HOP_APACHE_ORG
              + "</a>.\n\nBelow are a few documentation links to get started:\n")
  public void homepageLink(Event event) {
    handleWebLinkEvent(event, WEB_NAME_HOP_APACHE_ORG, WEB_LINK_HOP_APACHE_ORG);
  }

  public static final String WEB_NAME_GETTING_STARTED = "The getting started guide";
  public static final String WEB_LINK_GETTING_STARTED =
      "https://hop.apache.org/manual/latest/getting-started/";

  @GuiWidgetElement(
      id = "WelcomeWelcome.1010-getting-started",
      parentId = WELCOME_WELCOME_PARENT_ID,
      type = GuiElementType.LINK,
      label = "  * <a>" + WEB_NAME_GETTING_STARTED + "</a>")
  public void gettingStarted(Event event) {
    handleWebLinkEvent(event, WEB_NAME_GETTING_STARTED, WEB_LINK_GETTING_STARTED);
  }

  public static final String WEB_NAME_USER_MANUAL = "The Hop user manual";
  public static final String WEB_LINK_USER_MANUAL = "https://hop.apache.org/manual/latest/";

  @GuiWidgetElement(
      id = "WelcomeWelcome.1020-user-manual",
      parentId = WELCOME_WELCOME_PARENT_ID,
      type = GuiElementType.LINK,
      label = "  * <a>" + WEB_NAME_USER_MANUAL + "</a>\n\n\n\n")
  public void userManual(Event event) {
    handleWebLinkEvent(event, WEB_NAME_USER_MANUAL, WEB_LINK_USER_MANUAL);
  }

  private void handleWebLinkEvent(Event event, String text, String url) {
    try {
      if (text.equals(event.text)) {
        EnvironmentUtils.getInstance().openUrl(url);
      }
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error opening link to " + url, e);
    }
  }
}
