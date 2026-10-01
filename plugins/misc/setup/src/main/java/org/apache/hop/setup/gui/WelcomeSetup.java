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

package org.apache.hop.setup.gui;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.welcome.WelcomeDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

@GuiPlugin
public class WelcomeSetup {

  private static final Class<?> PKG = SetupGuiPlugin.class;

  private static final String WELCOME_SETUP_PARENT_ID = "WelcomeSetup.Parent.ID";

  public static final String LINK_OPEN_SETUP = "Configure Hop environment";
  public static final String WEB_NAME_SETUP_DOCS = "Hop setup documentation";
  public static final String WEB_LINK_SETUP_DOCS =
      "https://hop.apache.org/manual/latest/hop-tools/hop-setup.html";

  @GuiWidgetElement(
      type = GuiElementType.COMPOSITE,
      id = "20500-setup-welcome",
      label = "Environment setup",
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
        this, null, parentComposite, WELCOME_SETUP_PARENT_ID, null);
  }

  @GuiWidgetElement(
      id = "WelcomeSetup.10010.overview",
      parentId = WELCOME_SETUP_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "By default Hop stores configuration next to the installation. Set HOP_CONFIG_FOLDER and"
              + " HOP_AUDIT_FOLDER so upgrades keep your projects and settings.\n\n"
              + "Open <a>Configure Hop environment</a> from Tools \u2192 Configure Hop"
              + " environment\u2026")
  public void overviewLink(Event event) {
    openSetup(event);
  }

  @GuiWidgetElement(
      id = "WelcomeSetup.10020.cli",
      parentId = WELCOME_SETUP_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "\nFrom the command line, in the Hop install directory:\n\n"
              + "  ./hop setup apply --defaults\n\n"
              + "Then restart Hop. See the <a>Hop setup documentation</a>.")
  public void cliLink(Event event) {
    handleWebLink(event);
  }

  private void openSetup(Event event) {
    try {
      if (LINK_OPEN_SETUP.equals(event.text)) {
        SetupGuiPlugin.getInstance().menuToolsSetup();
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          e);
    }
  }

  private void handleWebLink(Event event) {
    try {
      if (WEB_NAME_SETUP_DOCS.equals(event.text)) {
        EnvironmentUtils.getInstance().openUrl(WEB_LINK_SETUP_DOCS);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          e);
    }
  }
}
