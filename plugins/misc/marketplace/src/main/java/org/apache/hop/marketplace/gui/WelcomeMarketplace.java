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

package org.apache.hop.marketplace.gui;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
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

/**
 * Welcome dialog topic for the plugin marketplace: why plugins moved out of the fat client, docs,
 * open dialog, and how to restore the full plugin set.
 */
@GuiPlugin
public class WelcomeMarketplace {

  private static final String WELCOME_MARKETPLACE_PARENT_ID = "WelcomeMarketplace.Parent.ID";

  public static final String WEB_NAME_MARKETPLACE_DOCS = "Marketplace documentation";
  public static final String WEB_LINK_MARKETPLACE_DOCS =
      "https://hop.apache.org/manual/latest/hop-tools/hop-marketplace.html";

  public static final String LINK_OPEN_MARKETPLACE = "Open Marketplace";

  @GuiWidgetElement(
      type = GuiElementType.COMPOSITE,
      id = "21000-marketplace-welcome",
      label = "Marketplace",
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
        this, null, parentComposite, WELCOME_MARKETPLACE_PARENT_ID, null);
  }

  @GuiWidgetElement(
      id = "WelcomeMarketplace.10010.overview",
      parentId = WELCOME_MARKETPLACE_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "Large and lesser-used plugins are no longer bundled in the default hop-client download "
              + "(Apache packaging size limit).\n\n"
              + "Install them from the marketplace: plugins are published as Maven zips on the "
              + "Apache Repository (repository.apache.org), with Maven Central as fallback.\n\n"
              + "See the <a>"
              + WEB_NAME_MARKETPLACE_DOCS
              + "</a> for CLI and GUI details.")
  public void overviewLink(Event event) {
    handleWebLinkEvent(event);
  }

  @GuiWidgetElement(
      id = "WelcomeMarketplace.10020.open-dialog",
      parentId = WELCOME_MARKETPLACE_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "\nOpen the marketplace from Tools → Marketplace…, the main toolbar icon after Save As…, "
              + "or click <a>"
              + LINK_OPEN_MARKETPLACE
              + "</a>.")
  public void openMarketplaceLink(Event event) {
    openMarketplace(event);
  }

  @GuiWidgetElement(
      id = "WelcomeMarketplace.10030.install-all",
      parentId = WELCOME_MARKETPLACE_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "\nTo install all optional plugins that used to ship in the fat client:\n\n"
              + "  • GUI: open the marketplace, select full-client-env.yaml as the environment file "
              + "(pre-filled when present at the install root), click Apply, then restart Hop.\n"
              + "  • CLI (from the Hop install directory):\n\n"
              + "      ./hop marketplace apply -f full-client-env.yaml\n\n"
              + "Do not use prune unless you also want other marketplace installs removed.\n\n"
              + "Click <a>"
              + LINK_OPEN_MARKETPLACE
              + "</a> to open the dialog and Apply now.")
  public void installAllLink(Event event) {
    openMarketplace(event);
  }

  private void openMarketplace(Event event) {
    try {
      if (WelcomeMarketplace.LINK_OPEN_MARKETPLACE.equals(event.text)) {
        MarketplaceGuiPlugin.getInstance().menuToolsMarketplace();
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Unable to open the marketplace dialog", e);
    }
  }

  private void handleWebLinkEvent(Event event) {
    try {
      if (WelcomeMarketplace.WEB_NAME_MARKETPLACE_DOCS.equals(event.text)) {
        EnvironmentUtils.getInstance().openUrl(WelcomeMarketplace.WEB_LINK_MARKETPLACE_DOCS);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          "Error",
          "Error opening link to " + WelcomeMarketplace.WEB_LINK_MARKETPLACE_DOCS,
          e);
    }
  }
}
