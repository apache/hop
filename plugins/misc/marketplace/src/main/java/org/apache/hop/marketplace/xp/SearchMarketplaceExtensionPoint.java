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

package org.apache.hop.marketplace.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.gui.MarketplaceDialog;
import org.apache.hop.marketplace.gui.MarketplaceGuiPlugin;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Opens the marketplace with the search box pre-filled, so the GUI can offer a way out of a missing
 * plugin without depending on the marketplace plugin being installed.
 *
 * <p>The payload is the plugin id to search for; blank or null lists everything.
 */
@ExtensionPoint(
    id = "SearchMarketplaceExtensionPoint",
    description = "Open the marketplace, searching for a plugin id",
    extensionPointId = "HopGuiSearchMarketplace")
public class SearchMarketplaceExtensionPoint implements IExtensionPoint<Object> {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object) {
    HopGui hopGui = HopGui.getInstance();
    String search = object instanceof String s ? s : null;
    try {
      new MarketplaceDialog(hopGui.getShell(), search).open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Header"),
          BaseMessages.getString(PKG, "MarketplaceDialog.Error.Open"),
          e);
    }
  }
}
