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

package org.apache.hop.ui.util;

import static org.apache.hop.core.Const.getDocUrl;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ShowHelpDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.config.ExplorerPerspectiveConfigSingleton;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

public class HelpUtils {
  private static final Class<?> PKG = HelpUtils.class;

  public static Button createHelpButton(final Composite parent, final IPlugin plugin) {
    Button button = newButton(parent);
    button.addListener(SWT.Selection, e -> openHelp(parent.getShell(), plugin));
    return button;
  }

  public static Button createHelpButton(final Composite parent, final String url) {
    Button button = newButton(parent);
    button.addListener(SWT.Selection, e -> openHelp(parent.getShell(), url));
    return button;
  }

  private static Button newButton(final Composite parent) {
    Button button = new Button(parent, SWT.PUSH);
    PropsUi.setLook(button);
    button.setImage(GuiResource.getInstance().getImageHelp());
    button.setText(BaseMessages.getString(PKG, "System.Button.Help"));
    button.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.Help"));
    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment(0, 0);
    fdButton.bottom = new FormAttachment(100, 0);
    button.setLayoutData(fdButton);
    // Always available in read-only dialogs
    BaseDialog.keepEnabledInReadOnly(button);
    return button;
  }

  public static boolean isPluginDocumented(IPlugin plugin) {
    if (plugin == null) {
      return false;
    }
    return !StringUtil.isEmpty(plugin.getDocumentationUrl());
  }

  public static void openHelp(Shell shell, IPlugin plugin) {
    if (shell == null || plugin == null) {
      return;
    }
    if (isPluginDocumented(plugin)) {
      openHelp(shell, getDocUrl(plugin.getDocumentationUrl()));
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      String msg = "";
      // only supports Transform, Action, Database and Metadata - extend if required.
      if (plugin.getPluginType().equals(TransformPluginType.class)) {
        msg = BaseMessages.getString(PKG, "System.Help.Transform.IsNotAvailable", plugin.getName());
      } else if (plugin.getPluginType().equals(ActionPluginType.class)) {
        msg = BaseMessages.getString(PKG, "System.Help.Action.IsNotAvailable", plugin.getName());
      } else if (plugin.getPluginType().equals(DatabasePluginType.class)) {
        msg = BaseMessages.getString(PKG, "System.Help.Database.IsNotAvailable", plugin.getName());
      } else {
        msg = BaseMessages.getString(PKG, "System.Help.Metadata.IsNotAvailable", plugin.getName());
      }

      mb.setMessage(msg);
      mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
      mb.open();
    }
  }

  /**
   * Open a documentation URL using the configured {@link HelpOpenMode}.
   *
   * @param shell context shell (transform/action dialog or main window)
   * @param url documentation URL
   */
  public static void openHelp(Shell shell, String url) {
    if (Utils.isEmpty(url)) {
      return;
    }
    try {
      openTrackedUrl(shell, appendUtmParameters(url));
    } catch (Exception ex) {
      Shell errorShell = shell != null ? shell : HopGui.getInstance().getShell();
      new ErrorDialog(errorShell, "Error", "Error opening URL", ex);
    }
  }

  static void openTrackedUrl(Shell shell, String trackedUrl) throws HopException {
    HelpOpenMode mode = currentOpenMode();
    switch (mode) {
      case TAB:
        openHelpInTab(trackedUrl);
        break;
      case DIALOG:
        openHelpInDialog(shell, trackedUrl);
        break;
      case BROWSER:
      default:
        EnvironmentUtils.getInstance().openUrl(trackedUrl);
        break;
    }
  }

  static HelpOpenMode currentOpenMode() {
    try {
      return ExplorerPerspectiveConfigSingleton.getConfig().getHelpOpenMode();
    } catch (Exception e) {
      return HelpOpenMode.BROWSER;
    }
  }

  /**
   * Add analytics tracking parameters for help-button <code>
   * mtm_campaign=hopgui&mtm_source=help_btn&mtm_kwd=write to log
   * </code>
   */
  static String appendUtmParameters(String url) {
    if (url == null || url.isEmpty()) {
      return url;
    }

    // campaign: hop gui, source: hop version
    String utmCampaign;
    utmCampaign = EnvironmentUtils.getInstance().isWeb() ? "HopWeb" : "hopGui";
    String utmSource = HopEnvironment.class.getPackage().getImplementationVersion();
    String utmParams = "mtm_campaign=" + encode(utmCampaign) + "&mtm_source=" + encode(utmSource);

    String separator = url.contains("?") ? "&" : "?";
    return url + separator + utmParams;
  }

  /** url params encode. */
  private static String encode(String field) {
    if (Utils.isEmpty(field)) {
      return field;
    }

    return URLEncoder.encode(field, StandardCharsets.UTF_8);
  }

  private static void openHelpInTab(String url) throws HopException {
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null) {
      IHopFileType htmlFileType = HopFileTypeRegistry.getInstance().findHopFileType("help.html");
      if (htmlFileType != null) {
        htmlFileType.openFile(hopGui, url, hopGui.getVariables());
        return;
      }
    }
    // Fallback
    EnvironmentUtils.getInstance().openUrl(url);
  }

  private static void openHelpInDialog(Shell shell, String url) throws HopException {
    Shell parent = resolveParentShell(shell);
    if (parent == null) {
      EnvironmentUtils.getInstance().openUrl(url);
      return;
    }

    Object existing = parent.getData(ShowHelpDialog.SHELL_DATA_KEY);
    if (existing instanceof ShowHelpDialog dialog && !dialog.isDisposed()) {
      dialog.setUrl(url);
      dialog.forceActive();
      return;
    }

    try {
      ShowHelpDialog dialog = new ShowHelpDialog(parent, url);
      dialog.open();
      parent.setData(ShowHelpDialog.SHELL_DATA_KEY, dialog);
      parent.addDisposeListener(e -> parent.setData(ShowHelpDialog.SHELL_DATA_KEY, null));
    } catch (Exception ex) {
      parent.setData(ShowHelpDialog.SHELL_DATA_KEY, null);
      new ErrorDialog(parent, "Error", "Error opening help dialog", ex);
      EnvironmentUtils.getInstance().openUrl(url);
    }
  }

  private static Shell resolveParentShell(Shell shell) {
    if (shell != null && !shell.isDisposed()) {
      return shell;
    }
    HopGui hopGui = HopGui.getInstance();
    if (hopGui != null && hopGui.getShell() != null && !hopGui.getShell().isDisposed()) {
      return hopGui.getShell();
    }
    return null;
  }
}
