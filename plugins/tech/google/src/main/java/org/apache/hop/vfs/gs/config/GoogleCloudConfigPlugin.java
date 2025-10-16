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

package org.apache.hop.vfs.gs.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@Setter
@Getter
@ConfigPlugin(
    id = "GoogleCloudStorageConfigPlugin",
    description = "Configuration options for Google Cloud",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "i18n::GoogleCloudPlugin.GuiPlugin.Description" // Tab label in options dialog
    )
public class GoogleCloudConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE =
      "10000-google-cloud-service-account-key-file";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_SCAN_FOLDERS_FOR_MODIF_DATE =
      "10010-google-cloud-service-scan-folders-for-modification-date";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_ATTEMPTS =
      "10100-google-cloud-service-max-attempts";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RETRY_DELAY =
      "10200-google-cloud-service-initial-retry-delay";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_RETRY_DELAY_MULTIPLIER =
      "10300-google-cloud-service-retry-delay-multiplier";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RETRY_DELAY =
      "10400-google-cloud-service-max-retry-delay";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_TOTAL_TIMEOUT =
      "10500-google-cloud-service-total-timeout";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RPC_TIMEOUT =
      "10600-google-cloud-service-inital-rpc-timeout";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_RPC_TIMEOUT_MULTIPLIER =
      "10700-google-cloud-service-rpc-timeout-multiplier";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RPC_TIMEOUT =
      "10800-google-cloud-service-max-rpc-timeout";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_CONNECT_TIMEOUT =
      "10900-google-cloud-service-connect-timeout";
  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_READ_TIMEOUT =
      "1100-google-cloud-service-read-timeout";

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      variables = true,
      label = "i18n::GoogleCloudPlugin.AccountKeyFile.Label",
      toolTip = "i18n::GoogleCloudPlugin.AccountKeyFile.Description")
  @CommandLine.Option(
      names = {"-gck", "--google-cloud-service-account-key-file"},
      description = "Configure the path to a Google Cloud service account JSON key file")
  private String serviceAccountKeyFile;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_SCAN_FOLDERS_FOR_MODIF_DATE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::GoogleCloudPlugin.ScanFolderForLastModificationDate.Label",
      toolTip = "i18n::GoogleCloudPlugin.ScanFolderForLastModificationDate.Description")
  private Boolean scanFoldersForModificationDate;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_ATTEMPTS,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.MaxAttempts.Label",
      toolTip = "i18n::GoogleCloudPlugin.MaxAttempts.Description")
  private String maxAttempts;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RETRY_DELAY,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.InitialRetryDelay.Label",
      toolTip = "i18n::GoogleCloudPlugin.InitialRetryDelay.Description")
  private String initialRetryDelay;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_RETRY_DELAY_MULTIPLIER,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.RetryDelayMultiplier.Label",
      toolTip = "i18n::GoogleCloudPlugin.RetryDelayMultiplier.Description")
  private String retryDelayMultiplier;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RETRY_DELAY,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.MaxRetryDelay.Label",
      toolTip = "i18n::GoogleCloudPlugin.MaxRetryDelay.Description")
  private String maxRetryDelay;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_TOTAL_TIMEOUT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.TotalTimeout.Label",
      toolTip = "i18n::GoogleCloudPlugin.TotalTimeout.Description")
  private String totalTimeout;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RPC_TIMEOUT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.InitialRpcTimeout.Label",
      toolTip = "i18n::GoogleCloudPlugin.InitialRpcTimeout.Description")
  private String initialRpcTimeout;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_RPC_TIMEOUT_MULTIPLIER,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.RpcTimeoutMultiplier.Label",
      toolTip = "i18n::GoogleCloudPlugin.RpcTimeoutMultiplier.Description")
  private String rpcTimeoutMultiplier;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RPC_TIMEOUT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.MaxRpcTimeout.Label",
      toolTip = "i18n::GoogleCloudPlugin.MaxRpcTimeout.Description")
  private String maxRpcTimeout;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_CONNECT_TIMEOUT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.ConnectTimeout.Label",
      toolTip = "i18n::GoogleCloudPlugin.ConnectTimeout.Description")
  private String connectTimeout;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_READ_TIMEOUT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "i18n::GoogleCloudPlugin.ReadTimeout.Label",
      toolTip = "i18n::GoogleCloudPlugin.ReadTimeout.Description")
  private String readTimeout;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static GoogleCloudConfigPlugin getInstance() {
    GoogleCloudConfigPlugin instance = new GoogleCloudConfigPlugin();

    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
    instance.serviceAccountKeyFile = config.getServiceAccountKeyFile();
    instance.scanFoldersForModificationDate = config.getScanFoldersForLastModifDate();
    instance.maxAttempts = config.getMaxAttempts();
    instance.initialRetryDelay = config.getInitialRetryDelay();
    instance.retryDelayMultiplier = config.getRetryDelayMultiplier();
    instance.maxRetryDelay = config.getMaxRetryDelay();
    instance.totalTimeout = config.getTotalTimeout();
    instance.initialRpcTimeout = config.getInitialRpcTimeout();
    instance.rpcTimeoutMultiplier = config.getRpcTimeoutMultiplier();
    instance.maxRpcTimeout = config.getMaxRpcTimeout();
    instance.connectTimeout = config.getConnectionTimeout();
    instance.readTimeout = config.getReadTimeout();

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (serviceAccountKeyFile != null) {
        config.setServiceAccountKeyFile(serviceAccountKeyFile);
        log.logBasic(
            "The Google Cloud service account JSON jey file is set to '"
                + serviceAccountKeyFile
                + "'");

        changed = true;
      }

      if (scanFoldersForModificationDate != null
          && scanFoldersForModificationDate.equals(Boolean.TRUE)) {
        config.setScanFoldersForLastModifDate(scanFoldersForModificationDate);
        log.logBasic(
            "Google Cloud Storage service will scan folders for the last file modification time.");
        changed = true;
      }

      if (maxAttempts != null) {
        config.setMaxAttempts(maxAttempts);
        log.logBasic("Google Cloud service max attempts set to " + maxAttempts);
        changed = true;
      }

      if (initialRetryDelay != null) {
        config.setInitialRetryDelay(initialRetryDelay);
        log.logBasic("Google Cloud service initialRetryDelay set to " + initialRetryDelay);
        changed = true;
      }

      if (retryDelayMultiplier != null) {
        config.setRetryDelayMultiplier(retryDelayMultiplier);
        log.logBasic("Google Cloud service retryDelayMultiplier set to " + retryDelayMultiplier);
        changed = true;
      }

      if (maxRetryDelay != null) {
        config.setMaxRetryDelay(maxRetryDelay);
        log.logBasic("Google Cloud service maxRetryDelay set to " + maxRetryDelay);
        changed = true;
      }

      if (totalTimeout != null) {
        config.setTotalTimeout(totalTimeout);
        log.logBasic("Google Cloud service totalTimeout set to " + totalTimeout);
        changed = true;
      }

      if (initialRpcTimeout != null) {
        config.setInitialRpcTimeout(initialRpcTimeout);
        log.logBasic("Google Cloud service initialRpcTimeout set to " + initialRpcTimeout);
        changed = true;
      }

      if (rpcTimeoutMultiplier != null) {
        config.setRpcTimeoutMultiplier(rpcTimeoutMultiplier);
        log.logBasic("Google Cloud service rpcTimeoutMultiplier set to " + rpcTimeoutMultiplier);
        changed = true;
      }

      if (maxRpcTimeout != null) {
        config.setMaxRpcTimeout(maxRpcTimeout);
        log.logBasic("Google Cloud service maxRpcTimeout set to " + maxRpcTimeout);
        changed = true;
      }

      if (connectTimeout != null) {
        config.setConnectionTimeout(connectTimeout);
        log.logBasic("Google Cloud service connectTimeout set to " + connectTimeout);
        changed = true;
      }

      if (readTimeout != null) {
        config.setReadTimeout(readTimeout);
        log.logBasic("Google Cloud service readTimeout set to " + readTimeout);
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        GoogleCloudConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Google Cloud configuration options", e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE:
          serviceAccountKeyFile = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setServiceAccountKeyFile(serviceAccountKeyFile);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_SCAN_FOLDERS_FOR_MODIF_DATE:
          scanFoldersForModificationDate = ((Button) control).getSelection();
          GoogleCloudConfigSingleton.getConfig()
              .setScanFoldersForLastModifDate(scanFoldersForModificationDate);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_ATTEMPTS:
          maxAttempts = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setMaxAttempts(maxAttempts);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RETRY_DELAY:
          initialRetryDelay = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setInitialRetryDelay(initialRetryDelay);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_RETRY_DELAY_MULTIPLIER:
          retryDelayMultiplier = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setRetryDelayMultiplier(retryDelayMultiplier);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RETRY_DELAY:
          maxRetryDelay = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setMaxRetryDelay(maxRetryDelay);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_TOTAL_TIMEOUT:
          totalTimeout = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setTotalTimeout(totalTimeout);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_INITIAL_RPC_TIMEOUT:
          initialRpcTimeout = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setInitialRpcTimeout(initialRpcTimeout);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_RPC_TIMEOUT_MULTIPLIER:
          rpcTimeoutMultiplier = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setRpcTimeoutMultiplier(rpcTimeoutMultiplier);
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_MAX_RPC_TIMEOUT:
          maxRpcTimeout = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setMaxRpcTimeout(maxRpcTimeout);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_CONNECT_TIMEOUT:
          connectTimeout = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setConnectionTimeout(connectTimeout);
          break;
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_READ_TIMEOUT:
          readTimeout = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setReadTimeout(readTimeout);
          break;
      }
    }
    // Save the project...
    //
    try {
      GoogleCloudConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }
}
