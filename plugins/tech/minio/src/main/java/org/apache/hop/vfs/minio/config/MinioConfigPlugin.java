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

package org.apache.hop.vfs.minio.config;

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

@Getter
@Setter
@ConfigPlugin(id = "MinioConfigPlugin", description = "MinIO VFS")
@GuiPlugin(description = "MinIO VFS")
public class MinioConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_MINIO_ACCOUNT = "10000-minio-account";
  private static final String WIDGET_ID_MINIO_KEY = "10100-minio-key";
  private static final String WIDGET_ID_MINIO_ENDPOINT_HOSTNAME = "10200-minio-endpoint-hostname";
  private static final String WIDGET_ID_MINIO_ENDPOINT_PORT = "10210-minio-endpoint-port";
  private static final String WIDGET_ID_MINIO_ENDPOINT_SECURE = "10220-minio-endpoint-secure";
  private static final String WIDGET_ID_MINIO_REGION = "10300-minio-region";
  private static final String WIDGET_ID_MINIO_PART_SIZE = "10400-minio-part-size";

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ACCOUNT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.AccessKey.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.AccessKey.Description")
  @CommandLine.Option(
      names = {"-mia", "--minio-access-key"},
      description = "The access key to use for Minio VFS")
  private String accessKey;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_KEY,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.SecretKey.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.SecretKey.Description")
  @CommandLine.Option(
      names = {"-mis", "--minio-secret-key"},
      description = "The secret key to use for Minio VFS")
  private String secretKey;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_HOSTNAME,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.Host.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.Host.Description")
  @CommandLine.Option(
      names = {"-mih", "--minio-endpoint-host"},
      description = "The hostname of the Minio service endpoint")
  private String endPointHostname;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_PORT,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.Port.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.Port.Description")
  @CommandLine.Option(
      names = {"-mit", "--minio-endpoint-port"},
      description = "The port of the Minio service endpoint")
  private String endPointPort;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_ENDPOINT_SECURE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.Secure.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.Secure.Description")
  @CommandLine.Option(
      names = {"-mie", "--minio-endpoint-secure"},
      description = "Secure the Minio service endpoint?")
  private boolean endPointSecure;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_REGION,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.Region.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.Region.Description")
  @CommandLine.Option(
      names = {"-mir", "--minio-region"},
      description = "The region to use for the MinIO service")
  private String region;

  @GuiWidgetElement(
      id = WIDGET_ID_MINIO_PART_SIZE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n:org.apache.hop.vfs.minio:MinioVFS.PartSize.Label",
      toolTip = "i18n:org.apache.hop.vfs.minio:MinioVFS.PartSize.Description")
  @CommandLine.Option(
      names = {"-mip", "--minio-part-size"},
      description = "The part size to use for MinIO objects")
  private String partSize;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static MinioConfigPlugin getInstance() {
    MinioConfigPlugin instance = new MinioConfigPlugin();

    MinioConfig config = MinioConfigSingleton.getConfig();
    instance.accessKey = config.getAccessKey();
    instance.secretKey = config.getSecretKey();
    instance.endPointHostname = config.getEndPointHostname();
    instance.endPointPort = config.getEndPointPort();
    instance.endPointSecure = config.isEndPointSecure();
    instance.region = config.getRegion();
    instance.partSize = config.getPartSize();

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    MinioConfig config = MinioConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (accessKey != null) {
        config.setAccessKey(accessKey);
        log.logBasic("The MinIO account is set to '" + accessKey + "'");
        changed = true;
      }
      if (secretKey != null) {
        config.setSecretKey(secretKey);
        log.logBasic("The MinIO key is set to '" + secretKey + "'");
        changed = true;
      }
      if (endPointHostname != null) {
        config.setEndPointHostname(endPointHostname);
        log.logBasic("The MinIO endpoint hostname is set to '" + endPointHostname + "'");
      }
      if (endPointPort != null) {
        config.setEndPointPort(endPointPort);
        log.logBasic("The MinIO endpoint port is set to '" + endPointPort + "'");
      }
      config.setEndPointSecure(endPointSecure);
      log.logBasic("The MinIO endpoint secure flag is set to '" + endPointSecure + "'");

      if (region != null) {
        config.setEndPointHostname(region);
        log.logBasic("The MinIO region is set to '" + region + "'");
      }
      if (partSize != null) {
        config.setEndPointHostname(partSize);
        log.logBasic("The MinIO part size is set to '" + partSize + "'");
      }

      // Save to file if anything changed
      //
      if (changed) {
        MinioConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling MinIO configuration options", e);
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
        case WIDGET_ID_MINIO_ACCOUNT:
          accessKey = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setAccessKey(accessKey);
          break;
        case WIDGET_ID_MINIO_KEY:
          secretKey = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setSecretKey(secretKey);
          break;
        case WIDGET_ID_MINIO_ENDPOINT_HOSTNAME:
          endPointHostname = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setEndPointHostname(endPointHostname);
          break;
        case WIDGET_ID_MINIO_ENDPOINT_PORT:
          endPointPort = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setEndPointPort(endPointPort);
          break;
        case WIDGET_ID_MINIO_ENDPOINT_SECURE:
          endPointSecure = ((Button) control).getSelection();
          MinioConfigSingleton.getConfig().setEndPointSecure(endPointSecure);
          break;
        case WIDGET_ID_MINIO_REGION:
          region = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setRegion(region);
          break;
        case WIDGET_ID_MINIO_PART_SIZE:
          partSize = ((TextVar) control).getText();
          MinioConfigSingleton.getConfig().setPartSize(partSize);
          break;
      }
    }
    // Save the project...
    //
    try {
      MinioConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }
}
