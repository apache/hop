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

package org.apache.hop.airflow.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.airflow.client.AirflowClient;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginModelWidgetsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.MessageBox;

/** Where an Apache Airflow 3 REST API (v2) is and how to sign in to it. */
@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "airflow-connection",
    name = "i18n::AirflowConnection.Name",
    description = "i18n::AirflowConnection.Description",
    image = "airflow-connection.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/airflow-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.AIRFLOW_CONNECTION,
    supportsGlobalReplace = true)
public class AirflowConnection extends HopMetadataBase
    implements Serializable, IHopMetadata, IGuiPluginModelWidgetsListener {

  private static final long serialVersionUID = 1L;

  private static final Class<?> PKG = AirflowConnection.class;

  private static final String PARENT = AirflowConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID;
  private static final String GROUP_SERVER = "i18n::AirflowConnection.Group.Server";
  private static final String GROUP_AUTHENTICATION = "i18n::AirflowConnection.Group.Authentication";

  public static final String WIDGET_AUTHENTICATION_TYPE = "airflow-authentication-type";
  public static final String WIDGET_USERNAME = "airflow-username";
  public static final String WIDGET_PASSWORD = "airflow-password";
  public static final String WIDGET_TOKEN = "airflow-token";

  @GuiWidgetElement(
      id = "airflow-description",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowConnection.Description.Label",
      toolTip = "i18n::AirflowConnection.Description.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String description;

  @GuiWidgetElement(
      id = "airflow-url",
      order = "0200",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowConnection.Url.Label",
      toolTip = "i18n::AirflowConnection.Url.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String url;

  @GuiWidgetElement(
      id = "airflow-connect-timeout",
      order = "0300",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowConnection.ConnectTimeout.Label",
      toolTip = "i18n::AirflowConnection.ConnectTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty(key = "connect_timeout_seconds")
  private String connectTimeoutSeconds;

  @GuiWidgetElement(
      id = "airflow-request-timeout",
      order = "0400",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowConnection.RequestTimeout.Label",
      toolTip = "i18n::AirflowConnection.RequestTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty(key = "request_timeout_seconds")
  private String requestTimeoutSeconds;

  @GuiWidgetElement(
      id = WIDGET_AUTHENTICATION_TYPE,
      order = "1000",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::AirflowConnection.AuthenticationType.Label",
      toolTip = "i18n::AirflowConnection.AuthenticationType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(key = "authentication_type", storeWithCode = true)
  private AirflowAuthenticationType authenticationType;

  @GuiWidgetElement(
      id = WIDGET_USERNAME,
      order = "1100",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      label = "i18n::AirflowConnection.Username.Label",
      toolTip = "i18n::AirflowConnection.Username.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "1200",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AirflowConnection.Password.Label",
      toolTip = "i18n::AirflowConnection.Password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = WIDGET_TOKEN,
      order = "1300",
      parentId = PARENT,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AirflowConnection.Token.Label",
      toolTip = "i18n::AirflowConnection.Token.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(password = true)
  private String token;

  public AirflowConnection() {
    this.url = "http://localhost:8080";
    this.connectTimeoutSeconds = "10";
    this.requestTimeoutSeconds = "60";
    this.authenticationType = AirflowAuthenticationType.USERNAME_PASSWORD;
  }

  @GuiWidgetElement(
      id = "airflow-test",
      order = "0500",
      parentId = PARENT,
      type = GuiElementType.BUTTON,
      label = "i18n::AirflowConnection.Test.Label",
      toolTip = "i18n::AirflowConnection.Test.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  public void testConnection(Object object) {
    AirflowConnection connection = (AirflowConnection) object;
    HopGui hopGui = HopGui.getInstance();
    String title = BaseMessages.getString(PKG, "AirflowConnection.Test.Title");
    try {
      String version = AirflowClient.create(connection, hopGui.getVariables()).testConnection();
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(title);
      box.setMessage(BaseMessages.getString(PKG, "AirflowConnection.Test.Success", version));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), title, BaseMessages.getString(PKG, "AirflowConnection.Test.Error"), e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Values are not on the widgets yet.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    hideUnusedCredentials(compositeWidgets);
  }

  @Override
  public void widgetModified(GuiCompositeWidgets compositeWidgets, String widgetId) {
    if (WIDGET_AUTHENTICATION_TYPE.equals(widgetId)) {
      hideUnusedCredentials(compositeWidgets);
    }
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // The editor reads the widgets back itself.
  }

  private void hideUnusedCredentials(GuiCompositeWidgets compositeWidgets) {
    if (compositeWidgets.getWidgetsMap() == null) {
      return;
    }
    AirflowAuthenticationType type = selectedAuthenticationType(compositeWidgets);
    Set<String> hidden = new HashSet<>();
    if (type != AirflowAuthenticationType.USERNAME_PASSWORD) {
      hidden.add(WIDGET_USERNAME);
      hidden.add(WIDGET_PASSWORD);
    }
    if (type != AirflowAuthenticationType.TOKEN) {
      hidden.add(WIDGET_TOKEN);
    }
    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  private AirflowAuthenticationType selectedAuthenticationType(
      GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_AUTHENTICATION_TYPE);
    if (control instanceof Combo combo) {
      String text = combo.getText();
      for (AirflowAuthenticationType type : AirflowAuthenticationType.values()) {
        if (type.getDescription().equals(text) || type.name().equals(text)) {
          return type;
        }
      }
    }
    return authenticationType == null
        ? AirflowAuthenticationType.USERNAME_PASSWORD
        : authenticationType;
  }
}
