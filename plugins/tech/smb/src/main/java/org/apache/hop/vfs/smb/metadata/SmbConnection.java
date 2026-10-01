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
package org.apache.hop.vfs.smb.metadata;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.vfs.IVfsBrowseLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.smb.SmbAuthType;
import org.apache.hop.vfs.smb.SmbConnectionTester;
import org.apache.hop.vfs.smb.SmbDialect;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.MessageBox;

@Getter
@Setter
@GuiPlugin(classLoaderGroup = "vfs-smb")
@HopMetadata(
    key = "smb-connection",
    name = "i18n::SmbConnection.Name",
    description = "i18n::SmbConnection.Description",
    image = "smb.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/smb-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_SMB_CONNECTION,
    classLoaderGroup = "vfs-smb")
public class SmbConnection extends HopMetadataBase
    implements Serializable, IHopMetadata, IVfsBrowseLocation, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = SmbConnection.class;

  public static final String GROUP_SERVER = "Server";
  public static final String GROUP_AUTHENTICATION = "Authentication";
  public static final String GROUP_SECURITY = "Security";

  public static final String WIDGET_AUTH = "smb-auth-type";
  public static final String WIDGET_DOMAIN = "smb-auth-domain";
  public static final String WIDGET_USERNAME = "smb-auth-username";
  public static final String WIDGET_PASSWORD = "smb-auth-password";

  @GuiWidgetElement(
      id = "smb-description",
      order = "0100",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Description.Label",
      toolTip = "i18n::Smb.Description.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String description;

  @GuiWidgetElement(
      id = "smb-hostname",
      order = "0200",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Hostname.Label",
      toolTip = "i18n::Smb.Hostname.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String hostname;

  @GuiWidgetElement(
      id = "smb-port",
      order = "0300",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Port.Label",
      toolTip = "i18n::Smb.Port.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String port = "445";

  @GuiWidgetElement(
      id = "smb-share",
      order = "0400",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Share.Label",
      toolTip = "i18n::Smb.Share.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String share;

  @GuiWidgetElement(
      id = "smb-base-path",
      order = "0500",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.BasePath.Label",
      toolTip = "i18n::Smb.BasePath.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  @HopMetadataProperty
  private String basePath;

  @GuiWidgetElement(
      id = "smb-test",
      order = "0600",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::Smb.Test.Label",
      toolTip = "i18n::Smb.Test.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SERVER,
      groupOrder = "010")
  public void testConnection(Object object) {
    SmbConnection meta = (SmbConnection) object;
    HopGui hopGui = HopGui.getInstance();
    String title = BaseMessages.getString(PKG, "Smb.Test.Title");
    try {
      String result = SmbConnectionTester.test(hopGui.getVariables(), meta);
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(title);
      box.setMessage(result);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), title, BaseMessages.getString(PKG, "Smb.Test.Error"), e);
    }
  }

  @GuiWidgetElement(
      id = WIDGET_AUTH,
      order = "1000",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::Smb.AuthType.Label",
      toolTip = "i18n::Smb.AuthType.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private SmbAuthType authType = SmbAuthType.NTLM;

  @GuiWidgetElement(
      id = WIDGET_DOMAIN,
      order = "1100",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Domain.Label",
      toolTip = "i18n::Smb.Domain.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String domain;

  @GuiWidgetElement(
      id = WIDGET_USERNAME,
      order = "1200",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.Username.Label",
      toolTip = "i18n::Smb.Username.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "1300",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::Smb.Password.Label",
      toolTip = "i18n::Smb.Password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = "smb-dialect",
      order = "2000",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::Smb.Dialect.Label",
      toolTip = "i18n::Smb.Dialect.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private SmbDialect minimumDialect = SmbDialect.SMB_2_0_2;

  @GuiWidgetElement(
      id = "smb-require-signing",
      order = "2100",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::Smb.RequireSigning.Label",
      toolTip = "i18n::Smb.RequireSigning.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean requireSigning;

  @GuiWidgetElement(
      id = "smb-encrypt",
      order = "2200",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::Smb.Encrypt.Label",
      toolTip = "i18n::Smb.Encrypt.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean encryptData;

  @GuiWidgetElement(
      id = "smb-dfs",
      order = "2300",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::Smb.Dfs.Label",
      toolTip = "i18n::Smb.Dfs.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean dfsEnabled;

  @GuiWidgetElement(
      id = "smb-call-timeout",
      order = "2400",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.CallTimeout.Label",
      toolTip = "i18n::Smb.CallTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private String callTimeoutSeconds = "60";

  @GuiWidgetElement(
      id = "smb-socket-timeout",
      order = "2500",
      parentId = SmbConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::Smb.SocketTimeout.Label",
      toolTip = "i18n::Smb.SocketTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_SECURITY,
      groupOrder = "030")
  @HopMetadataProperty
  private String socketTimeoutSeconds = "60";

  public SmbConnection() {
    this.port = "445";
    this.authType = SmbAuthType.NTLM;
    this.minimumDialect = SmbDialect.SMB_2_0_2;
    this.callTimeoutSeconds = "60";
    this.socketTimeoutSeconds = "60";
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Values are not on the widgets yet.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    hideCredentialsWhenGuest(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    hideCredentialsWhenGuest(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // The editor reads the widgets back itself.
  }

  private void hideCredentialsWhenGuest(GuiCompositeWidgets compositeWidgets) {
    if (compositeWidgets.getWidgetsMap() == null) {
      return;
    }
    Set<String> hidden = new HashSet<>();
    if (selectedAuth(compositeWidgets) == SmbAuthType.GUEST) {
      hidden.add(WIDGET_DOMAIN);
      hidden.add(WIDGET_USERNAME);
      hidden.add(WIDGET_PASSWORD);
    }
    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  private SmbAuthType selectedAuth(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_AUTH);
    String text = null;
    if (control instanceof Combo combo) {
      text = combo.getText();
    } else if (control instanceof ComboVar comboVar) {
      text = comboVar.getText();
    }
    if (text != null && !text.isBlank()) {
      try {
        return SmbAuthType.valueOf(text.trim());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet. The field is the better answer.
      }
    }
    return authType == null ? SmbAuthType.NTLM : authType;
  }
}
