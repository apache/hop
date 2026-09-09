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
package org.apache.hop.vfs.hdfs.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.vfs.hdfs.HdfsConnectionTester;
import org.apache.hop.vfs.hdfs.HdfsTransport;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

@Getter
@Setter
@GuiPlugin
@HopMetadata(
    key = "HdfsConnectionDefinition",
    name = "i18n::HdfsMeta.Name",
    description = "i18n::HdfsMeta.Description",
    image = "hdfs.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/hdfs-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_HDFS_CONNECTION,
    classLoaderGroup = "vfs-hdfs")
public class HdfsMeta extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final Class<?> PKG = HdfsMeta.class;

  public static final String GROUP_CLUSTER = "Cluster";
  public static final String GROUP_KERBEROS = "Kerberos";
  public static final String GROUP_TLS = "TLS";

  @GuiWidgetElement(
      id = "10000-description",
      order = "0100",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Description.Label",
      toolTip = "i18n::HdfsVFS.Description.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String description;

  @GuiWidgetElement(
      id = "10100-transport",
      order = "0200",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.COMBO,
      label = "i18n::HdfsVFS.Transport.Label",
      toolTip = "i18n::HdfsVFS.Transport.Tooltip",
      variables = false,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private HdfsTransport transport = HdfsTransport.HttpFS;

  @GuiWidgetElement(
      id = "10200-hostname",
      order = "0300",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Hostname.Label",
      toolTip = "i18n::HdfsVFS.Hostname.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String endpointHostname;

  @GuiWidgetElement(
      id = "10300-port",
      order = "0400",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Port.Label",
      toolTip = "i18n::HdfsVFS.Port.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String endpointPort;

  @GuiWidgetElement(
      id = "10400-https",
      order = "0500",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::HdfsVFS.Https.Label",
      toolTip = "i18n::HdfsVFS.Https.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private boolean https;

  @GuiWidgetElement(
      id = "10500-base-path",
      order = "0600",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.BasePath.Label",
      toolTip = "i18n::HdfsVFS.BasePath.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String basePath;

  @GuiWidgetElement(
      id = "10600-ha-namenodes",
      order = "0700",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 3,
      label = "i18n::HdfsVFS.HaNamenodes.Label",
      toolTip = "i18n::HdfsVFS.HaNamenodes.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String haNamenodes;

  @GuiWidgetElement(
      id = "10700-default-root",
      order = "0800",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.DefaultRoot.Label",
      toolTip = "i18n::HdfsVFS.DefaultRoot.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String defaultRoot;

  @GuiWidgetElement(
      id = "10800-simple-user",
      order = "0900",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.SimpleUser.Label",
      toolTip = "i18n::HdfsVFS.SimpleUser.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  @HopMetadataProperty
  private String simpleUser = "hop";

  @GuiWidgetElement(
      id = "10900-test-cluster",
      order = "0910",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::HdfsVFS.TestCluster.Label",
      toolTip = "i18n::HdfsVFS.TestCluster.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CLUSTER,
      groupOrder = "010")
  public void testClusterButton(Object object) {
    runTest(
        (HdfsMeta) object,
        HdfsConnectionTester::testCluster,
        "HdfsVFS.TestCluster.Success.Title",
        "HdfsVFS.TestCluster.Error.Title",
        "HdfsVFS.TestCluster.Error.Message");
  }

  @GuiWidgetElement(
      id = "20000-kerberos-enabled",
      order = "1000",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::HdfsVFS.KerberosEnabled.Label",
      toolTip = "i18n::HdfsVFS.KerberosEnabled.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean kerberosEnabled;

  @GuiWidgetElement(
      id = "20100-principal",
      order = "1100",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Principal.Label",
      toolTip = "i18n::HdfsVFS.Principal.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String principal;

  @GuiWidgetElement(
      id = "20200-keytab",
      order = "1200",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::HdfsVFS.Keytab.Label",
      toolTip = "i18n::HdfsVFS.Keytab.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String keytabPath;

  @GuiWidgetElement(
      id = "20300-krb5",
      order = "1300",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::HdfsVFS.Krb5.Label",
      toolTip = "i18n::HdfsVFS.Krb5.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String krb5ConfPath;

  @GuiWidgetElement(
      id = "20400-realm",
      order = "1400",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Realm.Label",
      toolTip = "i18n::HdfsVFS.Realm.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String realm;

  @GuiWidgetElement(
      id = "20500-kdc",
      order = "1500",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Kdc.Label",
      toolTip = "i18n::HdfsVFS.Kdc.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String kdc;

  @GuiWidgetElement(
      id = "20600-renewal",
      order = "1600",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::HdfsVFS.Renewal.Label",
      toolTip = "i18n::HdfsVFS.Renewal.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private String renewalIntervalMinutes = "360";

  @GuiWidgetElement(
      id = "20700-ticket-cache",
      order = "1700",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::HdfsVFS.TicketCache.Label",
      toolTip = "i18n::HdfsVFS.TicketCache.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  @HopMetadataProperty
  private boolean useTicketCache;

  @GuiWidgetElement(
      id = "20800-test-kerberos",
      order = "1710",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::HdfsVFS.TestKerberos.Label",
      toolTip = "i18n::HdfsVFS.TestKerberos.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_KERBEROS,
      groupOrder = "020")
  public void testKerberosButton(Object object) {
    runTest(
        (HdfsMeta) object,
        HdfsConnectionTester::testKerberos,
        "HdfsVFS.TestKerberos.Success.Title",
        "HdfsVFS.TestKerberos.Error.Title",
        "HdfsVFS.TestKerberos.Error.Message");
  }

  @GuiWidgetElement(
      id = "30000-truststore",
      order = "1800",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      typeFilename = HdfsTrustFilename.class,
      label = "i18n::HdfsVFS.Truststore.Label",
      toolTip = "i18n::HdfsVFS.Truststore.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TLS,
      groupOrder = "030")
  @HopMetadataProperty
  private String truststorePath;

  @GuiWidgetElement(
      id = "30100-truststore-password",
      order = "1900",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::HdfsVFS.TruststorePassword.Label",
      toolTip = "i18n::HdfsVFS.TruststorePassword.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TLS,
      groupOrder = "030")
  @HopMetadataProperty(password = true)
  private String truststorePassword;

  @GuiWidgetElement(
      id = "30200-hostname-verification",
      order = "2000",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::HdfsVFS.HostnameVerification.Label",
      toolTip = "i18n::HdfsVFS.HostnameVerification.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TLS,
      groupOrder = "030")
  @HopMetadataProperty
  private boolean hostnameVerification = true;

  @GuiWidgetElement(
      id = "30300-test-tls",
      order = "2010",
      parentId = HdfsMetaEditor.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::HdfsVFS.TestTls.Label",
      toolTip = "i18n::HdfsVFS.TestTls.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_TLS,
      groupOrder = "030")
  public void testTlsButton(Object object) {
    runTest(
        (HdfsMeta) object,
        HdfsConnectionTester::testTls,
        "HdfsVFS.TestTls.Success.Title",
        "HdfsVFS.TestTls.Error.Title",
        "HdfsVFS.TestTls.Error.Message");
  }

  @FunctionalInterface
  private interface Probe {
    String run(IVariables variables, HdfsMeta meta) throws Exception;
  }

  private void runTest(
      HdfsMeta meta, Probe probe, String successTitle, String errorTitle, String errorMessage) {
    HopGui hopGui = HopGui.getInstance();
    try {
      String result = probe.run(hopGui.getVariables(), meta);
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, successTitle));
      box.setMessage(result);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, errorTitle),
          BaseMessages.getString(PKG, errorMessage),
          e);
    }
  }

  public HdfsMeta() {
    this.transport = HdfsTransport.HttpFS;
    this.simpleUser = "hop";
    this.renewalIntervalMinutes = "360";
    this.hostnameVerification = true;
  }
}
