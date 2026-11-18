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

package org.apache.hop.vfs.sharepoint.metadatatype;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.IVfsBrowseLocation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadata;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@Getter
@Setter
@NullMarked
@GuiPlugin(classLoaderGroup = "vfs-graph")
@HopMetadata(
    key = "sharepoint-connection",
    name = "i18n::SharePointConnection.Name",
    description = "i18n::SharePointConnection.Description",
    image = "sharepoint.svg",
    category = HopMetadataCategory.FILE_STORAGE,
    documentationUrl = "/metadata-types/sharepoint-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.VFS_SHAREPOINT_CONNECTION,
    classLoaderGroup = "vfs-graph")
public class SharePointConnection extends HopMetadataBase
    implements IHopMetadata, IVfsBrowseLocation {
  private static final Class<?> PKG = SharePointConnection.class;

  public static final String GROUP_CONNECTION = "i18n::SharePointConnectionEditor.Group.Connection";
  public static final String GROUP_AUTHENTICATION =
      "i18n::SharePointConnectionEditor.Group.Authentication";

  public static final String WIDGET_AUTHENTICATION_TYPE = "sharepoint-authentication-type";
  public static final String WIDGET_CLIENT_ID = "sharepoint-client-id";
  public static final String WIDGET_CLIENT_SECRET = "sharepoint-client-secret";
  public static final String WIDGET_CERTIFICATE = "sharepoint-certificate";

  public enum AuthenticationType implements IEnumHasCodeAndDescription {

    /** Managed Identity. */
    MANAGED_IDENTITY("SharePointAuthenticationType.ManagedIdentity"),
    /** Client Secret authentication. */
    SECRET("SharePointAuthenticationType.Secret"),
    /** Client Certificate authentication. */
    CERTIFICATE("SharePointAuthenticationType.Certificate");

    /** The authentification type description. */
    @Getter private final String description;

    AuthenticationType(String description) {
      this.description = BaseMessages.getString(PKG, description);
    }

    @Override
    public String getCode() {
      return name();
    }

    /**
     * Return the authentification type for a certain code
     *
     * @param code the code to look for
     * @return the authentification type or MANAGED_IDENTITY if nothing matches.
     */
    public static AuthenticationType lookupCode(String code) {
      return IEnumHasCode.lookupCode(AuthenticationType.class, code, MANAGED_IDENTITY);
    }

    /**
     * Return the authentification type for a certain description
     *
     * @param description the description to look for
     * @return the authentification type or MANAGED_IDENTITY if nothing matches.
     */
    public static AuthenticationType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          AuthenticationType.class, description, MANAGED_IDENTITY);
    }
  }

  public SharePointConnection() {
    // Default to client secret authentication
    this.authenticationType = AuthenticationType.SECRET;
  }

  @GuiWidgetElement(
      id = "sharepoint-description",
      order = "0100",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONNECTION,
      groupOrder = "010",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      variables = false,
      label = "i18n::SharePointConnectionEditor.Description.Label")
  @HopMetadataProperty
  private @Nullable String description;

  @GuiWidgetElement(
      id = WIDGET_AUTHENTICATION_TYPE,
      order = "0200",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::SharePointConnectionEditor.AuthenticationType.Label")
  @HopMetadataProperty(storeWithCode = true)
  private AuthenticationType authenticationType;

  @GuiWidgetElement(
      id = "sharepoint-tenant-id",
      order = "0300",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONNECTION,
      groupOrder = "010",
      type = GuiElementType.TEXT,
      label = "i18n::SharePointConnectionEditor.TenantId.Label",
      toolTip = "i18n::SharePointConnectionEditor.TenantId.Tooltip")
  @HopMetadataProperty
  private @Nullable String tenantId;

  /**
   * The SharePoint host of the tenant, such as contoso.sharepoint.com. Needed when the application
   * can not read the root site of the tenant to find it.
   */
  @GuiWidgetElement(
      id = "sharepoint-hostname",
      order = "0400",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONNECTION,
      groupOrder = "010",
      type = GuiElementType.TEXT,
      label = "i18n::SharePointConnectionEditor.Hostname.Label",
      toolTip = "i18n::SharePointConnectionEditor.Hostname.Tooltip")
  @HopMetadataProperty
  private @Nullable String hostname;

  @GuiWidgetElement(
      id = WIDGET_CLIENT_ID,
      order = "0500",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020",
      type = GuiElementType.TEXT,
      label = "i18n::SharePointConnectionEditor.ClientId.Label",
      toolTip = "i18n::SharePointConnectionEditor.ClientId.Tooltip")
  @HopMetadataProperty
  private @Nullable String clientId;

  @GuiWidgetElement(
      id = WIDGET_CLIENT_SECRET,
      order = "0600",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020",
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::SharePointConnectionEditor.ClientSecret.Label",
      toolTip = "i18n::SharePointConnectionEditor.ClientSecret.Tooltip")
  @HopMetadataProperty(password = true)
  private @Nullable String clientSecret;

  /** Certificate with its private key in PEM format */
  @GuiWidgetElement(
      id = WIDGET_CERTIFICATE,
      order = "0700",
      parentId = SharePointConnectionEditor.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 12,
      label = "i18n::SharePointConnectionEditor.PEMCertificate.Label",
      toolTip = "i18n::SharePointConnectionEditor.PEMCertificate.Tooltip")
  @HopMetadataProperty(password = true)
  private @Nullable String certificate;

  public TokenCredential credentials(ILogChannel log, IVariables variables) throws HopException {

    String resolvedTenantId = variables.resolve(getTenantId());

    return switch (authenticationType) {
      case SECRET -> {
        log.logBasic("SharePoint authentication is using Client Secret Credentials");
        yield new ClientSecretCredentialBuilder()
            .tenantId(required(resolvedTenantId, "SharePointConnectionEditor.TenantId.Label"))
            .clientId(
                required(
                    variables.resolve(getClientId()), "SharePointConnectionEditor.ClientId.Label"))
            .clientSecret(
                required(
                    variables.resolve(getClientSecret()),
                    "SharePointConnectionEditor.ClientSecret.Label"))
            .build();
      }
      case CERTIFICATE -> {
        log.logBasic("SharePoint authentication is using Client Certificate Credentials");
        String resolvedCertificate =
            required(
                variables.resolve(getCertificate()),
                "SharePointConnectionEditor.PEMCertificate.Label");
        yield new ClientCertificateCredentialBuilder()
            .tenantId(required(resolvedTenantId, "SharePointConnectionEditor.TenantId.Label"))
            .clientId(
                required(
                    variables.resolve(getClientId()), "SharePointConnectionEditor.ClientId.Label"))
            .pemCertificate(
                new ByteArrayInputStream(resolvedCertificate.getBytes(StandardCharsets.UTF_8)))
            .build();
      }
      case MANAGED_IDENTITY -> {
        log.logBasic("SharePoint authentication is using Default Azure Credentials");
        yield new DefaultAzureCredentialBuilder().tenantId(resolvedTenantId).build();
      }
    };
  }

  /**
   * @param value the resolved value of an option
   * @param labelKey the key of the label of the option in the editor
   * @return the value
   * @throws HopException if the option is empty
   */
  private static String required(@Nullable String value, String labelKey) throws HopException {
    if (value == null || value.isBlank()) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "SharePointConnection.MissingOption", BaseMessages.getString(PKG, labelKey)));
    }
    return value;
  }
}
