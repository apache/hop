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
 *
 */

package org.apache.hop.core.variables.resolver;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
@GuiPlugin
@VariableResolverPlugin(
    id = "AzureKeyVault",
    name = "Azure Key Vault Variable Resolver",
    description = "Resolves variables from Azure Key Vault secrets")
public class AzureKeyVaultVariableResolver implements IVariableResolver {

  private static final LogChannel log = new LogChannel("AzureKeyVaultVariableResolver");

  private transient SecretClient secretClient;
  private transient boolean initialized = false;
  private transient boolean failedInitialization = false;

  @GuiWidgetElement(
      id = "azureKeyVaultUri",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AzureKeyVaultVariableResolver.AzureKeyVaultUri.Label",
      toolTip = "i18n::AzureKeyVaultVariableResolver.AzureKeyVaultUri.Tooltip")
  @HopMetadataProperty
  private String azureKeyVaultUri;

  @GuiWidgetElement(
      id = "azureTenantId",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AzureKeyVaultVariableResolver.AzureTenantId.Label",
      toolTip = "i18n::AzureKeyVaultVariableResolver.AzureTenantId.Tooltip")
  @HopMetadataProperty
  private String azureTenantId;

  @GuiWidgetElement(
      id = "azureClientId",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AzureKeyVaultVariableResolver.AzureClientId.Label",
      toolTip = "i18n::AzureKeyVaultVariableResolver.AzureClientId.Tooltip")
  @HopMetadataProperty
  private String azureClientId;

  @GuiWidgetElement(
      id = "azureClientSecret",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AzureKeyVaultVariableResolver.AzureClientSecret.Label",
      toolTip = "i18n::AzureKeyVaultVariableResolver.AzureClientSecret.Tooltip",
      password = true)
  @HopMetadataProperty(password = true)
  private String azureClientSecret;

  @Override
  public synchronized void init() {
    // This method is kept for interface compatibility but does nothing
    // Actual initialization happens in init(IVariables) which is called from resolve()
  }

  /**
   * Initialize the Azure Key Vault client with variable resolution
   *
   * @param variables Variables to resolve configuration values
   */
  private synchronized void init(IVariables variables) {
    try {
      if (initialized || failedInitialization) {
        log.logDetailed(
            "Azure Key Vault Variable Resolver already initialized or failed. initialized="
                + initialized
                + ", failedInitialization="
                + failedInitialization);
        return;
      }
      log.logDetailed("=== AZURE KEY VAULT VARIABLE RESOLVER: Starting initialization ===");

      // Resolve variables in configuration fields
      String resolvedVaultUri = variables.resolve(getAzureKeyVaultUri());
      String resolvedTenantId = variables.resolve(getAzureTenantId());
      String resolvedClientId = variables.resolve(getAzureClientId());
      String resolvedClientSecret = variables.resolve(getAzureClientSecret());

      log.logDetailed(
          "Azure Key Vault URI: "
              + (StringUtils.isEmpty(resolvedVaultUri) ? "EMPTY" : resolvedVaultUri));
      log.logDetailed(
          "Azure Tenant ID: "
              + (StringUtils.isEmpty(resolvedTenantId) ? "EMPTY" : resolvedTenantId));
      log.logDetailed(
          "Azure Client ID: "
              + (StringUtils.isEmpty(resolvedClientId) ? "EMPTY" : resolvedClientId));
      log.logDetailed(
          "Azure Client Secret: "
              + (StringUtils.isEmpty(resolvedClientSecret) ? "EMPTY" : "***SET***"));

      if (StringUtils.isEmpty(resolvedVaultUri)
          || StringUtils.isEmpty(resolvedTenantId)
          || StringUtils.isEmpty(resolvedClientId)
          || StringUtils.isEmpty(resolvedClientSecret)) {
        throw new HopException(
            "Missing one or more required Azure Key Vault configuration values in the Variable Resolver metadata.");
      }
      // Decrypt the client secret (supports both encrypted and plain text values)
      String decryptedClientSecret = Encr.decryptPasswordOptionallyEncrypted(resolvedClientSecret);

      // Let Azure SDK auto-discover HTTP client from Hop's core libraries
      ClientSecretCredential credential =
          new ClientSecretCredentialBuilder()
              .tenantId(resolvedTenantId)
              .clientId(resolvedClientId)
              .clientSecret(decryptedClientSecret)
              .build();
      this.secretClient =
          new SecretClientBuilder().vaultUrl(resolvedVaultUri).credential(credential).buildClient();
      log.logDetailed("Successfully initialized Azure Key Vault client for: " + resolvedVaultUri);
      initialized = true;
    } catch (Exception e) {
      log.logError(
          "Failed to initialize Azure Key Vault client. This resolver will be disabled.", e);
      failedInitialization = true;
    }
  }

  @Override
  public String resolve(String secretId, IVariables variables) throws HopException {
    log.logDetailed(
        "=== AZURE KEY VAULT VARIABLE RESOLVER: resolve() called with secretId='"
            + secretId
            + "' ===");

    // Initialize with variables for proper field resolution
    init(variables);

    if (failedInitialization || this.secretClient == null) {
      log.logError(
          "Cannot resolve secret - initialization failed or secretClient is null. failedInitialization="
              + failedInitialization
              + ", secretClient="
              + (secretClient == null ? "null" : "present"));
      return null;
    }
    if (StringUtils.isEmpty(secretId)) {
      log.logError("Cannot resolve secret - secretId is empty");
      return null;
    }
    try {
      String resolvedVaultUri = variables.resolve(getAzureKeyVaultUri());
      log.logDetailed(
          "Attempting to resolve secret '"
              + secretId
              + "' from Azure Key Vault: "
              + resolvedVaultUri);
      KeyVaultSecret secret = this.secretClient.getSecret(secretId);
      String value = secret.getValue();
      log.logDetailed(
          "Successfully resolved secret '"
              + secretId
              + "' (value length: "
              + (value != null ? value.length() : 0)
              + ")");
      return value;
    } catch (ResourceNotFoundException e) {
      log.logError("Secret not found in Azure Key Vault: '" + secretId + "'", e);
      return null;
    } catch (Exception e) {
      log.logError(
          "An error occurred while fetching secret '" + secretId + "' from Azure Key Vault", e);
      return null;
    }
  }

  @Override
  public void setPluginId() {
    /* Not needed */
  }

  @Override
  public String getPluginId() {
    return "AzureKeyVault";
  }

  @Override
  public void setPluginName(String pluginName) {
    /* Not needed */
  }

  @Override
  public String getPluginName() {
    return "Azure Key Vault Variable Resolver";
  }
}
