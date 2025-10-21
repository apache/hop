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

package org.apache.hop.core.variables.resolver.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.response.LogicalResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolverPlugin;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
@GuiPlugin
@VariableResolverPlugin(
    id = "Vault-Variable-Resolver",
    name = "Hashicorp Vault Variable Resolver",
    description = "Look up values of secrets in Hashicorp Vault",
    documentationUrl = "/metadata-types/variable-resolver/hashicorp-vault-variable-resolver.html")
public class VaultVariableResolver implements IVariableResolver {

  @GuiWidgetElement(
      id = "vaultAddress",
      order = "01",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.vaultAddress",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String vaultAddress;

  @GuiWidgetElement(
      id = "vaultToken",
      order = "02",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.vaultToken",
      type = GuiElementType.TEXT,
      password = true,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String vaultToken;

  @GuiWidgetElement(
      id = "verifyingSsl",
      order = "03",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.verifyingSsl",
      toolTip =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.tooltip.verifyingSsl",
      type = GuiElementType.CHECKBOX,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private boolean verifyingSsl;

  @GuiWidgetElement(
      id = "pemFilePath",
      order = "04",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.pemFilePath",
      type = GuiElementType.FILENAME,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String pemFilePath;

  @GuiWidgetElement(
      id = "pemString",
      order = "05",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.pemString",
      type = GuiElementType.TEXT,
      password = true,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String pemString;

  @GuiWidgetElement(
      id = "openTimeout",
      order = "06",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.openTimeout",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String openTimeout;

  @GuiWidgetElement(
      id = "readTimeout",
      order = "07",
      label =
          "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.label.readTimeout",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String readTimeout;

  @Override
  public String resolve(String secretPath, IVariables variables) throws HopException {
    try {
      // If we don't have any argument, give up immediately.
      //
      if (StringUtils.isEmpty(secretPath)) {
        return null;
      }

      String actualVaultToken = variables.resolve(vaultToken);
      String actualVaultAddress = variables.resolve(vaultAddress);
      final VaultConfig vaultConfig = new VaultConfig();
      vaultConfig.address(actualVaultAddress);
      vaultConfig.token(actualVaultToken);

      vaultConfig.sslVerify(isVerifyingSsl());
      String pemUtf8 = null;
      // Is our PEM String located in a file?
      //
      if (StringUtils.isNotEmpty(pemFilePath)) {
        try (InputStream is = HopVfs.getInputStream(variables.resolve(pemFilePath))) {
          pemUtf8 = readUtf8StringFromInputStream(is);
        }
      } else if (StringUtils.isNotEmpty(pemString)) {
        pemUtf8 = variables.resolve(pemString);
      }
      if (StringUtils.isNotEmpty(pemUtf8)) {
        vaultConfig.sslPemUTF8(pemUtf8);
      }

      if (StringUtils.isNotEmpty(openTimeout)) {
        int timeOut = Const.toInt(variables.resolve(openTimeout), -1);
        if (timeOut >= 0) {
          vaultConfig.openTimeout(timeOut);
        }
      }
      if (StringUtils.isNotEmpty(readTimeout)) {
        int timeOut = Const.toInt(variables.resolve(readTimeout), -1);
        if (timeOut >= 0) {
          vaultConfig.readTimeout(timeOut);
        }
      }

      vaultConfig.build();

      final Vault vault = new Vault(vaultConfig);

      LogicalResponse logicalResponse = vault.logical().read(secretPath);
      if (logicalResponse == null) {
        LogChannel.GENERAL.logDetailed(
            "The secret with path '" + secretPath + "' was not found in the vault");
        return null;
      }
      // If we don't have a value to retrieve, simple return the "data" value.
      //
      return logicalResponse.getData().get("data");
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error looking up secret '" + secretPath + "' in the Hashicorp Vault Variable resolver",
          e);
      return null;
    }
  }

  // Read the PEM file content in UTF8 from an input stream.
  //
  private String readUtf8StringFromInputStream(final InputStream input) throws IOException {
    final StringBuilder utf8 = new StringBuilder();
    try (BufferedReader in =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String string;
      while ((string = in.readLine()) != null) {
        utf8.append(string);
        utf8.append(Const.CR);
      }
    }
    return utf8.toString();
  }

  @Override
  public void setPluginId() {
    // Nothing to set
  }

  @Override
  public void init() {
    // Not used today
  }

  @Override
  public String getPluginId() {
    return "Vault-Variable-Resolver";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }

  @Override
  public String getPluginName() {
    return "Hashicorp Vault Variable Resolver";
  }
}
