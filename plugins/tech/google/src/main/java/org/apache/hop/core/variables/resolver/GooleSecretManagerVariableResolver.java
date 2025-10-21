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

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
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
    id = "Variable-Resolver-GoogleSecretManager",
    name = "Google Secret Manager Variable Resolver",
    description = "Automatically look up values of secrets in Google Secret Manager",
    documentationUrl =
        "/metadata-types/variable-resolver/google-secret-manager-variable-resolver.html")
public class GooleSecretManagerVariableResolver implements IVariableResolver {

  /** The name of the variable that will contain the expression in the pipeline. */
  @GuiWidgetElement(
      id = "projectId",
      order = "0!",
      label =
          "i18n:org.apache.hop.core.variables.resolver:GooleSecretManagerVariableResolver.label.ProjectId",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String projectId;

  /** The name of the variable that will contain the expression in the pipeline. */
  @GuiWidgetElement(
      id = "locationId",
      order = "02",
      label =
          "i18n:org.apache.hop.core.variables.resolver:GooleSecretManagerVariableResolver.label.LocationId",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String locationId;

  @Override
  public void init() {
    // Not used at this time.  If performance is too bad for the client construction we can still do
    // it.
    // For now we assume that it's all just using web services anyway.
  }

  @Override
  public String resolve(String secretId, IVariables variables) throws HopException {

    if (StringUtils.isEmpty(secretId)) {
      return null;
    }

    try {
      SecretManagerServiceSettings.Builder settingsBuilder =
          SecretManagerServiceSettings.newBuilder();
      if (StringUtils.isNotEmpty(locationId)) {
        String apiEndpoint = String.format("secretmanager.%s.rep.googleapis.com:443", locationId);
        settingsBuilder.setEndpoint(apiEndpoint);
      }
      SecretManagerServiceSettings settings = settingsBuilder.build();

      try (SecretManagerServiceClient client = SecretManagerServiceClient.create(settings)) {

        String actualLocationId = variables.resolve(locationId);
        String actualProjectId = variables.resolve(projectId);

        // Get the secret version name.
        //
        SecretVersionName secretName;
        if (StringUtils.isEmpty(actualLocationId)) {
          secretName =
              SecretVersionName.ofProjectSecretSecretVersionName(
                  actualProjectId, secretId, "latest");
        } else {
          secretName =
              SecretVersionName.ofProjectLocationSecretSecretVersionName(
                  actualProjectId, actualLocationId, secretId, "latest");
        }

        // Get the payload for this version
        //
        AccessSecretVersionResponse response = client.accessSecretVersion(secretName);

        // Create the secret.
        return response.getPayload().getData().toStringUtf8();
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error looking up secret key '" + secretId + "' in Google Secret Manager", e);
      return null;
    }
  }

  @Override
  public void setPluginId() {
    // Nothing to set
  }

  @Override
  public String getPluginId() {
    return "Variable-Resolver-GoogleSecretManager";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }

  @Override
  public String getPluginName() {
    return "Google Secret Manager Variable Resolver";
  }
}
