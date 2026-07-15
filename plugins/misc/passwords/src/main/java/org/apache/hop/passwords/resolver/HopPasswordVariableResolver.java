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

package org.apache.hop.passwords.resolver;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolverPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Resolves Hop-encoded (or optionally encrypted) password values for use in places that do not call
 * {@link Encr} themselves, such as VFS URIs (SFTP, etc.).
 *
 * <p>Expression format: {@code #{resolver-name:argument}}
 *
 * <ul>
 *   <li>With {@link #resolveAsVariable} enabled (recommended): {@code #{hop-pwd:SFTP_PASSWORD}}
 *       looks up variable {@code SFTP_PASSWORD}, then decrypts its value.
 *   <li>With {@link #resolveAsVariable} disabled: {@code #{hop-pwd:Encrypted abcd...}} decrypts the
 *       literal argument.
 * </ul>
 *
 * <p>This is an interoperability helper for trusted operators. The default Hop password encoder is
 * obfuscation, not strong encryption; use AES2 or a secrets manager for real confidentiality.
 */
@Getter
@Setter
@GuiPlugin
@VariableResolverPlugin(
    id = "HopPassword",
    name = "Hop Password Variable Resolver",
    description =
        "Decrypt Hop-encoded passwords (Encrypted / AES2) for use in expressions such as VFS URIs",
    documentationUrl = "/metadata-types/variable-resolver/hop-password-variable-resolver.html")
public class HopPasswordVariableResolver implements IVariableResolver {

  /**
   * When true, the expression argument after the first colon is treated as a variable name whose
   * value is looked up and then decrypted. When false, the argument is decrypted as a literal
   * encoded password string.
   */
  @GuiWidgetElement(
      id = "resolveAsVariable",
      order = "01",
      label = "i18n::HopPasswordVariableResolver.label.resolveAsVariable",
      toolTip = "i18n::HopPasswordVariableResolver.tooltip.resolveAsVariable",
      type = GuiElementType.CHECKBOX,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private boolean resolveAsVariable = true;

  /**
   * When true and {@link #resolveAsVariable} is enabled, throw if the named variable is not
   * defined. When false (default), a missing variable yields a null/empty resolution (expression
   * left unchanged by the variable system).
   */
  @GuiWidgetElement(
      id = "failIfVariableNotDefined",
      order = "02",
      label = "i18n::HopPasswordVariableResolver.label.failIfVariableNotDefined",
      toolTip = "i18n::HopPasswordVariableResolver.tooltip.failIfVariableNotDefined",
      type = GuiElementType.CHECKBOX,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private boolean failIfVariableNotDefined = false;

  @Override
  public void init() {
    // Nothing to initialize.
  }

  @Override
  public String getPluginId() {
    return "HopPassword";
  }

  @Override
  public String getPluginName() {
    return "Hop Password Variable Resolver";
  }

  @Override
  public String resolve(String secretPath, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(secretPath)) {
      return secretPath;
    }

    String encodedValue;
    if (resolveAsVariable) {
      String variableName = secretPath.trim();
      // Allow optional ${VAR} form if someone passes it literally.
      if (variableName.startsWith("${")
          && variableName.endsWith("}")
          && variableName.length() > 3) {
        variableName = variableName.substring(2, variableName.length() - 1);
      }
      encodedValue = variables.getVariable(variableName);
      if (encodedValue == null) {
        if (failIfVariableNotDefined) {
          throw new HopException("Password variable '" + variableName + "' is not defined");
        }
        return null;
      }
    } else {
      encodedValue = secretPath;
    }

    if (StringUtils.isEmpty(encodedValue)) {
      return encodedValue;
    }

    return Encr.decryptPasswordOptionallyEncrypted(encodedValue);
  }
}
