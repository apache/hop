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

import io.github.jopenlibs.vault.SslConfig;
import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.response.AuthResponse;
import io.github.jopenlibs.vault.response.LogicalResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ComboVar;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

@Getter
@Setter
public abstract class BaseVaultVariableResolver
    implements IVariableResolver, IGuiPluginCompositeWidgetsListener {

  static final String ID_VAULT_ADDRESS = "vaultAddress";
  static final String ID_AUTHENTICATION_TYPE = "authenticationType";
  static final String ID_VAULT_TOKEN = "vaultToken";
  static final String ID_KUBERNETES_ROLE = "kubernetesRole";
  static final String ID_KUBERNETES_JWT_PATH = "kubernetesJwtPath";
  static final String ID_KUBERNETES_JWT = "kubernetesJwt";
  static final String ID_KUBERNETES_AUTH_PATH = "kubernetesAuthPath";
  static final String ID_PATH_PREFIX = "pathPrefix";
  static final String ID_NAMESPACE = "namespace";
  static final String ID_VERIFYING_SSL = "verifyingSsl";
  static final String ID_PEM_FILE_PATH = "pemFilePath";
  static final String ID_PEM_STRING = "pemString";
  static final String ID_OPEN_TIMEOUT = "openTimeout";
  static final String ID_READ_TIMEOUT = "readTimeout";

  static final String DEFAULT_KUBERNETES_JWT_PATH =
      "/var/run/secrets/kubernetes.io/serviceaccount/token";
  static final String DEFAULT_KUBERNETES_AUTH_MOUNT = "kubernetes";
  static final String I18N_PREFIX =
      "i18n:org.apache.hop.core.variables.resolver.vault:VaultVariableResolver.";

  private static final String GROUP_CONNECTION = "Connection";
  private static final String GROUP_AUTHENTICATION = "Authentication";
  private static final String GROUP_SECRETS = "Secrets";
  private static final long REFRESH_MARGIN_MILLIS = 30_000L;

  private final Object clientLock = new Object();

  private transient Vault vaultClient;
  private transient String clientSignature;
  private transient long tokenExpiryMillis;
  private transient boolean tokenRenewable;

  @GuiWidgetElement(
      id = ID_VAULT_ADDRESS,
      order = "010",
      label = I18N_PREFIX + "label.vaultAddress",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String vaultAddress;

  @GuiWidgetElement(
      id = ID_NAMESPACE,
      order = "020",
      label = I18N_PREFIX + "label.namespace",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String namespace;

  @GuiWidgetElement(
      id = ID_VERIFYING_SSL,
      order = "030",
      label = I18N_PREFIX + "label.verifyingSsl",
      toolTip = I18N_PREFIX + "tooltip.verifyingSsl",
      type = GuiElementType.CHECKBOX,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected boolean verifyingSsl;

  @GuiWidgetElement(
      id = ID_PEM_FILE_PATH,
      order = "040",
      label = I18N_PREFIX + "label.pemFilePath",
      type = GuiElementType.FILENAME,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String pemFilePath;

  @GuiWidgetElement(
      id = ID_PEM_STRING,
      order = "050",
      label = I18N_PREFIX + "label.pemString",
      type = GuiElementType.TEXT,
      password = true,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String pemString;

  @GuiWidgetElement(
      id = ID_OPEN_TIMEOUT,
      order = "060",
      label = I18N_PREFIX + "label.openTimeout",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String openTimeout;

  @GuiWidgetElement(
      id = ID_READ_TIMEOUT,
      order = "070",
      label = I18N_PREFIX + "label.readTimeout",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_CONNECTION,
      groupOrder = "010")
  @HopMetadataProperty
  protected String readTimeout;

  @GuiWidgetElement(
      id = ID_AUTHENTICATION_TYPE,
      order = "010",
      label = I18N_PREFIX + "label.authenticationType",
      toolTip = I18N_PREFIX + "tooltip.authenticationType",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getAuthenticationTypes",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String authenticationType;

  @GuiWidgetElement(
      id = ID_VAULT_TOKEN,
      order = "020",
      label = I18N_PREFIX + "label.vaultToken",
      type = GuiElementType.TEXT,
      password = true,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String vaultToken;

  @GuiWidgetElement(
      id = ID_KUBERNETES_ROLE,
      order = "030",
      label = I18N_PREFIX + "label.kubernetesRole",
      toolTip = I18N_PREFIX + "tooltip.kubernetesRole",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String kubernetesRole;

  @GuiWidgetElement(
      id = ID_KUBERNETES_JWT_PATH,
      order = "040",
      label = I18N_PREFIX + "label.kubernetesJwtPath",
      toolTip = I18N_PREFIX + "tooltip.kubernetesJwtPath",
      type = GuiElementType.FILENAME,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String kubernetesJwtPath;

  @GuiWidgetElement(
      id = ID_KUBERNETES_JWT,
      order = "050",
      label = I18N_PREFIX + "label.kubernetesJwt",
      toolTip = I18N_PREFIX + "tooltip.kubernetesJwt",
      type = GuiElementType.TEXT,
      password = true,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String kubernetesJwt;

  @GuiWidgetElement(
      id = ID_KUBERNETES_AUTH_PATH,
      order = "060",
      label = I18N_PREFIX + "label.kubernetesAuthPath",
      toolTip = I18N_PREFIX + "tooltip.kubernetesAuthPath",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_AUTHENTICATION,
      groupOrder = "020")
  @HopMetadataProperty
  protected String kubernetesAuthPath;

  @GuiWidgetElement(
      id = ID_PATH_PREFIX,
      order = "010",
      label = I18N_PREFIX + "label.pathPrefix",
      type = GuiElementType.TEXT,
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_SECRETS,
      groupOrder = "030")
  @HopMetadataProperty
  protected String pathPrefix;

  protected BaseVaultVariableResolver() {
    authenticationType = VaultAuthType.TOKEN.name();
  }

  @Override
  public String resolve(String secretPath, IVariables variables) throws HopException {
    try {
      if (StringUtils.isEmpty(secretPath)) {
        return null;
      }

      Vault vault = getVault(variables);

      String path;
      if (StringUtils.isNotEmpty(pathPrefix)) {
        path = variables.resolve(pathPrefix) + secretPath;
      } else {
        path = secretPath;
      }

      LogicalResponse logicalResponse = vault.logical().read(path);
      if (logicalResponse == null) {
        LogChannel.GENERAL.logDetailed(
            "The secret with path '" + secretPath + "' was not found in the vault");
        return null;
      }
      // If we don't have a value to retrieve, simply return the "data" value.
      //
      return logicalResponse.getData().get("data");
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error looking up secret '" + secretPath + "' in the Variable resolver", e);
      return null;
    }
  }

  /**
   * Returns a Vault client for the current configuration, logging in or renewing when needed. The
   * client is reused until the resolved configuration changes or a Kubernetes token is close to
   * expiry.
   */
  protected Vault getVault(IVariables variables) throws HopException {
    String signature = clientSignature(variables);

    synchronized (clientLock) {
      if (vaultClient != null && signature.equals(clientSignature) && !tokenNeedsRefresh()) {
        return vaultClient;
      }
      if (vaultClient != null
          && signature.equals(clientSignature)
          && tokenNeedsRefresh()
          && tokenRenewable
          && tryRenew()) {
        return vaultClient;
      }

      vaultClient = buildVault(variables);
      clientSignature = signature;
      return vaultClient;
    }
  }

  protected Vault buildVault(IVariables variables) throws HopException {
    VaultAuthType authType = parseAuthType(variables.resolve(authenticationType));
    VaultConfig vaultConfig = buildBaseConfig(variables);

    if (authType == VaultAuthType.KUBERNETES) {
      return buildKubernetesVault(variables, vaultConfig);
    }
    return buildTokenVault(variables, vaultConfig);
  }

  private Vault buildTokenVault(IVariables variables, VaultConfig vaultConfig) throws HopException {
    String actualVaultToken = variables.resolve(vaultToken);
    if (StringUtils.isEmpty(actualVaultToken)) {
      throw new HopException(
          "A Vault token is required when the authentication type is "
              + VaultAuthType.TOKEN.name());
    }
    vaultConfig.token(actualVaultToken);
    finalizeConfig(vaultConfig);
    tokenExpiryMillis = 0L;
    tokenRenewable = false;
    return createVault(vaultConfig);
  }

  private Vault buildKubernetesVault(IVariables variables, VaultConfig vaultConfig)
      throws HopException {
    String role = variables.resolve(kubernetesRole);
    if (StringUtils.isEmpty(role)) {
      throw new HopException(
          "A Kubernetes role is required when the authentication type is "
              + VaultAuthType.KUBERNETES.name());
    }
    String jwt = loadServiceAccountJwt(variables);
    String loginPath = kubernetesLoginPath(variables.resolve(kubernetesAuthPath));

    finalizeConfig(vaultConfig);
    Vault loginVault = createVault(vaultConfig);
    AuthResponse response = loginByKubernetes(loginVault, role, jwt, loginPath);
    String clientToken = response == null ? null : response.getAuthClientToken();
    if (StringUtils.isEmpty(clientToken)) {
      throw new HopException(
          "Vault Kubernetes authentication did not return a client token for role '" + role + "'");
    }

    vaultConfig.token(clientToken);
    finalizeConfig(vaultConfig);
    updateLease(response);
    return createVault(vaultConfig);
  }

  private static void finalizeConfig(VaultConfig vaultConfig) throws HopException {
    try {
      vaultConfig.build();
    } catch (Exception e) {
      throw new HopException("Error building the Vault client configuration", e);
    }
  }

  protected Vault createVault(VaultConfig vaultConfig) {
    return Vault.create(vaultConfig);
  }

  protected AuthResponse loginByKubernetes(Vault vault, String role, String jwt, String loginPath)
      throws HopException {
    try {
      if (DEFAULT_KUBERNETES_LOGIN_PATH.equals(loginPath)) {
        return vault.auth().loginByKubernetes(role, jwt);
      }
      return vault.auth().loginByKubernetes(role, jwt, loginPath);
    } catch (Exception e) {
      throw new HopException("Unable to log in to Vault with Kubernetes authentication", e);
    }
  }

  private static final String DEFAULT_KUBERNETES_LOGIN_PATH =
      "auth/" + DEFAULT_KUBERNETES_AUTH_MOUNT;

  protected String loadServiceAccountJwt(IVariables variables) throws HopException {
    String jwt = variables.resolve(kubernetesJwt);
    if (StringUtils.isNotEmpty(jwt)) {
      return jwt.trim();
    }
    String path = variables.resolve(kubernetesJwtPath);
    if (StringUtils.isEmpty(path)) {
      path = DEFAULT_KUBERNETES_JWT_PATH;
    }
    try (InputStream is = HopVfs.getInputStream(path, variables)) {
      return readUtf8StringFromInputStream(is).trim();
    } catch (Exception e) {
      throw new HopException(
          "Could not read the Kubernetes service account token from '" + path + "'", e);
    }
  }

  static String kubernetesLoginPath(String mount) {
    if (StringUtils.isEmpty(mount)) {
      return DEFAULT_KUBERNETES_LOGIN_PATH;
    }
    String trimmed = mount.trim();
    if (trimmed.startsWith("auth/")) {
      return trimmed;
    }
    return "auth/" + trimmed;
  }

  VaultAuthType parseAuthType(String actualAuthType) throws HopException {
    if (StringUtils.isEmpty(actualAuthType)) {
      return VaultAuthType.TOKEN;
    }
    try {
      return VaultAuthType.valueOf(actualAuthType.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new HopException(
          "Unknown Vault authentication type '"
              + actualAuthType
              + "'. Valid values are: "
              + String.join(", ", authTypeNames()),
          e);
    }
  }

  public List<String> getAuthenticationTypes(
      ILogChannel logChannel, IHopMetadataProvider metadataProvider) {
    return authTypeNames();
  }

  private static List<String> authTypeNames() {
    List<String> names = new ArrayList<>();
    for (VaultAuthType type : VaultAuthType.values()) {
      names.add(type.name());
    }
    return names;
  }

  private VaultConfig buildBaseConfig(IVariables variables) throws HopException {
    try {
      String actualVaultAddress = variables.resolve(vaultAddress);
      final VaultConfig vaultConfig = new VaultConfig();
      vaultConfig.address(actualVaultAddress);
      vaultConfig.engineVersion(1);

      if (StringUtils.isNotEmpty(namespace)) {
        vaultConfig.nameSpace(variables.resolve(namespace));
      }

      final SslConfig sslConfig = new SslConfig();
      sslConfig.verify(isVerifyingSsl());
      String pemUtf8 = null;
      if (StringUtils.isNotEmpty(pemFilePath)) {
        try (InputStream is = HopVfs.getInputStream(variables.resolve(pemFilePath), variables)) {
          pemUtf8 = readUtf8StringFromInputStream(is);
        }
      } else if (StringUtils.isNotEmpty(pemString)) {
        pemUtf8 = variables.resolve(pemString);
      }
      if (StringUtils.isNotEmpty(pemUtf8)) {
        sslConfig.pemUTF8(pemUtf8);
      }
      sslConfig.build();
      vaultConfig.sslConfig(sslConfig);

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
      return vaultConfig;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error building the Vault client configuration", e);
    }
  }

  private String clientSignature(IVariables variables) {
    return String.join(
        "\t",
        Const.NVL(variables.resolve(vaultAddress), ""),
        Const.NVL(variables.resolve(authenticationType), ""),
        Const.NVL(variables.resolve(vaultToken), ""),
        Const.NVL(variables.resolve(kubernetesRole), ""),
        Const.NVL(variables.resolve(kubernetesJwtPath), ""),
        Const.NVL(variables.resolve(kubernetesJwt), ""),
        Const.NVL(variables.resolve(kubernetesAuthPath), ""),
        Const.NVL(variables.resolve(namespace), ""),
        Boolean.toString(verifyingSsl),
        Const.NVL(variables.resolve(pemFilePath), ""),
        Const.NVL(variables.resolve(pemString), ""),
        Const.NVL(variables.resolve(openTimeout), ""),
        Const.NVL(variables.resolve(readTimeout), ""));
  }

  private boolean tokenNeedsRefresh() {
    if (tokenExpiryMillis <= 0L) {
      return false;
    }
    long remaining = tokenExpiryMillis - System.currentTimeMillis();
    if (remaining <= 0L) {
      return true;
    }
    return tokenRenewable && remaining <= REFRESH_MARGIN_MILLIS;
  }

  private boolean tryRenew() {
    try {
      AuthResponse renewed = vaultClient.auth().renewSelf();
      updateLease(renewed);
      return true;
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "Could not renew the Vault token, will re-authenticate instead", e);
      return false;
    }
  }

  private void updateLease(AuthResponse response) {
    if (response == null) {
      tokenExpiryMillis = 0L;
      tokenRenewable = false;
      return;
    }
    long leaseSeconds = response.getAuthLeaseDuration();
    tokenRenewable = response.isAuthRenewable();
    tokenExpiryMillis = leaseSeconds <= 0L ? 0L : System.currentTimeMillis() + leaseSeconds * 1000L;
  }

  void expireCachedToken() {
    tokenExpiryMillis = System.currentTimeMillis();
    tokenRenewable = false;
  }

  // Read the PEM or JWT file content in UTF8 from an input stream.
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
  public void init() {
    // The client is built lazily on the first resolve() call: only then do we have the variables
    // needed to resolve the configuration fields.
  }

  @Override
  public abstract String getPluginId();

  @Override
  public abstract String getPluginName();

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    if (ID_AUTHENTICATION_TYPE.equals(widgetId)) {
      hideFieldsThatDoNotApply(compositeWidgets);
    }
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // Not needed, the editor reads the widgets back itself.
  }

  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    VaultAuthType authType = readAuthType(compositeWidgets);
    Set<String> hidden = new HashSet<>();
    if (authType != VaultAuthType.TOKEN) {
      hidden.add(ID_VAULT_TOKEN);
    }
    if (authType != VaultAuthType.KUBERNETES) {
      hidden.add(ID_KUBERNETES_ROLE);
      hidden.add(ID_KUBERNETES_JWT_PATH);
      hidden.add(ID_KUBERNETES_JWT);
      hidden.add(ID_KUBERNETES_AUTH_PATH);
    }
    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  private VaultAuthType readAuthType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_AUTHENTICATION_TYPE);
    String text = comboText(control);
    if (StringUtils.isNotEmpty(text)) {
      try {
        return VaultAuthType.valueOf(text.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        // A variable reference or nothing picked yet, so fall through to metadata.
      }
    }
    try {
      return parseAuthType(authenticationType);
    } catch (HopException e) {
      return VaultAuthType.TOKEN;
    }
  }

  private static String comboText(Control control) {
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    }
    return null;
  }
}
