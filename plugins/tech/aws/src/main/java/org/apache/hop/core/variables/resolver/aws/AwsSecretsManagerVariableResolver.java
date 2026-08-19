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

package org.apache.hop.core.variables.resolver.aws;

import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.resolver.IVariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.core.variables.resolver.VariableResolverPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.RegionMetadata;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * Looks up secrets in AWS Secrets Manager.
 *
 * <p>A secret is addressed by its name, not by its ARN: the variable resolver expression syntax
 * <code>#{name:secret:key}</code> splits on the colon, and an ARN contains colons of its own. Use
 * the secret name prefix option to reach secrets that need a longer path.
 *
 * <p>Secrets Manager secrets frequently hold JSON. Picking a single value out of that JSON is
 * handled generically by the variable resolver machinery, so this resolver simply returns the
 * secret payload as it is stored.
 */
@Getter
@Setter
@GuiPlugin
@VariableResolverPlugin(
    id = "AwsSecretsManager",
    name = "AWS Secrets Manager Variable Resolver",
    description = "Look up values of secrets in AWS Secrets Manager",
    documentationUrl =
        "/metadata-types/variable-resolver/aws-secrets-manager-variable-resolver.html")
public class AwsSecretsManagerVariableResolver
    implements IVariableResolver, IGuiPluginCompositeWidgetsListener {

  private static final String DEFAULT_VERSION_STAGE = "AWSCURRENT";

  /** Separates the region code from its description in the region combo box. */
  private static final String REGION_DESCRIPTION_SEPARATOR = " - ";

  static final String ID_REGION = "region";
  static final String ID_AUTHENTICATION_TYPE = "authenticationType";
  static final String ID_ACCESS_KEY = "accessKey";
  static final String ID_SECRET_KEY = "secretKey";
  static final String ID_SESSION_TOKEN = "sessionToken";
  static final String ID_CREDENTIALS_FILE = "credentialsFile";
  static final String ID_PROFILE_NAME = "profileName";
  static final String ID_ENDPOINT_OVERRIDE = "endpointOverride";
  static final String ID_SECRET_NAME_PREFIX = "secretNamePrefix";
  static final String ID_VERSION_STAGE = "versionStage";
  static final String ID_CACHE_TTL_SECONDS = "cacheTtlSeconds";

  private final LogChannel log = new LogChannel("AwsSecretsManagerVariableResolver");

  /** Guards {@link #client}, {@link #clientSignature} and {@link #cache}. */
  private final Object clientLock = new Object();

  private transient SecretsManagerClient client;

  /** The resolved configuration the cached client was built for. */
  private transient String clientSignature;

  private final transient Map<String, CachedSecret> cache = new ConcurrentHashMap<>();

  @GuiWidgetElement(
      id = ID_REGION,
      order = "010",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      comboValuesMethod = "getRegions",
      label = "i18n::AwsSecretsManagerVariableResolver.Region.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.Region.Tooltip")
  @HopMetadataProperty
  private String region;

  @GuiWidgetElement(
      id = ID_AUTHENTICATION_TYPE,
      order = "020",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      comboValuesMethod = "getAuthenticationTypes",
      label = "i18n::AwsSecretsManagerVariableResolver.AuthenticationType.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.AuthenticationType.Tooltip")
  @HopMetadataProperty
  private String authenticationType;

  @GuiWidgetElement(
      id = ID_ACCESS_KEY,
      order = "030",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AwsSecretsManagerVariableResolver.AccessKey.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.AccessKey.Tooltip")
  @HopMetadataProperty(password = true)
  private String accessKey;

  @GuiWidgetElement(
      id = ID_SECRET_KEY,
      order = "040",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AwsSecretsManagerVariableResolver.SecretKey.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.SecretKey.Tooltip")
  @HopMetadataProperty(password = true)
  private String secretKey;

  @GuiWidgetElement(
      id = ID_SESSION_TOKEN,
      order = "050",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::AwsSecretsManagerVariableResolver.SessionToken.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.SessionToken.Tooltip")
  @HopMetadataProperty(password = true)
  private String sessionToken;

  @GuiWidgetElement(
      id = ID_CREDENTIALS_FILE,
      order = "060",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::AwsSecretsManagerVariableResolver.CredentialsFile.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.CredentialsFile.Tooltip")
  @HopMetadataProperty
  private String credentialsFile;

  @GuiWidgetElement(
      id = ID_PROFILE_NAME,
      order = "070",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AwsSecretsManagerVariableResolver.ProfileName.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.ProfileName.Tooltip")
  @HopMetadataProperty
  private String profileName;

  @GuiWidgetElement(
      id = ID_ENDPOINT_OVERRIDE,
      order = "080",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AwsSecretsManagerVariableResolver.EndpointOverride.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.EndpointOverride.Tooltip")
  @HopMetadataProperty
  private String endpointOverride;

  @GuiWidgetElement(
      id = ID_SECRET_NAME_PREFIX,
      order = "090",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AwsSecretsManagerVariableResolver.SecretNamePrefix.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.SecretNamePrefix.Tooltip")
  @HopMetadataProperty
  private String secretNamePrefix;

  @GuiWidgetElement(
      id = ID_VERSION_STAGE,
      order = "100",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AwsSecretsManagerVariableResolver.VersionStage.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.VersionStage.Tooltip")
  @HopMetadataProperty
  private String versionStage;

  @GuiWidgetElement(
      id = ID_CACHE_TTL_SECONDS,
      order = "110",
      parentId = VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::AwsSecretsManagerVariableResolver.CacheTtlSeconds.Label",
      toolTip = "i18n::AwsSecretsManagerVariableResolver.CacheTtlSeconds.Tooltip")
  @HopMetadataProperty
  private String cacheTtlSeconds;

  public AwsSecretsManagerVariableResolver() {
    authenticationType = AwsSecretsManagerAuthType.AUTOMATIC.name();
    versionStage = DEFAULT_VERSION_STAGE;
    cacheTtlSeconds = "0";
  }

  @Override
  public void init() {
    // The client is built lazily on the first resolve() call: only then do we have the variables
    // needed to resolve the configuration fields.
  }

  @Override
  public String resolve(String secretName, IVariables variables) throws HopException {
    if (StringUtils.isEmpty(secretName)) {
      return null;
    }

    // An ARN never reaches us intact: the expression is split on ':' before this method is called,
    // so #{name:arn:aws:secretsmanager:...} arrives here as the bare string "arn". Refusing it is
    // friendlier than looking up a secret that was never meant.
    if ("arn".equals(secretName) || secretName.startsWith("arn:")) {
      log.logError(
          "Secret '"
              + secretName
              + "' looks like an ARN. Variable resolver expressions are split on ':', so an ARN "
              + "cannot be used here. Use the name of the secret and, when the names share a "
              + "common path, the secret name prefix option of this resolver.");
      return null;
    }

    String prefix = variables.resolve(secretNamePrefix);
    String secretId = StringUtils.isEmpty(prefix) ? secretName : prefix + secretName;

    long ttl = Const.toLong(variables.resolve(cacheTtlSeconds), 0L);
    if (ttl > 0) {
      CachedSecret cached = cache.get(secretId);
      if (cached != null && !cached.isExpired()) {
        return cached.value;
      }
    }

    try {
      SecretsManagerClient secretsManagerClient = getClient(variables);

      String stage = variables.resolve(versionStage);
      GetSecretValueRequest request =
          GetSecretValueRequest.builder()
              .secretId(secretId)
              .versionStage(StringUtils.isEmpty(stage) ? DEFAULT_VERSION_STAGE : stage)
              .build();

      GetSecretValueResponse response = secretsManagerClient.getSecretValue(request);
      String value = extractValue(secretId, response);

      if (value != null && ttl > 0) {
        cache.put(secretId, new CachedSecret(value, System.currentTimeMillis() + ttl * 1000L));
      }
      return value;
    } catch (ResourceNotFoundException e) {
      log.logError("Secret '" + secretId + "' was not found in AWS Secrets Manager", e);
      return null;
    } catch (Exception e) {
      log.logError("Error looking up secret '" + secretId + "' in AWS Secrets Manager", e);
      return null;
    }
  }

  /**
   * A Secrets Manager secret holds either a string or a binary payload. Binary secrets are returned
   * base64 encoded since a variable value has to be a String.
   */
  private String extractValue(String secretId, GetSecretValueResponse response) {
    if (response.secretString() != null) {
      return response.secretString();
    }
    if (response.secretBinary() != null) {
      log.logDetailed(
          "Secret '" + secretId + "' holds a binary value, returning it base64 encoded.");
      return Base64.getEncoder().encodeToString(response.secretBinary().asByteArray());
    }
    log.logError("Secret '" + secretId + "' holds neither a string nor a binary value");
    return null;
  }

  /**
   * Secrets Manager bills per API call and variable resolution happens often, so the client is
   * built once and kept. It is rebuilt when the resolved configuration changes, which can happen
   * when the configuration fields themselves contain variables.
   */
  protected SecretsManagerClient getClient(IVariables variables) throws HopException {
    String actualRegion = variables.resolve(region);
    String actualAuthType = variables.resolve(authenticationType);
    String actualAccessKey = variables.resolve(accessKey);
    String actualSecretKey = variables.resolve(secretKey);
    String actualSessionToken = variables.resolve(sessionToken);
    String actualCredentialsFile = variables.resolve(credentialsFile);
    String actualProfileName = variables.resolve(profileName);
    String actualEndpoint = variables.resolve(endpointOverride);

    String signature =
        String.join(
            "\t",
            Const.NVL(actualRegion, ""),
            Const.NVL(actualAuthType, ""),
            Const.NVL(actualAccessKey, ""),
            Const.NVL(actualSecretKey, ""),
            Const.NVL(actualSessionToken, ""),
            Const.NVL(actualCredentialsFile, ""),
            Const.NVL(actualProfileName, ""),
            Const.NVL(actualEndpoint, ""));

    synchronized (clientLock) {
      if (client != null && signature.equals(clientSignature)) {
        return client;
      }

      if (client != null) {
        // The configuration changed, so anything we cached for it is stale too.
        client.close();
        cache.clear();
      }

      SecretsManagerClientBuilder builder = SecretsManagerClient.builder();
      builder.region(resolveRegion(actualRegion));
      builder.credentialsProvider(
          buildCredentialsProvider(
              actualAuthType,
              actualAccessKey,
              actualSecretKey,
              actualSessionToken,
              actualCredentialsFile,
              actualProfileName));
      if (StringUtils.isNotEmpty(actualEndpoint)) {
        builder.endpointOverride(URI.create(actualEndpoint));
      }

      client = builder.build();
      clientSignature = signature;
      return client;
    }
  }

  /**
   * The AWS SDK needs a region to build a client. When none is configured we let the SDK look one
   * up the way the AWS CLI does, through AWS_REGION or the active profile.
   */
  private Region resolveRegion(String actualRegion) throws HopException {
    String code = regionCode(actualRegion);
    if (StringUtils.isNotEmpty(code)) {
      return Region.of(code);
    }
    try {
      return new DefaultAwsRegionProviderChain().getRegion();
    } catch (Exception e) {
      throw new HopException(
          "No AWS region is configured in this variable resolver and none could be found in the "
              + "environment. Set the region option or the AWS_REGION environment variable.",
          e);
    }
  }

  /**
   * The region combo box shows the code together with the description AWS gives it, so a stored
   * value can read "eu-west-1 - Europe (Ireland)". A region code never contains whitespace, which
   * makes the first word the code whichever of the two forms was saved.
   */
  static String regionCode(String region) {
    if (StringUtils.isEmpty(region)) {
      return region;
    }
    return region.trim().split("\\s", 2)[0];
  }

  AwsCredentialsProvider buildCredentialsProvider(
      String actualAuthType,
      String actualAccessKey,
      String actualSecretKey,
      String actualSessionToken,
      String actualCredentialsFile,
      String actualProfileName)
      throws HopException {

    AwsSecretsManagerAuthType authType = parseAuthType(actualAuthType);

    switch (authType) {
      case ACCESS_KEYS:
        if (StringUtils.isEmpty(actualAccessKey) || StringUtils.isEmpty(actualSecretKey)) {
          throw new HopException(
              "An access key and a secret key are required when the authentication type is "
                  + AwsSecretsManagerAuthType.ACCESS_KEYS.name());
        }
        String decryptedSecretKey = Encr.decryptPasswordOptionallyEncrypted(actualSecretKey);
        if (StringUtils.isEmpty(actualSessionToken)) {
          return StaticCredentialsProvider.create(
              AwsBasicCredentials.create(actualAccessKey, decryptedSecretKey));
        }
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(
                actualAccessKey,
                decryptedSecretKey,
                Encr.decryptPasswordOptionallyEncrypted(actualSessionToken)));

      case CREDENTIALS_FILE:
        ProfileCredentialsProvider.Builder profileBuilder = ProfileCredentialsProvider.builder();
        if (StringUtils.isNotEmpty(actualCredentialsFile)) {
          profileBuilder.profileFile(
              ProfileFile.builder()
                  .content(Paths.get(actualCredentialsFile))
                  .type(ProfileFile.Type.CREDENTIALS)
                  .build());
        }
        profileBuilder.profileName(
            StringUtils.isEmpty(actualProfileName) ? "default" : actualProfileName);
        return profileBuilder.build();

      default:
        return DefaultCredentialsProvider.create();
    }
  }

  AwsSecretsManagerAuthType parseAuthType(String actualAuthType) throws HopException {
    if (StringUtils.isEmpty(actualAuthType)) {
      return AwsSecretsManagerAuthType.AUTOMATIC;
    }
    try {
      return AwsSecretsManagerAuthType.valueOf(actualAuthType.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new HopException(
          "Unknown AWS authentication type '"
              + actualAuthType
              + "'. Valid values are: "
              + String.join(", ", authTypeNames()),
          e);
    }
  }

  private static List<String> authTypeNames() {
    List<String> names = new ArrayList<>();
    for (AwsSecretsManagerAuthType type : AwsSecretsManagerAuthType.values()) {
      names.add(type.name());
    }
    return names;
  }

  /**
   * Populates the region combo box from the regions the AWS SDK knows about, each with the
   * description AWS gives it, so that "eu-west-1" reads as "eu-west-1 - Europe (Ireland)".
   */
  public List<String> getRegions(ILogChannel logChannel, IHopMetadataProvider metadataProvider) {
    List<String> regions = new ArrayList<>();
    // An empty entry leaves the region to the environment.
    regions.add("");
    Region.regions().stream()
        .map(Region::id)
        .sorted()
        .forEach(id -> regions.add(id + REGION_DESCRIPTION_SEPARATOR + describeRegion(id)));
    return regions;
  }

  private String describeRegion(String id) {
    try {
      return RegionMetadata.of(Region.of(id)).description();
    } catch (Exception e) {
      // A region the SDK knows by id but carries no description for.
      return id;
    }
  }

  /** Populates the authentication type combo box. */
  public List<String> getAuthenticationTypes(
      ILogChannel logChannel, IHopMetadataProvider metadataProvider) {
    return authTypeNames();
  }

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

  /**
   * Keeps the editor down to the credentials that the chosen authentication type actually uses. The
   * three types have nothing in common, so leaving all of them on screen means most of the fields
   * can only ever stay empty.
   */
  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    AwsSecretsManagerAuthType authType = readAuthType(compositeWidgets);

    Set<String> hidden = new HashSet<>();
    if (authType != AwsSecretsManagerAuthType.ACCESS_KEYS) {
      hidden.add(ID_ACCESS_KEY);
      hidden.add(ID_SECRET_KEY);
      hidden.add(ID_SESSION_TOKEN);
    }
    if (authType != AwsSecretsManagerAuthType.CREDENTIALS_FILE) {
      hidden.add(ID_CREDENTIALS_FILE);
      hidden.add(ID_PROFILE_NAME);
    }
    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  /**
   * Reads the authentication type from the combo box rather than from this object, since the widget
   * is what the user just changed and the metadata is only written back later.
   */
  private AwsSecretsManagerAuthType readAuthType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_AUTHENTICATION_TYPE);
    if (control instanceof Combo combo) {
      try {
        return AwsSecretsManagerAuthType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing picked yet, so fall through to what the metadata holds.
      }
    }
    try {
      return parseAuthType(authenticationType);
    } catch (HopException e) {
      return AwsSecretsManagerAuthType.AUTOMATIC;
    }
  }

  @Override
  public String getPluginId() {
    return "AwsSecretsManager";
  }

  @Override
  public String getPluginName() {
    return "AWS Secrets Manager Variable Resolver";
  }

  /** A secret value with the point in time after which it should be looked up again. */
  private static class CachedSecret {
    private final String value;
    private final long expiryTime;

    private CachedSecret(String value, long expiryTime) {
      this.value = value;
      this.expiryTime = expiryTime;
    }

    private boolean isExpired() {
      return System.currentTimeMillis() >= expiryTime;
    }
  }
}
