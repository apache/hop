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
package org.apache.hop.databases.redshift;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

@DatabaseMetaPlugin(
    type = "REDSHIFT",
    typeDescription = "Redshift",
    image = "redshift.svg",
    documentationUrl = "/database/databases/redshift.html",
    classLoaderGroup = "redshift-db")
@GuiPlugin(id = "GUI-RedshiftDatabaseMeta")
public class RedshiftDatabaseMeta extends PostgreSqlDatabaseMeta
    implements IGuiPluginCompositeWidgetsListener {

  /** The host name of every serverless workgroup ends in this. */
  public static final String SERVERLESS_HOST_SUFFIX = ".redshift-serverless.amazonaws.com";

  public static final String ID_DEPLOYMENT_TYPE = "deploymentType";
  public static final String ID_WORKGROUP = "workgroup";
  public static final String ID_ACCOUNT_ID = "accountId";
  public static final String ID_CLUSTER_ID = "clusterId";
  public static final String ID_AWS_REGION = "awsRegion";
  public static final String ID_AUTHENTICATION_TYPE = "authenticationType";
  public static final String ID_AWS_ACCESS_KEY_ID = "awsAccessKeyId";
  public static final String ID_AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";
  public static final String ID_AWS_SESSION_TOKEN = "awsSessionToken";
  public static final String ID_AWS_PROFILE = "awsProfile";
  public static final String ID_DB_USER = "dbUser";
  public static final String ID_DB_GROUPS = "dbGroups";
  public static final String ID_AUTO_CREATE = "autoCreate";

  // Driver options, spelled the way https://docs.aws.amazon.com/redshift/latest/mgmt/
  // jdbc20-configuration-options.html spells them. The driver itself is not case sensitive.
  private static final String PROPERTY_ACCESS_KEY_ID = "AccessKeyID";
  private static final String PROPERTY_SECRET_ACCESS_KEY = "SecretAccessKey";
  private static final String PROPERTY_SESSION_TOKEN = "SessionToken";
  private static final String PROPERTY_PROFILE = "Profile";
  private static final String PROPERTY_DB_USER = "DbUser";
  private static final String PROPERTY_DB_GROUPS = "DbGroups";
  private static final String PROPERTY_AUTO_CREATE = "AutoCreate";
  private static final String PROPERTY_REGION = "Region";
  private static final String PROPERTY_CLUSTER_ID = "ClusterID";
  private static final String PROPERTY_IS_SERVERLESS = "isServerless";
  private static final String PROPERTY_SERVERLESS_WORK_GROUP = "serverlessWorkGroup";
  private static final String PROPERTY_SERVERLESS_ACCT_ID = "serverlessAcctId";

  private static final List<IDatabaseTypeRule> TYPE_RULES =
      DatabaseTypes.rules()
          // Redshift is Postgres derived but has neither JSONB nor INET: semi structured data
          // goes in a SUPER, and an address is text like anything else.
          .write(IValueMeta.TYPE_JSON)
          .as("SUPER")
          .build();

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_DEPLOYMENT_TYPE,
      order = "04",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::RedshiftDatabaseMeta.label.DeploymentType",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.DeploymentType")
  @HopMetadataProperty(enumNameWhenNotFound = "PROVISIONED")
  private RedshiftDeploymentType deploymentType = RedshiftDeploymentType.PROVISIONED;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_WORKGROUP,
      order = "05",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.Workgroup",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.Workgroup")
  @HopMetadataProperty
  private String workgroup;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_ACCOUNT_ID,
      order = "06",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.AccountId",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AccountId")
  @HopMetadataProperty
  private String accountId;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_CLUSTER_ID,
      order = "07",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.ClusterId",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.ClusterId")
  @HopMetadataProperty
  private String clusterId;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AWS_REGION,
      order = "08",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.AwsRegion",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AwsRegion")
  @HopMetadataProperty
  private String awsRegion;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AUTHENTICATION_TYPE,
      order = "09",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::RedshiftDatabaseMeta.label.AuthenticationType",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AuthenticationType")
  @HopMetadataProperty(enumNameWhenNotFound = "DATABASE")
  private RedshiftAuthenticationType authenticationType = RedshiftAuthenticationType.DATABASE;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AWS_ACCESS_KEY_ID,
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.AwsAccessKeyId",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AwsAccessKeyId")
  @HopMetadataProperty
  private String awsAccessKeyId;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AWS_SECRET_ACCESS_KEY,
      order = "11",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::RedshiftDatabaseMeta.label.AwsSecretAccessKey",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AwsSecretAccessKey")
  @HopMetadataProperty(password = true)
  private String awsSecretAccessKey;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AWS_SESSION_TOKEN,
      order = "12",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::RedshiftDatabaseMeta.label.AwsSessionToken",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AwsSessionToken")
  @HopMetadataProperty(password = true)
  private String awsSessionToken;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AWS_PROFILE,
      order = "13",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.AwsProfile",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AwsProfile")
  @HopMetadataProperty
  private String awsProfile;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_DB_USER,
      order = "14",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.DbUser",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.DbUser")
  @HopMetadataProperty
  private String dbUser;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_DB_GROUPS,
      order = "15",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::RedshiftDatabaseMeta.label.DbGroups",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.DbGroups")
  @HopMetadataProperty
  private String dbGroups;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_AUTO_CREATE,
      order = "16",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::RedshiftDatabaseMeta.label.AutoCreate",
      toolTip = "i18n::RedshiftDatabaseMeta.toolTip.AutoCreate")
  @HopMetadataProperty
  private boolean autoCreate;

  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    return TYPE_RULES;
  }

  public RedshiftDatabaseMeta() {
    addExtraOption("REDSHIFT", "tcpKeepAlive", "true");
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 5439;
    }
    return -1;
  }

  @Override
  public String getDriverClass() {
    return "com.amazon.redshift.jdbc42.Driver";
  }

  @Override
  public DriverDownload getDriverDownload() {
    return DriverDownload.builder()
        .mavenCoordinate("com.amazon.redshift:redshift-jdbc42")
        .defaultVersion("2.2.8")
        .licenseCategory("A")
        .licenseName("Apache-2.0")
        .licenseUrl("https://github.com/aws/amazon-redshift-jdbc-driver/blob/master/LICENSE")
        .vendor("Amazon Web Services")
        .vendorUrl("https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-download-driver.html")
        .build();
  }

  /**
   * The driver only asks AWS for temporary credentials when the URL says {@code iam}, and it only
   * finds a serverless workgroup when the host name is the one AWS publishes for it. Both of those
   * are decisions the connection already carries, so neither has to be typed by hand.
   *
   * <p>Variables are deliberately left alone here. A {@link BaseDatabaseMeta} subclass has no
   * variables to resolve with, but {@code DatabaseMeta.getURL(IVariables)} resolves whatever this
   * returns, so a workgroup or a region can perfectly well be a variable.
   */
  @Override
  public String getURL(String hostname, String port, String databaseName) {
    String scheme = authenticationType.isIam() ? "jdbc:redshift:iam://" : "jdbc:redshift://";
    return scheme + serverHostname(hostname) + ":" + port + "/" + databaseName;
  }

  /**
   * The host name to connect to: the one that was entered, or the one a serverless workgroup is
   * guaranteed to have.
   *
   * @param hostname the host name on the connection, used for a provisioned cluster
   * @return the host name the URL should carry
   */
  private String serverHostname(String hostname) {
    // A serverless endpoint is built only when nothing was entered. Anyone sitting behind a custom
    // domain name or a load balancer enters that instead, and still names the workgroup below so
    // the driver knows which one it is talking to -- it can no longer tell from the host name.
    if (deploymentType == RedshiftDeploymentType.SERVERLESS && StringUtils.isEmpty(hostname)) {
      return buildServerlessHostname(workgroup, accountId, awsRegion);
    }
    return hostname;
  }

  /**
   * Build the endpoint AWS publishes for a serverless workgroup:
   *
   * <pre>&lt;workgroup&gt;.&lt;account-id&gt;.&lt;region&gt;.redshift-serverless.amazonaws.com
   * </pre>
   *
   * <p>Note this is the <em>workgroup</em> name, not the namespace name -- they are easy to mix up
   * and a namespace name gives a host that does not resolve.
   *
   * @param workgroup the workgroup name
   * @param accountId the 12 digit AWS account number
   * @param region the AWS region the workgroup runs in, for example eu-west-1
   * @return the endpoint host name
   */
  public static String buildServerlessHostname(String workgroup, String accountId, String region) {
    return Const.NVL(workgroup, "")
        + "."
        + Const.NVL(accountId, "")
        + "."
        + Const.NVL(region, "")
        + SERVERLESS_HOST_SUFFIX;
  }

  /**
   * Everything the driver needs beyond the URL. These deliberately do not go in the URL: the
   * secrets among them would end up in log lines and in the connection string shown on the dialog,
   * and AWS asks for them to be URL encoded there, which is a trap nobody needs.
   *
   * <p>Passing them as properties also makes them work for a manually entered URL, which is how
   * anyone with a custom domain name in front of their cluster has to connect.
   */
  @Override
  public Properties getConnectionProperties(IVariables variables) {
    Properties properties = new Properties();

    if (deploymentType == RedshiftDeploymentType.SERVERLESS) {
      // The driver works these out from the host name, but not when something like a load
      // balancer or a custom domain name sits in front of it. Saying so costs nothing.
      properties.put(PROPERTY_IS_SERVERLESS, "true");
      putIfFilled(properties, PROPERTY_SERVERLESS_WORK_GROUP, variables.resolve(workgroup));
      putIfFilled(properties, PROPERTY_SERVERLESS_ACCT_ID, variables.resolve(accountId));
    } else if (authenticationType.isIam()) {
      // The same problem on a cluster: the driver reads the cluster name out of the host name,
      // and a load balancer or a custom domain name leaves it nothing to read.
      putIfFilled(properties, PROPERTY_CLUSTER_ID, variables.resolve(clusterId));
    }
    // The region builds a serverless endpoint and tells IAM which AWS to ask. It says nothing to
    // a plain user and password connection, so a value left behind by another choice stays here.
    if (deploymentType == RedshiftDeploymentType.SERVERLESS || authenticationType.isIam()) {
      putIfFilled(properties, PROPERTY_REGION, variables.resolve(awsRegion));
    }

    if (!authenticationType.isIam()) {
      return properties;
    }

    switch (authenticationType) {
      case IAM_CREDENTIALS -> {
        putIfFilled(properties, PROPERTY_ACCESS_KEY_ID, variables.resolve(awsAccessKeyId));
        putIfFilled(properties, PROPERTY_SECRET_ACCESS_KEY, decrypt(variables, awsSecretAccessKey));
        putIfFilled(properties, PROPERTY_SESSION_TOKEN, decrypt(variables, awsSessionToken));
      }
      case IAM_PROFILE -> putIfFilled(properties, PROPERTY_PROFILE, variables.resolve(awsProfile));
      default -> {
        // IAM_DEFAULT_CHAIN: saying nothing is what makes the driver go and look for itself.
      }
    }

    // A serverless workgroup issues credentials for the IAM identity itself: its GetCredentials
    // call takes a database and a workgroup and nothing else. Naming a database user, its groups
    // or asking for it to be created are all GetClusterCredentials ideas, and only a provisioned
    // cluster has any use for them.
    if (deploymentType == RedshiftDeploymentType.SERVERLESS) {
      return properties;
    }

    // Which database user the temporary credentials are for. Falling back to the user name on the
    // connection means the obvious place to have typed it is also the right one.
    String user = StringUtils.isNotEmpty(dbUser) ? dbUser : getUsername();
    putIfFilled(properties, PROPERTY_DB_USER, variables.resolve(user));
    putIfFilled(properties, PROPERTY_DB_GROUPS, variables.resolve(dbGroups));
    if (autoCreate) {
      properties.put(PROPERTY_AUTO_CREATE, "true");
    }

    return properties;
  }

  /**
   * Add a driver option, trimmed. None of what goes through here -- a key, a region, a workgroup
   * name -- can meaningfully start or end in whitespace, and a stray space picked up while copying
   * an access key out of the AWS console otherwise comes back as an authentication failure with
   * nothing on screen to explain it.
   */
  private void putIfFilled(Properties properties, String name, String value) {
    if (StringUtils.isNotBlank(value)) {
      properties.put(name, value.trim());
    }
  }

  private String decrypt(IVariables variables, String password) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password));
  }

  @Override
  public String getExtraOptionsHelpText() {
    return "http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html";
  }

  /**
   * The superclass method checks whether or not the command setFetchSize() is supported by the
   * driver. In the case of Redshift, setFetchSize() is supported, but in the case of LIMIT, the
   * Redshift driver will enforce that the value for fetch size is less than or equal to the value
   * specified in the LIMIT clause.
   *
   * <p>To avoid these problems, this method (and supportsSetMaxRows()) returns false
   *
   * @return false
   */
  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  /**
   * Redshift does not recognize the JDBC "setMaxRows" parameter
   *
   * @return false
   */
  @Override
  public boolean isSupportsSetMaxRows() {
    return false;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do, the values aren't set yet at this point.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // Not needed, the dialog reads the widgets back itself.
  }

  /**
   * Keeps the dialog down to the fields that can actually do something for the connection being
   * edited. Between two deployments and four ways to authenticate, showing everything at once would
   * leave most of the tab permanently empty and the fields that matter hard to pick out.
   */
  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    RedshiftDeploymentType deployment = readDeploymentType(compositeWidgets);
    RedshiftAuthenticationType authentication = readAuthenticationType(compositeWidgets);
    boolean serverless = deployment == RedshiftDeploymentType.SERVERLESS;

    Set<String> hidden = new HashSet<>();
    // The host name stays on offer for a serverless workgroup: left empty Hop builds the endpoint,
    // filled in it wins, which is how a custom domain name or a load balancer is reached.
    hideUnless(hidden, serverless, ID_WORKGROUP, ID_ACCOUNT_ID);
    // A cluster only has to name itself when IAM has to find it.
    hideUnless(hidden, !serverless && authentication.isIam(), ID_CLUSTER_ID);
    // The region builds the host name, and IAM needs it to know which AWS to ask.
    hideUnless(hidden, serverless || authentication.isIam(), ID_AWS_REGION);

    hideUnless(
        hidden,
        authentication == RedshiftAuthenticationType.IAM_CREDENTIALS,
        ID_AWS_ACCESS_KEY_ID,
        ID_AWS_SECRET_ACCESS_KEY,
        ID_AWS_SESSION_TOKEN);
    hideUnless(hidden, authentication == RedshiftAuthenticationType.IAM_PROFILE, ID_AWS_PROFILE);
    // Only a provisioned cluster maps IAM onto a named database user.
    hideUnless(
        hidden, authentication.isIam() && !serverless, ID_DB_USER, ID_DB_GROUPS, ID_AUTO_CREATE);

    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  private void hideUnless(Set<String> hidden, boolean applies, String... widgetIds) {
    if (!applies) {
      hidden.addAll(List.of(widgetIds));
    }
  }

  private RedshiftDeploymentType readDeploymentType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_DEPLOYMENT_TYPE);
    if (control instanceof Combo combo) {
      try {
        return RedshiftDeploymentType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return getDeploymentType();
  }

  private RedshiftAuthenticationType readAuthenticationType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_AUTHENTICATION_TYPE);
    if (control instanceof Combo combo) {
      try {
        return RedshiftAuthenticationType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return getAuthenticationType();
  }
}
