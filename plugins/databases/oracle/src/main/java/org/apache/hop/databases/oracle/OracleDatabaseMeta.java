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

package org.apache.hop.databases.oracle;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.SqlScriptParser;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.StandardJdbcTypeMapper;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

/** Contains Oracle specific information through static final members */
@DatabaseMetaPlugin(
    type = "ORACLE",
    typeDescription = "Oracle",
    image = "oracle.svg",
    documentationUrl = "/database/databases/oracle.html",
    classLoaderGroup = "oracle-db")
@GuiPlugin(id = "GUI-OracleDatabaseMeta")
public class OracleDatabaseMeta extends BaseDatabaseMeta
    implements IDatabase, IGuiPluginCompositeWidgetsListener {

  /** RAW and LONG RAW arrive as variable length binary but are really strings. */
  private static final List<IDatabaseTypeRule> RAW_RULES =
      DatabaseTypes.rules()
          .read(Types.VARBINARY, Types.LONGVARBINARY)
          .where(
              (variables, databaseMeta, column) ->
                  !StandardJdbcTypeMapper.displaySizeIsTwiceThePrecision(databaseMeta, column))
          .as(IValueMeta.TYPE_STRING, DatabaseColumn::getDisplaySize, column -> -1)
          // A number with no usable size at all: the precision can be 38, too big for a double.
          .read(Types.DECIMAL, Types.NUMERIC, Types.DOUBLE, Types.FLOAT, Types.REAL)
          .where(
              (variables, databaseMeta, column) ->
                  StandardJdbcTypeMapper.numericScale(column) <= 0
                      && StandardJdbcTypeMapper.numericLength(column) <= 0)
          .as(IValueMeta.TYPE_BIGNUMBER, -1, -1)
          .build();

  private static final List<IDatabaseTypeRule> NUMBER_38_AS_INTEGER =
      precision38(IValueMeta.TYPE_INTEGER);
  private static final List<IDatabaseTypeRule> NUMBER_38_AS_BIGNUMBER =
      precision38(IValueMeta.TYPE_BIGNUMBER);

  private static List<IDatabaseTypeRule> precision38(int hopType) {
    return DatabaseTypes.rules()
        .read(Types.DECIMAL, Types.NUMERIC, Types.DOUBLE, Types.FLOAT, Types.REAL)
        .where(
            (variables, databaseMeta, column) ->
                StandardJdbcTypeMapper.numericScale(column) == 0
                    && StandardJdbcTypeMapper.numericLength(column) == 38)
        .as(hopType, 38, 0)
        .build();
  }

  /**
   * Oracle grew a JSON type in 21c. Against an older server the driver's type list says so and the
   * column becomes a CLOB, which is where JSON lived before the type existed.
   */
  private static final List<IDatabaseTypeRule> JSON_RULES =
      DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("JSON").build();

  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    // A 38 digit number is an integer unless this connection asked for the strict reading. That
    // option used to sit on the interface every dialect implements; it is Oracle's own.
    List<IDatabaseTypeRule> rules = new ArrayList<>(RAW_RULES.size() + JSON_RULES.size() + 1);
    rules.addAll(isStrictBigNumberInterpretation() ? NUMBER_38_AS_BIGNUMBER : NUMBER_38_AS_INTEGER);
    rules.addAll(RAW_RULES);
    rules.addAll(JSON_RULES);
    return rules;
  }

  /** Oracle 21c is the first with a JSON type; before it, JSON lived in a CLOB. */
  private static final int FIRST_VERSION_WITH_JSON = 21;

  @Override
  public boolean isColumnTypeAvailable(String columnType) {
    if ("JSON".equals(columnType)) {
      return serverIsAtLeast(FIRST_VERSION_WITH_JSON);
    }
    return true;
  }

  private static final String STRICT_BIGNUMBER_INTERPRETATION = "STRICT_NUMBER_38_INTERPRETATION";
  public static final String CONST_SELECT = "SELECT ";
  public static final String CONST_JDBC_ORACLE_THIN = "jdbc:oracle:thin:@";

  /** The port an Oracle listener conventionally uses for TCPS, as opposed to 1521 for plain TCP. */
  public static final int DEFAULT_TCPS_PORT = 2484;

  // JDBC property names, see the Oracle JDBC developer's guide.
  //
  public static final String PROPERTY_TNS_ADMIN = "oracle.net.tns_admin";
  public static final String PROPERTY_WALLET_LOCATION = "oracle.net.wallet_location";
  public static final String PROPERTY_WALLET_PASSWORD = "oracle.net.wallet_password";
  public static final String PROPERTY_SSL_SERVER_DN_MATCH = "oracle.net.ssl_server_dn_match";
  public static final String PROPERTY_SSL_SERVER_CERT_DN = "oracle.net.ssl_server_cert_dn";
  public static final String PROPERTY_TRUST_STORE = "javax.net.ssl.trustStore";
  public static final String PROPERTY_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String PROPERTY_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
  public static final String PROPERTY_KEY_STORE = "javax.net.ssl.keyStore";
  public static final String PROPERTY_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  public static final String PROPERTY_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";

  // Widget ids, kept as constants because the show/hide logic below looks them up.
  //
  public static final String ID_HOSTNAME = BaseDatabaseMeta.ELEMENT_ID_HOSTNAME;
  public static final String ID_PORT = BaseDatabaseMeta.ELEMENT_ID_PORT;
  public static final String ID_CONNECTION_TYPE = "connectionType";
  public static final String ID_TNS_ADMIN = "tnsAdmin";
  public static final String ID_USE_TCPS = "useTcps";
  public static final String ID_TLS_CREDENTIAL_TYPE = "tlsCredentialType";
  public static final String ID_WALLET_DIRECTORY = "walletDirectory";
  public static final String ID_WALLET_PASSWORD = "walletPassword";
  public static final String ID_TRUST_STORE_FILE = "trustStoreFile";
  public static final String ID_TRUST_STORE_PASSWORD = "trustStorePassword";
  public static final String ID_TRUST_STORE_TYPE = "trustStoreType";
  public static final String ID_KEY_STORE_FILE = "keyStoreFile";
  public static final String ID_KEY_STORE_PASSWORD = "keyStorePassword";
  public static final String ID_KEY_STORE_TYPE = "keyStoreType";
  public static final String ID_SSL_SERVER_DN_MATCH = "sslServerDnMatch";
  public static final String ID_SSL_SERVER_CERT_DN = "sslServerCertDn";

  @GuiWidgetElement(
      id = ID_CONNECTION_TYPE,
      order = "04",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::OracleDatabaseMeta.label.ConnectionType",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.ConnectionType")
  @HopMetadataProperty(enumNameWhenNotFound = "AUTOMATIC")
  private OracleConnectionType connectionType = OracleConnectionType.AUTOMATIC;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_TNS_ADMIN,
      order = "05",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::OracleDatabaseMeta.label.TnsAdmin",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.TnsAdmin")
  @HopMetadataProperty
  private String tnsAdmin;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_USE_TCPS,
      order = "06",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OracleDatabaseMeta.label.UseTcps",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.UseTcps")
  @HopMetadataProperty
  private boolean useTcps;

  @GuiWidgetElement(
      id = ID_TLS_CREDENTIAL_TYPE,
      order = "07",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::OracleDatabaseMeta.label.TlsCredentialType",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.TlsCredentialType")
  @HopMetadataProperty(enumNameWhenNotFound = "NONE")
  private OracleTlsCredentialType tlsCredentialType = OracleTlsCredentialType.NONE;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_WALLET_DIRECTORY,
      order = "08",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::OracleDatabaseMeta.label.WalletDirectory",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.WalletDirectory")
  @HopMetadataProperty
  private String walletDirectory;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_WALLET_PASSWORD,
      order = "09",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::OracleDatabaseMeta.label.WalletPassword",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.WalletPassword")
  @HopMetadataProperty(password = true)
  private String walletPassword;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_TRUST_STORE_FILE,
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::OracleDatabaseMeta.label.TrustStoreFile",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.TrustStoreFile")
  @HopMetadataProperty
  private String trustStoreFile;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_TRUST_STORE_PASSWORD,
      order = "11",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::OracleDatabaseMeta.label.TrustStorePassword")
  @HopMetadataProperty(password = true)
  private String trustStorePassword;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_TRUST_STORE_TYPE,
      order = "12",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OracleDatabaseMeta.label.TrustStoreType",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.StoreType")
  @HopMetadataProperty
  private String trustStoreType;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_KEY_STORE_FILE,
      order = "13",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::OracleDatabaseMeta.label.KeyStoreFile",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.KeyStoreFile")
  @HopMetadataProperty
  private String keyStoreFile;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_KEY_STORE_PASSWORD,
      order = "14",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::OracleDatabaseMeta.label.KeyStorePassword")
  @HopMetadataProperty(password = true)
  private String keyStorePassword;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_KEY_STORE_TYPE,
      order = "15",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OracleDatabaseMeta.label.KeyStoreType",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.StoreType")
  @HopMetadataProperty
  private String keyStoreType;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_SSL_SERVER_DN_MATCH,
      order = "16",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::OracleDatabaseMeta.label.SslServerDnMatch",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.SslServerDnMatch")
  @HopMetadataProperty
  private boolean sslServerDnMatch = true;

  @Getter
  @Setter
  @GuiWidgetElement(
      id = ID_SSL_SERVER_CERT_DN,
      order = "17",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::OracleDatabaseMeta.label.SslServerCertDn",
      toolTip = "i18n::OracleDatabaseMeta.toolTip.SslServerCertDn")
  @HopMetadataProperty
  private String sslServerCertDn;

  /**
   * Connections saved before these options existed have no value stored, which deserializes to
   * null. Both accessors fall back to the default so that such a connection keeps behaving exactly
   * the way it did -- and so the dialog never shows an empty combo, which it cannot read back.
   *
   * @return the connection type, never null
   */
  public OracleConnectionType getConnectionType() {
    return connectionType == null ? OracleConnectionType.AUTOMATIC : connectionType;
  }

  public void setConnectionType(OracleConnectionType connectionType) {
    this.connectionType = connectionType == null ? OracleConnectionType.AUTOMATIC : connectionType;
  }

  /**
   * @return the TLS credential type, never null
   */
  public OracleTlsCredentialType getTlsCredentialType() {
    return tlsCredentialType == null ? OracleTlsCredentialType.NONE : tlsCredentialType;
  }

  public void setTlsCredentialType(OracleTlsCredentialType tlsCredentialType) {
    this.tlsCredentialType =
        tlsCredentialType == null ? OracleTlsCredentialType.NONE : tlsCredentialType;
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 1521;
    }
    return -1;
  }

  /**
   * @return Whether or not the database can use auto increment type of fields (pk)
   */
  @Override
  public boolean isSupportsAutoInc() {
    return false;
  }

  /**
   * @see IDatabase#getLimitClause(int)
   */
  @Override
  public String getLimitClause(int nrRows) {
    // Minimum version Oracle 12c Release 1 (released in 2013)
    return " FETCH FIRST " + nrRows + " ROWS ONLY";
  }

  /**
   * Returns the minimal SQL to launch in order to determine the layout of the resultset for a given
   * database table
   *
   * @param tableName The name of the table to determine the layout for
   * @return The SQL to launch.
   */
  @Override
  public String getSqlQueryFields(String tableName) {
    return "SELECT * FROM " + tableName + " WHERE 1=0";
  }

  @Override
  public String getSqlViewDefinition(String schemaName, String viewName) {
    if (Utils.isEmpty(viewName)) {
      return null;
    }
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT TEXT FROM ALL_VIEWS WHERE VIEW_NAME = ").append(quoteSqlString(viewName));
    if (!Utils.isEmpty(schemaName)) {
      sql.append(" AND OWNER = ").append(quoteSqlString(schemaName));
    }
    return sql.toString();
  }

  @Override
  public String getSqlTableExists(String tableName) {
    return getSqlQueryFields(tableName);
  }

  @Override
  public String getSqlColumnExists(String columnName, String tableName) {
    return getSqlQueryColumnFields(columnName, tableName);
  }

  public String getSqlQueryColumnFields(String columnName, String tableName) {
    return CONST_SELECT + columnName + " FROM " + tableName + " WHERE 1=0";
  }

  @Override
  public String getDriverClass() {
    return "oracle.jdbc.driver.OracleDriver";
  }

  @Override
  public DriverDownload getDriverDownload() {
    return DriverDownload.builder()
        .mavenCoordinate("com.oracle.database.jdbc:ojdbc11")
        // The SSO keystore type an Oracle Wallet needs lives in oraclepki, not in ojdbc, and
        // ojdbc declares no dependencies at all - so without asking for it by name a cwallet.sso
        // connection fails with "SSO KeyStore not available". Verified: oraclepki carries the
        // oracle.security.pki classes, both ojdbc11 and ojdbc17 carry none.
        //
        .companionCoordinates(List.of("com.oracle.database.security:oraclepki"))
        .defaultVersion("23.26.2.0.0")
        .licenseCategory("X")
        .licenseName("Oracle Free Use Terms and Conditions (OTN)")
        .licenseUrl("https://www.oracle.com/downloads/licenses/oracle-free-license.html")
        .vendor("Oracle")
        .vendorUrl("https://www.oracle.com/database/technologies/appdev/jdbc.html")
        .build();
  }

  @Override
  public String getURL(String hostname, String port, String databaseName)
      throws HopDatabaseException {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return getThinUrl(hostname, port, databaseName);
    } else {
      // OCI
      // Let's see if we have an database name
      if (!Utils.isEmpty(databaseName)) {
        // Has the user specified hostname & port number?
        if (!Utils.isEmpty(hostname) && !Utils.isEmpty(port)) {
          // User wants the full url
          return "jdbc:oracle:oci:@(description=(address=(host="
              + hostname
              + ")(protocol=tcp)(port="
              + port
              + "))(connect_data=(sid="
              + databaseName
              + ")))";
        } else {
          // User wants the shortcut url
          return "jdbc:oracle:oci:@" + databaseName;
        }
      } else {
        throw new HopDatabaseException(
            "Unable to construct a JDBC URL: at least the database name must be specified");
      }
    }
  }

  /** Builds the thin driver URL for the selected {@link OracleConnectionType}. */
  private String getThinUrl(String hostname, String port, String databaseName) {
    OracleConnectionType type = getConnectionType();

    // A TNS alias resolves through tnsnames.ora and a hand written descriptor is complete as it
    // stands: in both cases the protocol, host and port come from what the user provides, so there
    // is nothing for us to build and TCPS is not ours to switch on either.
    //
    if (type == OracleConnectionType.TNS_ALIAS || type == OracleConnectionType.DESCRIPTOR) {
      return CONST_JDBC_ORACLE_THIN + Const.NVL(databaseName, "");
    }

    // AUTOMATIC reproduces the historical behaviour exactly, including the order the three cases
    // were tested in: a / or : prefix wins over the "no host and port means a descriptor" case.
    // Reversing those two changes the URL for oddities like ("", "", "/SERVICE"), which are broken
    // either way but were not ours to alter.
    //
    if (type == OracleConnectionType.AUTOMATIC) {
      boolean prefixed =
          !Utils.isEmpty(databaseName)
              && (databaseName.startsWith("/") || databaseName.startsWith(":"));
      // -1 shows up as the port for file based connections.
      boolean noHostAndPort =
          Utils.isEmpty(hostname) && (Utils.isEmpty(port) || "-1".equals(port.trim()));

      // The database name already holds a full descriptor - a RAC address list, a failover
      // descriptor - and carries its own protocol, so TCPS is not ours to add either.
      //
      if (!prefixed && noHostAndPort) {
        return CONST_JDBC_ORACLE_THIN + Const.NVL(databaseName, "");
      }
      if (isUseTcps()) {
        return getTcpsDescriptorUrl(hostname, port, databaseName, type);
      }
      return prefixed
          ? CONST_JDBC_ORACLE_THIN + hostname + ":" + port + databaseName
          : CONST_JDBC_ORACLE_THIN + hostname + ":" + port + ":" + databaseName;
    }

    if (isUseTcps()) {
      return getTcpsDescriptorUrl(hostname, port, databaseName, type);
    }

    return type == OracleConnectionType.SERVICE_NAME
        ? CONST_JDBC_ORACLE_THIN
            + "//"
            + Const.NVL(hostname, "")
            + ":"
            + Const.NVL(port, "")
            + "/"
            + stripPrefix(databaseName)
        : CONST_JDBC_ORACLE_THIN
            + Const.NVL(hostname, "")
            + ":"
            + Const.NVL(port, "")
            + ":"
            + stripPrefix(databaseName);
  }

  /**
   * The short {@code host:port:sid} forms cannot express a protocol, so a TLS connection always
   * needs the long descriptor.
   */
  private String getTcpsDescriptorUrl(
      String hostname, String port, String databaseName, OracleConnectionType type) {

    // AUTOMATIC keeps the same reading of the database name as the plain TCP path above: a leading
    // / means a service name, anything else is treated as a SID.
    //
    boolean sid =
        switch (type) {
          case SERVICE_NAME -> false;
          case SID -> true;
          default -> databaseName == null || !databaseName.startsWith("/");
        };

    return CONST_JDBC_ORACLE_THIN
        + "(description=(address=(protocol=tcps)(host="
        + Const.NVL(hostname, "")
        + ")(port="
        + (Utils.isEmpty(port) || "-1".equals(port.trim())
            ? Integer.toString(DEFAULT_TCPS_PORT)
            : port)
        + "))(connect_data=("
        + (sid ? "sid=" : "service_name=")
        + stripPrefix(databaseName)
        + ")))";
  }

  /**
   * Drops the leading {@code :} or {@code /} that marks a SID or a service name in AUTOMATIC mode.
   * Once the type is chosen explicitly the marker is redundant, and leaving it in would produce a
   * double separator.
   */
  private String stripPrefix(String databaseName) {
    if (Utils.isEmpty(databaseName)) {
      return Const.NVL(databaseName, "");
    }
    if (databaseName.startsWith("/") || databaseName.startsWith(":")) {
      return databaseName.substring(1);
    }
    return databaseName;
  }

  /**
   * Oracle doesn't support options in the URL, we need to put these in a Properties object at
   * connection time...
   */
  @Override
  public boolean isSupportsOptionsInURL() {
    return false;
  }

  /**
   * Turns the TLS and wallet settings into the JDBC properties the Oracle driver reads.
   *
   * <p>These deliberately do not end up in the URL. Only one of the URL branches builds a
   * descriptor we could have injected them into, so putting them here is what makes them work for a
   * TNS alias, for the short {@code host:port:sid} forms, and above all for a manually entered URL
   * -- which is the most common way to connect to an Autonomous Database.
   */
  @Override
  public Properties getConnectionProperties(IVariables variables) {
    Properties properties = new Properties();

    // Useful with or without TLS: it is also how a plain tnsnames.ora gets found.
    //
    putIfFilled(properties, PROPERTY_TNS_ADMIN, variables.resolve(tnsAdmin));

    // Not gated on the TCPS checkbox alone. A TNS alias or a hand written descriptor can perfectly
    // well name protocol=tcps itself, and its certificates would then never reach the driver.
    // Credentials being configured at all is the better signal that TLS is in play.
    //
    if (!isUseTcps() && getTlsCredentialType() == OracleTlsCredentialType.NONE) {
      return properties;
    }

    // Verifying that the certificate belongs to the server we asked for is what makes TLS worth
    // anything here, so it is on by default and only ever off because someone said so.
    //
    properties.put(PROPERTY_SSL_SERVER_DN_MATCH, Boolean.toString(isSslServerDnMatch()));
    putIfFilled(properties, PROPERTY_SSL_SERVER_CERT_DN, variables.resolve(sslServerCertDn));

    switch (getTlsCredentialType()) {
      case WALLET -> {
        String directory = variables.resolve(walletDirectory);
        if (StringUtils.isNotEmpty(directory)) {
          properties.put(
              PROPERTY_WALLET_LOCATION,
              "(SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=" + directory + ")))");
        }
        putIfFilled(properties, PROPERTY_WALLET_PASSWORD, decrypt(variables, walletPassword));
      }
      case JKS -> {
        putIfFilled(properties, PROPERTY_TRUST_STORE, variables.resolve(trustStoreFile));
        putIfFilled(
            properties, PROPERTY_TRUST_STORE_PASSWORD, decrypt(variables, trustStorePassword));
        putIfFilled(properties, PROPERTY_TRUST_STORE_TYPE, variables.resolve(trustStoreType));
        putIfFilled(properties, PROPERTY_KEY_STORE, variables.resolve(keyStoreFile));
        putIfFilled(properties, PROPERTY_KEY_STORE_PASSWORD, decrypt(variables, keyStorePassword));
        putIfFilled(properties, PROPERTY_KEY_STORE_TYPE, variables.resolve(keyStoreType));
      }
      default -> {
        // NONE: one way TLS against the JVM's default trust store.
      }
    }

    return properties;
  }

  private void putIfFilled(Properties properties, String name, String value) {
    if (StringUtils.isNotEmpty(value)) {
      properties.put(name, value);
    }
  }

  private String decrypt(IVariables variables, String password) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password));
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
   * edited. Most Oracle connections are a plain host and port, and a wall of TLS and wallet fields
   * that will only ever stay empty makes the ones that matter harder to find.
   */
  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    OracleConnectionType type = readConnectionType(compositeWidgets);

    // A TNS alias and a hand written descriptor say where and how to connect all by themselves, so
    // neither the address nor the TCPS checkbox has anything left to decide. Their certificates
    // are still ours to pass on, which is why the credentials below stay put.
    //
    boolean addressApplies =
        type != OracleConnectionType.TNS_ALIAS && type != OracleConnectionType.DESCRIPTOR;
    boolean tlsApplies =
        !addressApplies || readCheckbox(compositeWidgets, ID_USE_TCPS, isUseTcps());
    OracleTlsCredentialType credentials = readCredentialType(compositeWidgets);

    Set<String> hidden = new HashSet<>();
    hideUnless(hidden, addressApplies, ID_HOSTNAME, ID_PORT, ID_USE_TCPS);
    hideUnless(hidden, tlsApplies, ID_TLS_CREDENTIAL_TYPE);
    hideUnless(hidden, tlsApplies, ID_SSL_SERVER_DN_MATCH, ID_SSL_SERVER_CERT_DN);
    hideUnless(
        hidden,
        tlsApplies && credentials == OracleTlsCredentialType.WALLET,
        ID_WALLET_DIRECTORY,
        ID_WALLET_PASSWORD);
    hideUnless(
        hidden,
        tlsApplies && credentials == OracleTlsCredentialType.JKS,
        ID_TRUST_STORE_FILE,
        ID_TRUST_STORE_PASSWORD,
        ID_TRUST_STORE_TYPE,
        ID_KEY_STORE_FILE,
        ID_KEY_STORE_PASSWORD,
        ID_KEY_STORE_TYPE);

    compositeWidgets.setWidgetsHidden(this, hidden);
  }

  private void hideUnless(Set<String> hidden, boolean applies, String... widgetIds) {
    if (!applies) {
      hidden.addAll(List.of(widgetIds));
    }
  }

  private OracleConnectionType readConnectionType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_CONNECTION_TYPE);
    if (control instanceof Combo combo) {
      try {
        return OracleConnectionType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return getConnectionType();
  }

  /**
   * Widgets can be switched off through disabledGuiElements.xml, in which case there is nothing to
   * read the setting from and we fall back to what is on the metadata itself.
   */
  private boolean readCheckbox(
      GuiCompositeWidgets compositeWidgets, String widgetId, boolean fallback) {
    Control control = compositeWidgets.getWidgetsMap().get(widgetId);
    return control instanceof Button button ? button.getSelection() : fallback;
  }

  private OracleTlsCredentialType readCredentialType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(ID_TLS_CREDENTIAL_TYPE);
    if (control instanceof Combo combo) {
      try {
        return OracleTlsCredentialType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return getTlsCredentialType();
  }

  /**
   * @return true if the database supports sequences
   */
  @Override
  public boolean isSupportsSequences() {
    return true;
  }

  /**
   * Check if a sequence exists.
   *
   * @param sequenceName The sequence to check
   * @return The SQL to get the name of the sequence back from the databases data dictionary
   */
  @Override
  public String getSqlSequenceExists(String sequenceName) {
    int dotPos = sequenceName.indexOf('.');
    String sql = "";
    if (dotPos == -1) {
      // if schema is not specified try to get sequence which belongs to current user
      sql =
          "SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = '" + sequenceName.toUpperCase() + "'";
    } else {
      String schemaName = sequenceName.substring(0, dotPos);
      String seqName = sequenceName.substring(dotPos + 1);
      sql =
          "SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = '"
              + seqName.toUpperCase()
              + "' AND SEQUENCE_OWNER = '"
              + schemaName.toUpperCase()
              + "'";
    }
    return sql;
  }

  /**
   * Get the current value of a database sequence
   *
   * @param sequenceName The sequence to check
   * @return The current value of a database sequence
   */
  @Override
  public String getSqlCurrentSequenceValue(String sequenceName) {
    return CONST_SELECT + sequenceName + ".currval FROM DUAL";
  }

  /**
   * Get the SQL to get the next value of a sequence. (Oracle only)
   *
   * @param sequenceName The sequence name
   * @return the SQL to get the next value of a sequence. (Oracle only)
   */
  @Override
  public String getSqlNextSequenceValue(String sequenceName) {
    return CONST_SELECT + sequenceName + ".nextval FROM DUAL";
  }

  @Override
  public boolean isSupportsSequenceNoMaxValueOption() {
    return true;
  }

  /**
   * @return true if we need to supply the schema-name to getTables in order to get a correct list
   *     of items.
   */
  @Override
  public boolean useSchemaNameForTableList() {
    return true;
  }

  /**
   * @return true if the database supports synonyms
   */
  @Override
  public boolean isSupportsSynonyms() {
    return true;
  }

  /**
   * Generates the SQL statement to add a column to the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD ( "
        + getColumnDefinition(v, tk, pk, useAutoinc, true, false, ColumnContext.Purpose.ADD_COLUMN)
        + " ) ";
  }

  /**
   * Generates the SQL statement to drop a column from the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to drop a column from the specified table
   */
  @Override
  public String getDropColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    return "ALTER TABLE " + tableName + " DROP ( " + v.getName() + " ) " + Const.CR;
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoinc whether or not this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether or not to add a semi-colon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement(
      String tableName, IValueMeta v, String tk, boolean useAutoinc, String pk, boolean semicolon) {
    IValueMeta tmpColumn = v.clone();
    String tmpName = v.getName();
    boolean isQuoted = tmpName.startsWith("\"") && tmpName.endsWith("\"");
    if (isQuoted) {
      // remove the quotes first.
      //
      tmpName = tmpName.substring(1, tmpName.length() - 1);
    }

    int threeoh = tmpName.length() >= 30 ? 30 : tmpName.length();
    tmpName = tmpName.substring(0, threeoh);

    tmpName += "_KTL"; // should always be shorter than 35 positions

    // put the quotes back if needed.
    //
    if (isQuoted) {
      tmpName = "\"" + tmpName + "\"";
    }
    tmpColumn.setName(tmpName);

    // Read to go.
    //
    String sql = "";

    // Create a new tmp column
    sql +=
        getAddColumnStatement(tableName, tmpColumn, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
    // copy the old data over to the tmp column
    sql +=
        "UPDATE " + tableName + " SET " + tmpColumn.getName() + "=" + v.getName() + ";" + Const.CR;
    // drop the old column
    sql += getDropColumnStatement(tableName, v, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
    // create the wanted column
    sql += getAddColumnStatement(tableName, v, tk, useAutoinc, pk, semicolon) + ";" + Const.CR;
    // copy the data from the tmp column to the wanted column (again)
    // All this to avoid the rename clause as this is not supported on all Oracle versions
    sql +=
        "UPDATE " + tableName + " SET " + v.getName() + "=" + tmpColumn.getName() + ";" + Const.CR;
    // drop the temp column
    sql += getDropColumnStatement(tableName, tmpColumn, tk, useAutoinc, pk, semicolon);

    return sql;
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCR) {
    StringBuilder retval = new StringBuilder(128);

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval.append(fieldname).append(' ');
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP:
        if (isSupportsTimestampDataType()) {
          retval.append("TIMESTAMP");
        } else {
          retval.append("DATE");
        }
        break;
      case IValueMeta.TYPE_DATE:
        retval.append("DATE");
        break;
      case IValueMeta.TYPE_BOOLEAN:
        retval.append("CHAR(1)");
        break;
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
        retval.append("NUMBER");
        if (length > 0) {
          retval.append('(').append(length);
          if (precision > 0) {
            retval.append(", ").append(precision);
          }
          retval.append(')');
        }
        break;
      case IValueMeta.TYPE_INTEGER:
        retval.append("INTEGER");
        break;
      case IValueMeta.TYPE_STRING:
        if (length >= DatabaseMeta.CLOB_LENGTH) {
          retval.append("CLOB");
        } else {
          if (length == 1) {
            retval.append("CHAR(1)");
          } else if (length > 0 && length <= getMaxVARCHARLength()) {
            retval.append("VARCHAR2(").append(length).append(')');
          } else {
            if (length <= 0) {
              retval.append("VARCHAR2(2000)"); // We don't know, so we just use the maximum...
            } else {
              retval.append("CLOB");
            }
          }
        }
        break;
      case IValueMeta.TYPE_BINARY: // the BLOB can contain binary data.
        retval.append("BLOB");
        break;
      default:
        retval.append(" UNKNOWN");
        break;
    }

    if (addCR) {
      retval.append(Const.CR);
    }

    return retval.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.ibridge.hop.core.database.IDatabase#getReservedWords()
   */
  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ACCESS",
      "ADD",
      "ALL",
      "ALTER",
      "AND",
      "ANY",
      "ARRAYLEN",
      "AS",
      "ASC",
      "AUDIT",
      "BETWEEN",
      "BY",
      "CHAR",
      "CHECK",
      "CLUSTER",
      "COLUMN",
      "COMMENT",
      "COMPRESS",
      "CONNECT",
      "CREATE",
      "CURRENT",
      "DATE",
      "DECIMAL",
      "DEFAULT",
      "DELETE",
      "DESC",
      "DISTINCT",
      "DROP",
      "ELSE",
      "EXCLUSIVE",
      "EXISTS",
      "FILE",
      "FLOAT",
      "FOR",
      "FROM",
      "GRANT",
      "GROUP",
      "HAVING",
      "IDENTIFIED",
      "IMMEDIATE",
      "IN",
      "INCREMENT",
      "INDEX",
      "INITIAL",
      "INSERT",
      "INTEGER",
      "INTERSECT",
      "INTO",
      "IS",
      "LEVEL",
      "LIKE",
      "LOCK",
      "LONG",
      "MAXEXTENTS",
      "MINUS",
      "MODE",
      "MODIFY",
      "NOAUDIT",
      "NOCOMPRESS",
      "NOT",
      "NOTFOUND",
      "NOWAIT",
      "NULL",
      "NUMBER",
      "OF",
      "OFFLINE",
      "ON",
      "ONLINE",
      "OPTION",
      "OR",
      "ORDER",
      "PCTFREE",
      "PRIOR",
      "PRIVILEGES",
      "PUBLIC",
      "RAW",
      "RENAME",
      "RESOURCE",
      "REVOKE",
      "ROW",
      "ROWID",
      "ROWLABEL",
      "ROWNUM",
      "ROWS",
      "SELECT",
      "SESSION",
      "SET",
      "SHARE",
      "SIZE",
      "SMALLINT",
      "SQLBUF",
      "START",
      "SUCCESSFUL",
      "SYNONYM",
      "SYSDATE",
      "TABLE",
      "THEN",
      "TO",
      "TRIGGER",
      "UID",
      "UNION",
      "UNIQUE",
      "UPDATE",
      "USER",
      "VALIDATE",
      "VALUES",
      "VARCHAR",
      "VARCHAR2",
      "VIEW",
      "WHENEVER",
      "WHERE",
      "WITH"
    };
  }

  /**
   * @return The SQL on this database to get a list of stored procedures.
   */
  @Override
  public String getSqlListOfProcedures() {
    return "SELECT OWNER||'.'||OBJECT_NAME||NVL2(PROCEDURE_NAME,'.'||PROCEDURE_NAME,NULL) FROM ALL_PROCEDURES ORDER BY 1";
  }

  @Override
  public String getSqlLockTables(String[] tableNames) {
    StringBuilder sql = new StringBuilder(128);
    for (String tableName : tableNames) {
      sql.append("LOCK TABLE ").append(tableName).append(" IN EXCLUSIVE MODE;").append(Const.CR);
    }
    return sql.toString();
  }

  @Override
  public String getSqlUnlockTables(String[] tableNames) {
    return null; // commit handles the unlocking!
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  @Override
  public String getExtraOptionsHelpText() {
    return "http://download.oracle.com/docs/cd/B19306_01/java.102/b14355/urls.htm#i1006362";
  }

  /**
   * Verifies on the specified database connection if an index exists on the fields with the
   * specified name.
   *
   * @param database a connected database
   * @param schemaName
   * @param tableName
   * @param idxFields
   * @return true if the index exists, false if it doesn't.
   * @throws HopDatabaseException
   */
  @Override
  public boolean hasIndex(
      Database database, String schemaName, String tableName, String[] idxFields)
      throws HopDatabaseException {

    String schemaTable =
        database.getDatabaseMeta().getQuotedSchemaTableCombination(database, schemaName, tableName);

    boolean[] exists = new boolean[idxFields.length];
    for (int i = 0; i < exists.length; i++) {
      exists[i] = false;
    }

    try {
      //
      // Get the info from the data dictionary...
      //
      String sql = "SELECT * FROM USER_IND_COLUMNS WHERE TABLE_NAME = '" + schemaTable + "'";
      ResultSet res = null;
      try {
        res = database.openQuery(sql);
        if (res != null) {
          Object[] row = database.getRow(res);
          while (row != null) {
            String column = database.getReturnRowMeta().getString(row, "COLUMN_NAME", "");
            int idx = Const.indexOfString(column, idxFields);
            if (idx >= 0) {
              exists[idx] = true;
            }

            row = database.getRow(res);
          }

        } else {
          return false;
        }
      } finally {
        if (res != null) {
          database.closeQuery(res);
        }
      }

      // See if all the fields are indexed...
      boolean all = true;
      for (int i = 0; i < exists.length && all; i++) {
        if (!exists[i]) {
          all = false;
          break;
        }
      }

      return all;
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Unable to determine if indexes exists on table [" + schemaTable + "]", e);
    }
  }

  @Override
  public boolean isRequiresCreateTablePrimaryKeyAppend() {
    return true;
  }

  /**
   * Most databases allow you to retrieve result metadata by preparing a SELECT statement.
   *
   * @return true if the database supports retrieval of query metadata from a prepared statement.
   *     False if the query needs to be executed first.
   */
  @Override
  public boolean isSupportsPreparedStatementMetadataRetrieval() {
    return true;
  }

  /**
   * @return The maximum number of columns in a database, {@literal <=0} means: no known limit
   */
  @Override
  public int getMaxColumnsInIndex() {
    return 32;
  }

  /**
   * @return The SQL on this database to get a list of sequences.
   */
  @Override
  public String getSqlListOfSequences() {
    return "SELECT SEQUENCE_NAME FROM all_sequences";
  }

  /**
   * @param string
   * @return A string that is properly quoted for use in an Oracle SQL statement (insert, update,
   *     delete, etc)
   */
  @Override
  public String quoteSqlString(String string) {
    string = string.replaceAll("'", "''");
    string = string.replaceAll("\\n", "'||chr(13)||'");
    string = string.replaceAll("\\r", "'||chr(10)||'");
    return "'" + string + "'";
  }

  /** Returns a false as Oracle does not allow for the releasing of savepoints. */
  @Override
  public boolean isReleaseSavepoint() {
    return false;
  }

  /**
   * Returns an empty string as most databases do not support tablespaces. Subclasses can override
   * this method to generate the DDL.
   *
   * @param variables variables needed for variable substitution.
   * @param databaseMeta databaseMeta needed for it's quoteField method. Since we are doing variable
   *     substitution we need to meta so that we can act on the variable substitution first and then
   *     the creation of the entire string that will be retuned.
   * @param tablespace tablespaceName name of the tablespace.
   * @return String the TABLESPACE tablespaceName section of an Oracle CREATE DDL statement.
   */
  @Override
  public String getTablespaceDDL(
      IVariables variables, DatabaseMeta databaseMeta, String tablespace) {
    if (!Utils.isEmpty(tablespace)) {
      return "TABLESPACE " + databaseMeta.quoteField(variables.resolve(tablespace));
    } else {
      return "";
    }
  }

  @Override
  public boolean IsSupportsErrorHandlingOnBatchUpdates() {
    return false;
  }

  @Override
  public int getMaxVARCHARLength() {
    return 2000;
  }

  /**
   * Oracle does not support a construct like 'drop table if exists', which is apparently legal
   * syntax in many other RDBMSs. So we need to implement the same behavior and avoid throwing
   * 'table does not exist' exception.
   *
   * @param tableName Name of the table to drop
   * @return 'drop table if exists'-like statement for Oracle
   */
  @Override
  public String getDropTableIfExistsStatement(String tableName) {
    return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        + tableName
        + "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
  }

  @Override
  public SqlScriptParser createSqlScriptParser() {
    return new SqlScriptParser(false);
  }

  /**
   * @return true if using strict number(38) interpretation
   */
  @Override
  public boolean isStrictBigNumberInterpretation() {
    return "Y".equalsIgnoreCase(getAttributeProperty(STRICT_BIGNUMBER_INTERPRETATION, "N"));
  }

  /**
   * @param strictBigNumberInterpretation true if use strict number(38) interpretation
   */
  public void setStrictBigNumberInterpretation(boolean strictBigNumberInterpretation) {
    getAttributes().put(STRICT_BIGNUMBER_INTERPRETATION, strictBigNumberInterpretation ? "Y" : "N");
  }

  @Override
  public boolean isOracleVariant() {
    return true;
  }

  @Override
  public String getStartQuote() {
    return "\"";
  }

  @Override
  public String getEndQuote() {
    return "\"";
  }

  @Override
  public void addDefaultOptions() {
    setSupportsBooleanDataType(true);
    setSupportsTimestampDataType(true);
  }
}
