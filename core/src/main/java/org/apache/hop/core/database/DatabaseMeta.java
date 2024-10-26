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

package org.apache.hop.core.database;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * This class defines the database specific parameters for a certain database type. It also provides
 * static information regarding a number of well known databases.
 */
@HopMetadata(
    key = "rdbms",
    name = "i18n::DatabaseMeta.name",
    description = "i18n::DatabaseMeta.Description",
    image = "ui/images/database.svg",
    documentationUrl = "/metadata-types/rdbms-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
public class DatabaseMeta extends HopMetadataBase implements Cloneable, IHopMetadata {
  private static final Class<?> PKG = Database.class;

  private static final String CONST_TABLE = "TABLE";
  private static final String CONST_CONNECTION_ERROR = "DatabaseMeta.report.ConnectionError";
  public static final String XML_TAG = "connection";

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DatabaseMeta-PluginSpecific-Options";

  // Comparator for sorting databases alphabetically by name
  public static final Comparator<DatabaseMeta> comparator =
      (DatabaseMeta dbm1, DatabaseMeta dbm2) -> dbm1.getName().compareToIgnoreCase(dbm2.getName());

  @HopMetadataProperty(key = "rdbms")
  private IDatabase iDatabase;

  private static volatile Future<Map<String, IDatabase>> allDatabaseInterfaces;

  static {
    init();
  }

  public static void init() {
    PluginRegistry.getInstance()
        .addPluginListener(
            DatabasePluginType.class,
            new IPluginTypeListener() {
              @Override
              public void pluginAdded(Object serviceObject) {
                clearDatabaseInterfacesMap();
              }

              @Override
              public void pluginRemoved(Object serviceObject) {
                clearDatabaseInterfacesMap();
              }

              @Override
              public void pluginChanged(Object serviceObject) {
                clearDatabaseInterfacesMap();
              }
            });
  }

  private boolean readOnly = false;

  /** Connect natively through JDBC thin driver to the database. */
  public static final int TYPE_ACCESS_NATIVE = 0;

  /** Short description of the access type, used in serialization. */
  public static final String[] dbAccessTypeCode = {"Native"};

  /** Longer description for user interactions. */
  public static final String[] dbAccessTypeDesc = {"Native (JDBC)"};

  /**
   * Use this length in a String value to indicate that you want to use a CLOB in stead of a normal
   * text field.
   */
  public static final int CLOB_LENGTH = 9999999;

  /** The value to store in the attributes so that an empty value doesn't get lost... */
  public static final String EMPTY_OPTIONS_STRING = "><EMPTY><";

  /**
   * Construct a new database connections. Note that not all these parameters are not always
   * mandatory.
   *
   * @param name The database name
   * @param type The type of database
   * @param access The type of database access
   * @param host The hostname or IP address
   * @param db The database name
   * @param port The port on which the database listens.
   * @param user The username
   * @param pass The password
   */
  public DatabaseMeta(
      String name,
      String type,
      String access,
      String host,
      String db,
      String port,
      String user,
      String pass) {
    setValues(name, type, access, host, db, port, user, pass);
    addOptions();
  }

  /** Create an empty database connection */
  public DatabaseMeta() {
    setDefault();
    addOptions();
  }

  public static DatabaseMeta loadDatabase(
      IHopMetadataProvider metadataProvider, String connectionName) throws HopXmlException {
    if (metadataProvider == null || StringUtils.isEmpty(connectionName)) {
      return null; // Nothing to find or load
    }
    try {
      return metadataProvider.getSerializer(DatabaseMeta.class).load(connectionName);
    } catch (Exception e) {
      throw new HopXmlException(
          "Unable to load relational database connection '" + connectionName + "'", e);
    }
  }

  /** Set default values for an Generic database. */
  public void setDefault() {
    setValues("", "NONE", "Native", "", "", "", "", "");
  }

  /** Add a list of common options for some databases. */
  public void addOptions() {
    iDatabase.addDefaultOptions();
    setSupportsBooleanDataType(true);
    setSupportsTimestampDataType(true);
  }

  public DatabaseMeta(DatabaseMeta databaseMeta) {
    this();
    replaceMeta(databaseMeta);
  }

  /**
   * @return the system dependend database interface for this database metadata definition
   */
  public IDatabase getIDatabase() {
    return iDatabase;
  }

  /**
   * Set the system dependend database interface for this database metadata definition
   *
   * @param iDatabase the system dependend database interface
   */
  public void setIDatabase(IDatabase iDatabase) {
    this.iDatabase = iDatabase;
  }

  /**
   * Search for the right type of IDatabase object and clone it.
   *
   * @param databaseType the type of IDatabase to look for (description)
   * @return The requested IDatabase
   * @throws HopDatabaseException when the type could not be found or referenced.
   */
  public static final IDatabase getIDatabase(String databaseType) throws HopDatabaseException {
    IDatabase di = findIDatabase(databaseType);
    if (di == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG, "DatabaseMeta.Error.DatabaseInterfaceNotFound", databaseType));
    }
    return (IDatabase) di.clone();
  }

  /**
   * Search for the right type of IDatabase object and return it.
   *
   * @param databaseTypeDesc the type of IDatabase to look for (id or description)
   * @return The requested IDatabase
   * @throws HopDatabaseException when the type could not be found or referenced.
   */
  private static final IDatabase findIDatabase(String databaseTypeDesc)
      throws HopDatabaseException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(DatabasePluginType.class, databaseTypeDesc);
    if (plugin == null) {
      plugin = registry.findPluginWithName(DatabasePluginType.class, databaseTypeDesc);
    }

    if (plugin == null) {
      throw new HopDatabaseException(
          "database type with plugin id [" + databaseTypeDesc + "] couldn't be found!");
    }

    return getIDatabaseMap().get(plugin.getIds()[0]);
  }

  @Override
  public Object clone() {
    return new DatabaseMeta(this);
  }

  public void replaceMeta(DatabaseMeta databaseMeta) {
    this.setValues(
        databaseMeta.getName(),
        databaseMeta.getPluginId(),
        databaseMeta.getAccessTypeDesc(),
        databaseMeta.getHostname(),
        databaseMeta.getDatabaseName(),
        databaseMeta.getPort(),
        databaseMeta.getUsername(),
        databaseMeta.getPassword());
    this.setServername(databaseMeta.getServername());
    this.setDataTablespace(databaseMeta.getDataTablespace());
    this.setIndexTablespace(databaseMeta.getIndexTablespace());

    this.iDatabase = (IDatabase) databaseMeta.iDatabase.clone();

    // Replace all attributes
    this.getAttributes().putAll(databaseMeta.getAttributes());

    this.setChanged();
  }

  public void setValues(
      String name,
      String type,
      String access,
      String host,
      String db,
      String port,
      String user,
      String pass) {
    try {
      iDatabase = getIDatabase(type);
    } catch (HopDatabaseException kde) {
      throw new RuntimeException("Database type not found!", kde);
    }

    setName(name);
    setAccessType(getAccessType(access));
    setHostname(host);
    setDBName(db);
    setPort(port);
    setUsername(user);
    setPassword(pass);
    setServername(null);
    setChanged(false);
  }

  public void setDatabaseType(String type) {
    IDatabase oldInterface = iDatabase;

    try {
      iDatabase = getIDatabase(type);
    } catch (HopDatabaseException kde) {
      throw new RuntimeException("Database type [" + type + "] not found!", kde);
    }

    setAccessType(oldInterface.getAccessType());
    setHostname(oldInterface.getHostname());
    setDBName(oldInterface.getDatabaseName());
    setPort(oldInterface.getPort());
    setUsername(oldInterface.getUsername());
    setPassword(oldInterface.getPassword());
    setServername(oldInterface.getServername());
    setDataTablespace(oldInterface.getDataTablespace());
    setIndexTablespace(oldInterface.getIndexTablespace());
    setChanged(oldInterface.isChanged());
  }

  public void setValues(DatabaseMeta info) {
    iDatabase = (IDatabase) info.iDatabase.clone();
  }

  /**
   * @return The plugin ID of the database interface
   */
  public String getPluginId() {
    return iDatabase.getPluginId();
  }

  /**
   * @return The name of the database plugin type
   */
  public String getPluginName() {
    return iDatabase.getPluginName();
  }

  /*
   * Sets the type of database.
   *
   * @param db_type The database type public void
   */

  /**
   * Return the type of database access. One of
   *
   * <p>TYPE_ACCESS_NATIVE
   *
   * <p>TYPE_ACCESS_OCI
   *
   * <p>
   *
   * @return The type of database access.
   */
  public int getAccessType() {
    return iDatabase.getAccessType();
  }

  /**
   * Set the type of database access.
   *
   * @param accessType The access type.
   */
  public void setAccessType(int accessType) {
    iDatabase.setAccessType(accessType);
  }

  /**
   * Gets you a short description of the type of database access.
   *
   * @return A short description of the type of database access.
   */
  public String getAccessTypeDesc() {
    return dbAccessTypeCode[getAccessType()];
  }

  /**
   * Return the hostname of the machine on which the database runs.
   *
   * @return The hostname of the database.
   */
  public String getHostname() {
    return iDatabase.getHostname();
  }

  /**
   * Sets the hostname of the machine on which the database runs.
   *
   * @param hostname The hostname of the machine on which the database runs.
   */
  public void setHostname(String hostname) {
    iDatabase.setHostname(hostname);
  }

  /**
   * Return the port on which the database listens as a String. Allows for parameterisation.
   *
   * @return The database port.
   */
  public String getPort() {
    return iDatabase.getPort();
  }

  /**
   * Sets the port on which the database listens.
   *
   * @param port The port number on which the database listens
   */
  public void setPort(String port) {
    iDatabase.setPort(port);
  }

  /**
   * Return the name of the database.
   *
   * @return The database name.
   */
  public String getDatabaseName() {
    return iDatabase.getDatabaseName();
  }

  /**
   * Set the name of the database.
   *
   * @param databaseName The new name of the database
   */
  public void setDBName(String databaseName) {
    iDatabase.setDatabaseName(databaseName);
  }

  /**
   * Get the username to log into the database on this connection.
   *
   * @return The username to log into the database on this connection.
   */
  public String getUsername() {
    return iDatabase.getUsername();
  }

  /**
   * Sets the username to log into the database on this connection.
   *
   * @param username The username
   */
  public void setUsername(String username) {
    iDatabase.setUsername(username);
  }

  /**
   * Get the password to log into the database on this connection.
   *
   * @return the password to log into the database on this connection.
   */
  public String getPassword() {
    return iDatabase.getPassword();
  }

  /**
   * Sets the password to log into the database on this connection.
   *
   * @param password the password to log into the database on this connection.
   */
  public void setPassword(String password) {
    iDatabase.setPassword(password);
  }

  /**
   * @param servername the Informix servername
   */
  public void setServername(String servername) {
    iDatabase.setServername(servername);
  }

  /**
   * @return the Informix servername
   */
  public String getServername() {
    return iDatabase.getServername();
  }

  public String getDataTablespace() {
    return iDatabase.getDataTablespace();
  }

  public void setDataTablespace(String dataTablespace) {
    iDatabase.setDataTablespace(dataTablespace);
  }

  public String getIndexTablespace() {
    return iDatabase.getIndexTablespace();
  }

  public void setIndexTablespace(String indexTablespace) {
    iDatabase.setIndexTablespace(indexTablespace);
  }

  public void setChanged() {
    setChanged(true);
  }

  public void setChanged(boolean ch) {
    iDatabase.setChanged(ch);
  }

  public boolean hasChanged() {
    return iDatabase.isChanged();
  }

  public void clearChanged() {
    iDatabase.setChanged(false);
  }

  /**
   * @return A manually entered URL which will be used over the internally generated one
   */
  public String getManualUrl() {
    return iDatabase.getManualUrl();
  }

  /**
   * @param manualUrl A manually entered URL which will be used over the internally generated one
   */
  public void setManualUrl(String manualUrl) {
    iDatabase.setManualUrl(manualUrl);
  }

  @Override
  public String toString() {
    return getName();
  }

  /**
   * @return The extra attributes for this database connection
   */
  public Map<String, String> getAttributes() {
    return iDatabase.getAttributes();
  }

  /**
   * Set extra attributes on this database connection
   *
   * @param attributes The extra attributes to set on this database connection.
   */
  public void setAttributes(Map<String, String> attributes) {
    iDatabase.setAttributes(attributes);
  }

  @Override
  public int hashCode() {
    return getName().hashCode(); // name of connection is unique!
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DatabaseMeta databaseMeta && getName().equals(databaseMeta.getName());
  }

  public String getURL(IVariables variables) throws HopDatabaseException {

    /**
     * A manually constructed URL overrides all other options. We make sure to see that there's no
     * variables or tab in the field
     */
    if (StringUtils.isNotEmpty(getManualUrl()) && StringUtils.isNotBlank(getManualUrl())) {
      return getManualUrl();
    }

    String hostname = variables.resolve(getHostname());
    String port = variables.resolve(getPort());
    String databaseName = variables.resolve(getDatabaseName());

    String baseUrl =
        iDatabase.getURL(
            variables.resolve(hostname), variables.resolve(port), variables.resolve(databaseName));
    String url = variables.resolve(baseUrl);

    if (iDatabase.isSupportsOptionsInURL()) {
      url = appendExtraOptions(variables, url, getExtraOptions());
    }

    return url;
  }

  protected String appendExtraOptions(
      IVariables variables, String url, Map<String, String> extraOptions) {
    if (extraOptions.isEmpty()) {
      return url;
    }

    StringBuilder urlBuilder = new StringBuilder(url);

    final String optionIndicator = getExtraOptionIndicator();
    final String optionSeparator = getExtraOptionSeparator();
    final String valueSeparator = getExtraOptionValueSeparator();

    Iterator<String> iterator = extraOptions.keySet().iterator();
    boolean first = true;
    while (iterator.hasNext()) {
      String typedParameter = iterator.next();
      int dotIndex = typedParameter.indexOf('.');
      if (dotIndex == -1) {
        continue;
      }

      final String value = extraOptions.get(typedParameter);
      if (Utils.isEmpty(value) || value.equals(EMPTY_OPTIONS_STRING)) {
        // skip this science no value is provided
        continue;
      }

      final String typeCode = typedParameter.substring(0, dotIndex);
      final String parameter = typedParameter.substring(dotIndex + 1);

      // Only add to the URL if it's the same database type code,
      // or underlying database is the same for both id's, and any subset of
      // connection settings for one database is valid for another
      boolean dbForBothDbInterfacesIsSame = false;
      try {
        IDatabase primaryDb = getDbInterface(typeCode);
        dbForBothDbInterfacesIsSame =
            databaseForBothDbInterfacesIsTheSame(primaryDb, getIDatabase());
      } catch (HopDatabaseException e) {
        getGeneralLogger()
            .logError(
                "IDatabase with "
                    + typeCode
                    + " database type is not found! Parameter "
                    + parameter
                    + "won't be appended to URL");
      }
      if (dbForBothDbInterfacesIsSame) {
        if (first && url.indexOf(valueSeparator) == -1) {
          urlBuilder.append(optionIndicator);
        } else {
          urlBuilder.append(optionSeparator);
        }

        urlBuilder
            .append(variables.resolve(parameter))
            .append(valueSeparator)
            .append(variables.resolve(value));
        first = false;
      }
    }

    return urlBuilder.toString();
  }

  /**
   * This method is designed to identify whether the actual database for two database connection
   * types is the same. This situation can occur in two cases: 1. plugin id of {@code primary} is
   * the same as plugin id of {@code secondary} 2. {@code secondary} is a descendant {@code primary}
   * (with any deepness).
   */
  protected boolean databaseForBothDbInterfacesIsTheSame(IDatabase primary, IDatabase secondary) {
    if (primary == null || secondary == null) {
      throw new IllegalArgumentException("IDatabase shouldn't be null!");
    }

    if (primary.getPluginId() == null || secondary.getPluginId() == null) {
      return false;
    }

    if (primary.getPluginId().equals(secondary.getPluginId())) {
      return true;
    }

    return primary.getClass().isAssignableFrom(secondary.getClass());
  }

  /**
   * @return The extra JDBC options for this connection type with the variables in options and
   *     values expanded
   */
  public Properties getConnectionProperties(IVariables variables) {
    Properties properties = new Properties();

    Map<String, String> map = getExtraOptionsMap();
    for (String option : map.keySet()) {
      String value = map.get(option);
      properties.put(variables.resolve(option), variables.resolve(Const.NVL(value, "")));
    }

    return properties;
  }

  public String getExtraOptionIndicator() {
    return getIDatabase().getExtraOptionIndicator();
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is semicolon
   *     ; )
   */
  public String getExtraOptionSeparator() {
    return getIDatabase().getExtraOptionSeparator();
  }

  /**
   * @return The extra option value separator in database URL for this platform (usually this is the
   *     equal sign = )
   */
  public String getExtraOptionValueSeparator() {
    return getIDatabase().getExtraOptionValueSeparator();
  }

  /**
   * @return all the extra JDBC options, in their original form, for this specific database type
   */
  public Map<String, String> getExtraOptionsMap() {
    Map<String, String> optionsMap = new HashMap<>();

    Map<String, String> map = getExtraOptions();
    if (map.size() > 0) {
      Iterator<String> iterator = map.keySet().iterator();
      while (iterator.hasNext()) {
        String typedParameter = iterator.next();
        int dotIndex = typedParameter.indexOf('.');
        if (dotIndex >= 0) {
          String typeCode = typedParameter.substring(0, dotIndex);
          String parameter = typedParameter.substring(dotIndex + 1);
          String value = map.get(typedParameter);

          // Only add to the URL if it's the same database type code...
          //
          if (iDatabase.getPluginId().equals(typeCode)) {
            if (value != null && value.equals(EMPTY_OPTIONS_STRING)) {
              value = "";
            }
            optionsMap.put(parameter, value);
          }
        }
      }
    }

    return optionsMap;
  }

  /**
   * Add an extra option to the attributes list
   *
   * @param databaseTypeCode The database type code for which the option applies
   * @param option The option to set
   * @param value The value of the option
   */
  public void addExtraOption(String databaseTypeCode, String option, String value) {
    iDatabase.addExtraOption(databaseTypeCode, option, value);
  }

  public void applyDefaultOptions(IDatabase iDatabase) {
    final Map<String, String> extraOptions = getExtraOptions();

    final Map<String, String> defaultOptions = iDatabase.getDefaultOptions();
    for (String option : defaultOptions.keySet()) {
      String value = defaultOptions.get(option);
      String[] split = option.split("[.]", 2);
      if (!extraOptions.containsKey(option) && split.length == 2) {
        addExtraOption(split[0], split[1], value);
      }
    }
  }

  public boolean supportsAutoinc() {
    return iDatabase.isSupportsAutoInc();
  }

  public boolean supportsSequences() {
    return iDatabase.isSupportsSequences();
  }

  public String getSqlSequenceExists(String sequenceName) {
    return iDatabase.getSqlSequenceExists(sequenceName);
  }

  public boolean supportsBitmapIndex() {
    return iDatabase.isSupportsBitmapIndex();
  }

  public boolean supportsSetLong() {
    return iDatabase.isSupportsSetLong();
  }

  /**
   * @return true if the database supports schemas
   */
  public boolean supportsSchemas() {
    return iDatabase.isSupportsSchemas();
  }

  /**
   * @return true if the database supports catalogs
   */
  public boolean supportsCatalogs() {
    return iDatabase.isSupportsCatalogs();
  }

  /**
   * @return true when the database engine supports empty transaction. (for example Informix does
   *     not on a non-ANSI database type!)
   */
  public boolean supportsEmptyTransactions() {
    return iDatabase.isSupportsEmptyTransactions();
  }

  /**
   * See if this database supports the setCharacterStream() method on a PreparedStatement.
   *
   * @return true if we can set a Stream on a field in a PreparedStatement. False if not.
   */
  public boolean supportsSetCharacterStream() {
    return iDatabase.isSupportsSetCharacterStream();
  }

  /**
   * Get the maximum length of a text field for this database connection. This includes optional
   * CLOB, Memo and Text fields. (the maximum!)
   *
   * @return The maximum text field length for this database type. (mostly CLOB_LENGTH)
   */
  public int getMaxTextFieldLength() {
    return iDatabase.getMaxTextFieldLength();
  }

  public static final int getAccessType(String dbaccess) {
    int i;

    if (dbaccess == null) {
      return TYPE_ACCESS_NATIVE;
    }

    for (i = 0; i < dbAccessTypeCode.length; i++) {
      if (dbAccessTypeCode[i].equalsIgnoreCase(dbaccess)) {
        return i;
      }
    }
    for (i = 0; i < dbAccessTypeDesc.length; i++) {
      if (dbAccessTypeDesc[i].equalsIgnoreCase(dbaccess)) {
        return i;
      }
    }

    return TYPE_ACCESS_NATIVE;
  }

  public static final String getAccessTypeDesc(int dbaccess) {
    if (dbaccess < 0) {
      return null;
    }
    if (dbaccess > dbAccessTypeCode.length) {
      return null;
    }

    return dbAccessTypeCode[dbaccess];
  }

  public static final String getAccessTypeDescLong(int dbaccess) {
    if (dbaccess < 0) {
      return null;
    }
    if (dbaccess > dbAccessTypeDesc.length) {
      return null;
    }

    return dbAccessTypeDesc[dbaccess];
  }

  public static final IDatabase[] getDatabaseInterfaces() {
    List<IDatabase> list = new ArrayList<>(getIDatabaseMap().values());
    return list.toArray(new IDatabase[list.size()]);
  }

  /**
   * Clear the database interfaces map. The map is cached by getDatabaseInterfacesMap(), but in some
   * instances it may need to be reloaded (such as adding/updating Database plugins). After calling
   * clearDatabaseInterfacesMap(), the next call to getDatabaseInterfacesMap() will reload the map.
   */
  public static final void clearDatabaseInterfacesMap() {
    allDatabaseInterfaces = null;
  }

  private static final Future<Map<String, IDatabase>> createDatabaseInterfacesMap() {
    return ExecutorUtil.getExecutor()
        .submit(
            new Callable<Map<String, IDatabase>>() {
              private Map<String, IDatabase> doCreate() {
                ILogChannel log = LogChannel.GENERAL;
                PluginRegistry registry = PluginRegistry.getInstance();

                List<IPlugin> plugins = registry.getPlugins(DatabasePluginType.class);
                HashMap<String, IDatabase> tmpAllDatabaseInterfaces = new HashMap<>();
                for (IPlugin plugin : plugins) {
                  try {
                    IDatabase iDatabase = (IDatabase) registry.loadClass(plugin);
                    iDatabase.setPluginId(plugin.getIds()[0]);
                    iDatabase.setPluginName(plugin.getName());
                    tmpAllDatabaseInterfaces.put(plugin.getIds()[0], iDatabase);
                  } catch (HopPluginException cnfe) {
                    log.logError(
                        "Could not create connection entry for "
                            + plugin.getName()
                            + ".  "
                            + cnfe.getCause().getClass().getName());
                    if (log.isDebug()) {
                      log.logDebug("Debug-Error loading plugin: " + plugin, cnfe);
                    }
                  } catch (Exception e) {
                    log.logError("Error loading plugin: " + plugin, e);
                  }
                }
                return Collections.unmodifiableMap(tmpAllDatabaseInterfaces);
              }

              @Override
              public Map<String, IDatabase> call() throws Exception {
                return doCreate();
              }
            });
  }

  public static final Map<String, IDatabase> getIDatabaseMap() {
    Future<Map<String, IDatabase>> allDatabaseInterfaces = DatabaseMeta.allDatabaseInterfaces;
    while (allDatabaseInterfaces == null) {
      DatabaseMeta.allDatabaseInterfaces = createDatabaseInterfacesMap();
      allDatabaseInterfaces = DatabaseMeta.allDatabaseInterfaces;
    }
    try {
      return allDatabaseInterfaces.get();
    } catch (Exception e) {
      clearDatabaseInterfacesMap();
      // doCreate() above doesn't declare any exceptions so anything that comes out SHOULD be a
      // runtime exception
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public static final int[] getAccessTypeList(String dbTypeDesc) {
    try {
      IDatabase di = findIDatabase(dbTypeDesc);
      return di.getAccessTypeList();
    } catch (HopDatabaseException kde) {
      return null;
    }
  }

  public static final int getPortForDBType(String strtype, String straccess) {
    try {
      IDatabase di = getIDatabase(strtype);
      di.setAccessType(getAccessType(straccess));
      return di.getDefaultDatabasePort();
    } catch (HopDatabaseException kde) {
      return -1;
    }
  }

  public int getDefaultDatabasePort() {
    return iDatabase.getDefaultDatabasePort();
  }

  public int getNotFoundTK(boolean useAutoIncrement) {
    return iDatabase.getNotFoundTK(useAutoIncrement);
  }

  public String getDriverClass(IVariables variables) {
    return variables.resolve(iDatabase.getDriverClass());
  }

  public String stripCR(String sbsql) {
    if (sbsql == null) {
      return null;
    }
    return stripCR(new StringBuilder(sbsql));
  }

  public String stripCR(StringBuffer sbsql) {
    // DB2 Can't handle \n in SQL Statements...
    if (!supportsNewLinesInSql()) {
      // Remove CR's
      for (int i = sbsql.length() - 1; i >= 0; i--) {
        if (sbsql.charAt(i) == '\n' || sbsql.charAt(i) == '\r') {
          sbsql.setCharAt(i, ' ');
        }
      }
    }

    return sbsql.toString();
  }

  public String stripCR(StringBuilder sbsql) {
    // DB2 Can't handle \n in SQL Statements...
    if (!supportsNewLinesInSql()) {
      // Remove CR's
      for (int i = sbsql.length() - 1; i >= 0; i--) {
        if (sbsql.charAt(i) == '\n' || sbsql.charAt(i) == '\r') {
          sbsql.setCharAt(i, ' ');
        }
      }
    }

    return sbsql.toString();
  }

  public String getSeqNextvalSql(String sequenceName) {
    return iDatabase.getSqlNextSequenceValue(sequenceName);
  }

  public String getSqlCurrentSequenceValue(String sequenceName) {
    return iDatabase.getSqlCurrentSequenceValue(sequenceName);
  }

  public boolean isFetchSizeSupported() {
    return iDatabase.isFetchSizeSupported();
  }

  /**
   * Indicates the need to insert a placeholder (0) for auto increment fields.
   *
   * @return true if we need a placeholder for auto increment fields in insert statements.
   */
  public boolean needsPlaceHolder() {
    return iDatabase.isNeedsPlaceHolder();
  }

  public String getFunctionSum() {
    return iDatabase.getFunctionSum();
  }

  public String getFunctionAverage() {
    return iDatabase.getFunctionAverage();
  }

  public String getFunctionMaximum() {
    return iDatabase.getFunctionMaximum();
  }

  public String getFunctionMinimum() {
    return iDatabase.getFunctionMinimum();
  }

  public String getFunctionCount() {
    return iDatabase.getFunctionCount();
  }

  /**
   * Check the database connection parameters and give back an array of remarks
   *
   * @return an array of remarks Strings
   */
  public String[] checkParameters() {
    ArrayList<String> remarks = new ArrayList<>();

    if (getIDatabase() == null) {
      remarks.add(BaseMessages.getString(PKG, "DatabaseMeta.BadInterface"));
    }

    if (getName() == null || getName().length() == 0) {
      remarks.add(BaseMessages.getString(PKG, "DatabaseMeta.BadConnectionName"));
    }

    if (getIDatabase().isRequiresName()
        && getIDatabase().getManualUrl().isEmpty()
        && (getDatabaseName() == null || getDatabaseName().length() == 0)) {
      remarks.add(BaseMessages.getString(PKG, "DatabaseMeta.BadDatabaseName"));
    }

    return remarks.toArray(new String[remarks.size()]);
  }

  /**
   * Calculate the schema-table combination, usually this is the schema and table separated with a
   * dot. (schema.table)
   *
   * @param schemaName the schema-name or null if no schema is used.
   * @param tableName the table name
   * @return the schemaname-tablename combination
   */
  public String getQuotedSchemaTableCombination(
      IVariables variables, String schemaName, String tableName) {
    if (Utils.isEmpty(schemaName)) {
      if (Utils.isEmpty(getPreferredSchemaName())) {
        return quoteField(variables.resolve(tableName)); // no need to look further
      } else {
        return iDatabase.getSchemaTableCombination(
            quoteField(variables.resolve(getPreferredSchemaName())),
            quoteField(variables.resolve(tableName)));
      }
    } else {
      return iDatabase.getSchemaTableCombination(
          quoteField(variables.resolve(schemaName)), quoteField(variables.resolve(tableName)));
    }
  }

  public boolean isClob(IValueMeta v) {
    boolean retval = true;

    if (v == null || v.getLength() < DatabaseMeta.CLOB_LENGTH) {
      retval = false;
    } else {
      return true;
    }
    return retval;
  }

  public String getFieldDefinition(IValueMeta v, String tk, String pk, boolean useAutoIncrement) {
    return getFieldDefinition(v, tk, pk, useAutoIncrement, true, true);
  }

  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldname,
      boolean addCr) {

    String definition =
        v.getDatabaseColumnTypeDefinition(iDatabase, tk, pk, useAutoIncrement, addFieldname, addCr);
    if (!Utils.isEmpty(definition)) {
      return definition;
    }

    return iDatabase.getFieldDefinition(v, tk, pk, useAutoIncrement, addFieldname, addCr);
  }

  public String getLimitClause(int nrRows) {
    return iDatabase.getLimitClause(nrRows);
  }

  /**
   * @param tableName The table or schema-table combination. We expect this to be quoted properly
   *     already!
   * @return the SQL for to get the fields of this table.
   */
  public String getSqlQueryFields(String tableName) {
    return iDatabase.getSqlQueryFields(tableName);
  }

  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    String retval =
        iDatabase.getAddColumnStatement(tableName, v, tk, useAutoIncrement, pk, semicolon);
    retval += Const.CR;
    if (semicolon) {
      retval += ";" + Const.CR;
    }
    return retval;
  }

  public String getDropColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    String retval =
        iDatabase.getDropColumnStatement(tableName, v, tk, useAutoIncrement, pk, semicolon);
    retval += Const.CR;
    if (semicolon) {
      retval += ";" + Const.CR;
    }
    return retval;
  }

  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    String retval =
        iDatabase.getModifyColumnStatement(tableName, v, tk, useAutoIncrement, pk, semicolon);
    retval += Const.CR;
    if (semicolon) {
      retval += ";" + Const.CR;
    }

    return retval;
  }

  /**
   * @return an array of reserved words for the database type...
   */
  public String[] getReservedWords() {
    return iDatabase.getReservedWords();
  }

  /**
   * @return true if reserved words need to be double quoted ("password", "select", ...)
   */
  public boolean quoteReservedWords() {
    return iDatabase.isQuoteReservedWords();
  }

  /**
   * @return The start quote sequence, mostly just double quote, but sometimes [, ...
   */
  public String getStartQuote() {
    return iDatabase.getStartQuote();
  }

  /**
   * @return The end quote sequence, mostly just double quote, but sometimes ], ...
   */
  public String getEndQuote() {
    return iDatabase.getEndQuote();
  }

  /**
   * Returns a quoted field if this is needed: contains spaces, is a reserved word, ...
   *
   * @param field The fieldname to check for quoting
   * @return The quoted field (if this is needed.
   */
  public String quoteField(String field) {
    if (Utils.isEmpty(field)) {
      return null;
    }

    if (isForcingIdentifiersToLowerCase()) {
      field = field.toLowerCase();
    } else if (isForcingIdentifiersToUpperCase()) {
      field = field.toUpperCase();
    }

    // If the field already contains quotes, we don't touch it anymore, just return the same
    // string...
    if (field.indexOf(getStartQuote()) >= 0 || field.indexOf(getEndQuote()) >= 0) {
      return field;
    }

    if (isReservedWord(field) && quoteReservedWords()) {
      return handleCase(getStartQuote() + field + getEndQuote());
    } else {
      if (iDatabase.isQuoteAllFields()
          || hasSpacesInField(field)
          || hasSpecialCharInField(field)
          || hasDotInField(field)) {
        return getStartQuote() + field + getEndQuote();
      } else {
        return field;
      }
    }
  }

  private String handleCase(String field) {
    if (preserveReservedCase()) {
      return field;
    } else {
      if (iDatabase.isDefaultingToUppercase()) {
        return field.toUpperCase();
      } else {
        return field.toLowerCase();
      }
    }
  }

  /**
   * Determines whether or not this field is in need of quoting:<br>
   * - When the fieldname contains spaces<br>
   * - When the fieldname is a reserved word<br>
   *
   * @param fieldname the fieldname to check if there is a need for quoting
   * @return true if the fieldname needs to be quoted.
   */
  public boolean isInNeedOfQuoting(String fieldname) {
    return isReservedWord(fieldname) || hasSpacesInField(fieldname);
  }

  /**
   * Returns true if the string specified is a reserved word on this database type.
   *
   * @param word The word to check
   * @return true if word is a reserved word on this database.
   */
  public boolean isReservedWord(String word) {
    String[] reserved = getReservedWords();
    return Const.indexOfString(word, reserved) >= 0;
  }

  /**
   * Detects if a field has spaces in the name. We need to quote the field in that case.
   *
   * @param fieldname The fieldname to check for spaces
   * @return true if the fieldname contains spaces
   */
  public boolean hasSpacesInField(String fieldname) {
    if (fieldname == null) {
      return false;
    }
    return fieldname.indexOf(' ') >= 0;
  }

  /**
   * Detects if a field has spaces in the name. We need to quote the field in that case.
   *
   * @param fieldname The fieldname to check for spaces
   * @return true if the fieldname contains spaces
   */
  public boolean hasSpecialCharInField(String fieldname) {
    if (fieldname == null) {
      return false;
    }
    if (fieldname.indexOf('/') >= 0) {
      return true;
    }
    if (fieldname.indexOf('-') >= 0) {
      return true;
    }
    if (fieldname.indexOf('+') >= 0) {
      return true;
    }
    if (fieldname.indexOf(',') >= 0) {
      return true;
    }
    if (fieldname.indexOf('*') >= 0) {
      return true;
    }
    if (fieldname.indexOf('(') >= 0) {
      return true;
    }
    if (fieldname.indexOf(')') >= 0) {
      return true;
    }
    if (fieldname.indexOf('{') >= 0) {
      return true;
    }
    if (fieldname.indexOf('}') >= 0) {
      return true;
    }
    if (fieldname.indexOf('[') >= 0) {
      return true;
    }
    if (fieldname.indexOf(']') >= 0) {
      return true;
    }
    if (fieldname.indexOf('%') >= 0) {
      return true;
    }
    if (fieldname.indexOf('@') >= 0) {
      return true;
    }
    return fieldname.indexOf('?') >= 0;
  }

  public boolean hasDotInField(String fieldname) {
    if (fieldname == null) {
      return false;
    }
    return fieldname.indexOf('.') >= 0;
  }

  /**
   * Checks the fields specified for reserved words and quotes them.
   *
   * @param fields the list of fields to check
   * @return true if one or more values have a name that is a reserved word on this database type.
   */
  public boolean replaceReservedWords(IRowMeta fields) {
    boolean hasReservedWords = false;
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v = fields.getValueMeta(i);
      if (isReservedWord(v.getName())) {
        hasReservedWords = true;
        v.setName(quoteField(v.getName()));
      }
    }
    return hasReservedWords;
  }

  /**
   * Checks the fields specified for reserved words
   *
   * @param fields the list of fields to check
   * @return The nr of reserved words for this database.
   */
  public int getNrReservedWords(IRowMeta fields) {
    int nrReservedWords = 0;
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v = fields.getValueMeta(i);
      if (isReservedWord(v.getName())) {
        nrReservedWords++;
      }
    }
    return nrReservedWords;
  }

  /**
   * @return a list of types to get the available tables
   */
  public String[] getTableTypes() {
    return iDatabase.getTableTypes();
  }

  /**
   * @return a list of types to get the available views
   */
  public String[] getViewTypes() {
    return iDatabase.getViewTypes();
  }

  /**
   * @return a list of types to get the available synonyms
   */
  public String[] getSynonymTypes() {
    return iDatabase.getSynonymTypes();
  }

  /**
   * @return true if we need to supply the schema-name to getTables in order to get a correct list
   *     of items.
   */
  public boolean useSchemaNameForTableList() {
    return iDatabase.useSchemaNameForTableList();
  }

  /**
   * @return true if the database supports views
   */
  public boolean supportsViews() {
    return iDatabase.isSupportsViews();
  }

  /**
   * @return true if the database supports synonyms
   */
  public boolean supportsSynonyms() {
    return iDatabase.isSupportsSynonyms();
  }

  /**
   * @return The SQL on this database to get a list of stored procedures.
   */
  public String getSqlListOfProcedures() {
    return iDatabase.getSqlListOfProcedures();
  }

  /**
   * @param tableName The tablename to be truncated
   * @return The SQL statement to remove all rows from the specified statement, if possible without
   *     using transactions
   */
  public String getTruncateTableStatement(IVariables variables, String schema, String tableName) {
    return iDatabase.getTruncateTableStatement(
        getQuotedSchemaTableCombination(variables, schema, tableName));
  }

  /**
   * @return true if the database rounds floating point numbers to the right precision. For example
   *     if the target field is number(7,2) the value 12.399999999 is converted into 12.40
   */
  public boolean supportsFloatRoundingOnUpdate() {
    return iDatabase.isSupportsFloatRoundingOnUpdate();
  }

  /**
   * @param tableNames The names of the tables to lock
   * @return The SQL commands to lock database tables for write purposes. null is returned in case
   *     locking is not supported on the target database.
   */
  public String getSqlLockTables(String[] tableNames) {
    return iDatabase.getSqlLockTables(tableNames);
  }

  /**
   * @param tableNames The names of the tables to unlock
   * @return The SQL commands to unlock databases tables. null is returned in case locking is not
   *     supported on the target database.
   */
  public String getSqlUnlockTables(String[] tableNames) {
    return iDatabase.getSqlUnlockTables(tableNames);
  }

  /**
   * @return a feature list for the chosen database type.
   */
  public List<RowMetaAndData> getFeatureSummary(IVariables variables) {
    List<RowMetaAndData> list = new ArrayList<>();
    RowMetaAndData r = null;
    final String par = "Parameter";
    final String val = "Value";

    IValueMeta testValue = new ValueMetaString("FIELD");
    testValue.setLength(30);

    if (iDatabase != null) {
      // Type of database
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Database type");
      r.addValue(val, IValueMeta.TYPE_STRING, getPluginId());
      list.add(r);
      // Type of access
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Access type");
      r.addValue(val, IValueMeta.TYPE_STRING, getAccessTypeDesc());
      list.add(r);
      // Name of database
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Database name");
      r.addValue(val, IValueMeta.TYPE_STRING, getDatabaseName());
      list.add(r);
      // server host name
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Server hostname");
      r.addValue(val, IValueMeta.TYPE_STRING, getHostname());
      list.add(r);
      // Port number
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Service port");
      r.addValue(val, IValueMeta.TYPE_STRING, getPort());
      list.add(r);
      // Username
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Username");
      r.addValue(val, IValueMeta.TYPE_STRING, getUsername());
      list.add(r);
      // Informix server
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Informix server name");
      r.addValue(val, IValueMeta.TYPE_STRING, getServername());
      list.add(r);
      // Other properties...
      for (String key : getAttributes().keySet()) {
        String value = getAttributes().get(key);
        r = new RowMetaAndData();
        r.addValue(par, IValueMeta.TYPE_STRING, "Extra attribute [" + key + "]");
        r.addValue(val, IValueMeta.TYPE_STRING, value);
        list.add(r);
      }

      // driver class
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Driver class");
      r.addValue(val, IValueMeta.TYPE_STRING, getDriverClass(variables));
      list.add(r);
      // URL
      String pwd = getPassword();
      setPassword("password"); // Don't give away the password in the URL!
      String url = "";
      try {
        url = getURL(variables);
      } catch (Exception e) {
        url = "";
      } // SAP etc.
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "URL");
      r.addValue(val, IValueMeta.TYPE_STRING, url);
      list.add(r);
      setPassword(pwd);
      // SQL: Next sequence value
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "SQL: next sequence value");
      r.addValue(val, IValueMeta.TYPE_STRING, getSeqNextvalSql("SEQUENCE"));
      list.add(r);
      // is set fetch size supported
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supported: set fetch size");
      r.addValue(val, IValueMeta.TYPE_STRING, isFetchSizeSupported() ? "Y" : "N");
      list.add(r);
      // needs place holder for auto increment
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "auto increment field needs placeholder");
      r.addValue(val, IValueMeta.TYPE_STRING, needsPlaceHolder() ? "Y" : "N");
      list.add(r);
      // Sum function
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "SUM aggregate function");
      r.addValue(val, IValueMeta.TYPE_STRING, getFunctionSum());
      list.add(r);
      // Avg function
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "AVG aggregate function");
      r.addValue(val, IValueMeta.TYPE_STRING, getFunctionAverage());
      list.add(r);
      // Minimum function
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "MIN aggregate function");
      r.addValue(val, IValueMeta.TYPE_STRING, getFunctionMinimum());
      list.add(r);
      // Maximum function
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "MAX aggregate function");
      r.addValue(val, IValueMeta.TYPE_STRING, getFunctionMaximum());
      list.add(r);
      // Count function
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "COUNT aggregate function");
      r.addValue(val, IValueMeta.TYPE_STRING, getFunctionCount());
      list.add(r);
      // Schema-table combination
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Schema / Table combination");
      r.addValue(
          val,
          IValueMeta.TYPE_STRING,
          getQuotedSchemaTableCombination(variables, "SCHEMA", CONST_TABLE));
      list.add(r);
      // Limit clause
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "LIMIT clause for 100 rows");
      r.addValue(val, IValueMeta.TYPE_STRING, getLimitClause(100));
      list.add(r);
      // add column statement
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Add column statement");
      r.addValue(
          val,
          IValueMeta.TYPE_STRING,
          getAddColumnStatement(CONST_TABLE, testValue, null, false, null, false));
      list.add(r);
      // drop column statement
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Drop column statement");
      r.addValue(
          val,
          IValueMeta.TYPE_STRING,
          getDropColumnStatement(CONST_TABLE, testValue, null, false, null, false));
      list.add(r);
      // Modify column statement
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Modify column statement");
      r.addValue(
          val,
          IValueMeta.TYPE_STRING,
          getModifyColumnStatement(CONST_TABLE, testValue, null, false, null, false));
      list.add(r);

      // List of reserved words
      String reserved = "";
      if (getReservedWords() != null) {
        for (int i = 0; i < getReservedWords().length; i++) {
          reserved += (i > 0 ? ", " : "") + getReservedWords()[i];
        }
      }
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "List of reserved words");
      r.addValue(val, IValueMeta.TYPE_STRING, reserved);
      list.add(r);

      // Quote reserved words?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Quote reserved words?");
      r.addValue(val, IValueMeta.TYPE_STRING, quoteReservedWords() ? "Y" : "N");
      list.add(r);
      // Start Quote
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "Start quote for reserved words");
      r.addValue(val, IValueMeta.TYPE_STRING, getStartQuote());
      list.add(r);
      // End Quote
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "End quote for reserved words");
      r.addValue(val, IValueMeta.TYPE_STRING, getEndQuote());
      list.add(r);

      // List of table types
      String types = "";
      String[] slist = getTableTypes();
      if (slist != null) {
        for (int i = 0; i < slist.length; i++) {
          types += (i > 0 ? ", " : "") + slist[i];
        }
      }
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "List of JDBC table types");
      r.addValue(val, IValueMeta.TYPE_STRING, types);
      list.add(r);

      // List of view types
      types = "";
      slist = getViewTypes();
      if (slist != null) {
        for (int i = 0; i < slist.length; i++) {
          types += (i > 0 ? ", " : "") + slist[i];
        }
      }
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "List of JDBC view types");
      r.addValue(val, IValueMeta.TYPE_STRING, types);
      list.add(r);

      // List of synonym types
      types = "";
      slist = getSynonymTypes();
      if (slist != null) {
        for (int i = 0; i < slist.length; i++) {
          types += (i > 0 ? ", " : "") + slist[i];
        }
      }
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "List of JDBC synonym types");
      r.addValue(val, IValueMeta.TYPE_STRING, types);
      list.add(r);

      // Use schema-name to get list of tables?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "use schema name to get table list?");
      r.addValue(val, IValueMeta.TYPE_STRING, useSchemaNameForTableList() ? "Y" : "N");
      list.add(r);
      // supports view?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports views?");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsViews() ? "Y" : "N");
      list.add(r);
      // supports synonyms?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports synonyms?");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsSynonyms() ? "Y" : "N");
      list.add(r);
      // SQL: get list of procedures?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "SQL: list of procedures");
      r.addValue(val, IValueMeta.TYPE_STRING, getSqlListOfProcedures());
      list.add(r);
      // SQL: get truncate table statement?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "SQL: truncate table");
      String truncateStatement = getTruncateTableStatement(variables, "SCHEMA", CONST_TABLE);
      r.addValue(
          val,
          IValueMeta.TYPE_STRING,
          truncateStatement != null ? truncateStatement : "Not supported by this database type");
      list.add(r);
      // supports float rounding on update?
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports floating point rounding on update/insert");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsFloatRoundingOnUpdate() ? "Y" : "N");
      list.add(r);
      // supports time stamp to date conversion
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports timestamp-date conversion");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsTimeStampToDateConversion() ? "Y" : "N");
      list.add(r);
      // supports batch updates
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports batch updates");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsBatchUpdates() ? "Y" : "N");
      list.add(r);
      // supports boolean values
      r = new RowMetaAndData();
      r.addValue(par, IValueMeta.TYPE_STRING, "supports boolean data type");
      r.addValue(val, IValueMeta.TYPE_STRING, supportsBooleanDataType() ? "Y" : "N");
      list.add(r);
    }

    return list;
  }

  /**
   * @return true if the database result sets support getTimeStamp() to retrieve date-time. (Date)
   */
  public boolean supportsTimeStampToDateConversion() {
    return iDatabase.isSupportsTimeStampToDateConversion();
  }

  /**
   * @return true if the database JDBC driver supports batch updates For example Interbase doesn't
   *     support this!
   */
  public boolean supportsBatchUpdates() {
    return iDatabase.isSupportsBatchUpdates();
  }

  /**
   * @return true if the database supports a boolean, bit, logical, ... datatype
   */
  public boolean supportsBooleanDataType() {
    return iDatabase.isSupportsBooleanDataType();
  }

  /**
   * @param b Set to true if the database supports a boolean, bit, logical, ... datatype
   */
  public void setSupportsBooleanDataType(boolean b) {
    iDatabase.setSupportsBooleanDataType(b);
  }

  /**
   * @return true if the database supports the Timestamp data type (nanosecond precision and all)
   */
  public boolean supportsTimestampDataType() {
    return iDatabase.isSupportsTimestampDataType();
  }

  /**
   * @param b Set to true if the database supports the Timestamp data type (nanosecond precision and
   *     all)
   */
  public void setSupportsTimestampDataType(boolean b) {
    iDatabase.setSupportsTimestampDataType(b);
  }

  /**
   * @return true if reserved words' case should be preserved
   */
  public boolean preserveReservedCase() {
    return iDatabase.isPreserveReservedCase();
  }

  /**
   * @param b set to true if reserved words' case should be preserved
   */
  public void setPreserveReservedCase(boolean b) {
    iDatabase.setPreserveReservedCase(b);
  }

  /**
   * Changes the names of the fields to their quoted equivalent if this is needed
   *
   * @param fields The row of fields to change
   */
  public void quoteReservedWords(IRowMeta fields) {
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v = fields.getValueMeta(i);
      v.setName(quoteField(v.getName()));
    }
  }

  /**
   * @return a map of all the extra URL options you want to set.
   */
  public Map<String, String> getExtraOptions() {
    return iDatabase.getExtraOptions();
  }

  /**
   * @return true if the database supports connection options in the URL, false if they are put in a
   *     Properties object.
   */
  public boolean supportsOptionsInURL() {
    return iDatabase.isSupportsOptionsInURL();
  }

  /**
   * @return extra help text on the supported options on the selected database platform.
   */
  public String getExtraOptionsHelpText() {
    return iDatabase.getExtraOptionsHelpText();
  }

  /**
   * @return true if the database JDBC driver supports getBlob on the resultset. If not we must use
   *     getBytes() to get the data.
   */
  public boolean supportsGetBlob() {
    return iDatabase.isSupportsGetBlob();
  }

  /**
   * @return The SQL to execute right after connecting
   */
  public String getConnectSql() {
    return iDatabase.getConnectSql();
  }

  /**
   * @param sql The SQL to execute right after connecting
   */
  public void setConnectSql(String sql) {
    iDatabase.setConnectSql(sql);
  }

  /**
   * @return true if the database supports setting the maximum number of return rows in a resultset.
   */
  public boolean supportsSetMaxRows() {
    return iDatabase.isSupportsSetMaxRows();
  }

  /**
   * Verify the name of the database and if required, change it if it already exists in the list of
   * databases.
   *
   * @param databases the databases to check against.
   * @param oldname the old name of the database
   * @return the new name of the database connection
   */
  public String verifyAndModifyDatabaseName(List<DatabaseMeta> databases, String oldname) {
    String name = getName();
    if (name.equalsIgnoreCase(oldname)) {
      return name; // nothing to see here: move along!
    }

    int nr = 2;
    while (DatabaseMeta.findDatabase(databases, getName()) != null) {
      setName(name + " " + nr);
      nr++;
    }
    return getName();
  }

  public String getSqlTableExists(String tableName) {
    return iDatabase.getSqlTableExists(tableName);
  }

  public String getSqlColumnExists(String columnname, String tableName) {
    return iDatabase.getSqlColumnExists(columnname, tableName);
  }

  /**
   * Get the DELETE statement for the current database given the table name
   *
   * @param tableName
   * @return
   */
  public String getSqlDeleteStmt(String tableName) {
    return iDatabase.getSqlDeleteStmt(tableName);
  }

  /**
   * Get the UPDATE statement for the current database given the table name
   *
   * @param tableName
   * @return
   */
  public String getSqlUpdateStmt(String tableName) {
    return iDatabase.getSqlUpdateStmt(tableName);
  }

  /* Returns weather or not the database supports a custom SQL statement to perform delete operations */
  public boolean isSupportsCustomDeleteStmt() {
    return iDatabase.isSupportsCustomDeleteStmt();
  }

  /* Returns weather or not the database supports a custom SQL statement to perform update operations */
  public boolean isSupportsCustomUpdateStmt() {
    return iDatabase.isSupportsCustomUpdateStmt();
  }

  /**
   * @return true if the database is streaming results (normally this is an option just for MySQL).
   */
  public boolean isStreamingResults() {
    return iDatabase.isStreamingResults();
  }

  /**
   * @param useStreaming true if we want the database to stream results (normally this is an option
   *     just for MySQL).
   */
  public void setStreamingResults(boolean useStreaming) {
    iDatabase.setStreamingResults(useStreaming);
  }

  /**
   * @return true if all fields should always be quoted in db
   */
  public boolean isQuoteAllFields() {
    return iDatabase.isQuoteAllFields();
  }

  /**
   * @param quoteAllFields true if all fields in DB should be quoted.
   */
  public void setQuoteAllFields(boolean quoteAllFields) {
    iDatabase.setQuoteAllFields(quoteAllFields);
  }

  /**
   * @return true if all identifiers should be forced to lower case
   */
  public boolean isForcingIdentifiersToLowerCase() {
    return iDatabase.isForcingIdentifiersToLowerCase();
  }

  /**
   * @param forceLowerCase true if all identifiers should be forced to lower case
   */
  public void setForcingIdentifiersToLowerCase(boolean forceLowerCase) {
    iDatabase.setForcingIdentifiersToLowerCase(forceLowerCase);
  }

  /**
   * @return true if all identifiers should be forced to upper case
   */
  public boolean isForcingIdentifiersToUpperCase() {
    return iDatabase.isForcingIdentifiersToUpperCase();
  }

  /**
   * @param forceUpperCase true if all identifiers should be forced to upper case
   */
  public void setForcingIdentifiersToUpperCase(boolean forceUpperCase) {
    iDatabase.setForcingIdentifiersToUpperCase(forceUpperCase);
  }

  /**
   * Find a database with a certain name in an arraylist of databases.
   *
   * @param databases The ArrayList of databases
   * @param dbname The name of the database connection
   * @return The database object if one was found, null otherwise.
   */
  public static final DatabaseMeta findDatabase(List<DatabaseMeta> databases, String dbname) {
    if (databases == null || dbname == null) {
      return null;
    }

    for (int i = 0; i < databases.size(); i++) {
      DatabaseMeta ci = databases.get(i);
      if (ci.getName().trim().equalsIgnoreCase(dbname.trim())) {
        return ci;
      }
    }
    return null;
  }

  public static int indexOfName(String[] databaseNames, String name) {
    if (databaseNames == null || name == null) {
      return -1;
    }

    for (int i = 0; i < databaseNames.length; i++) {
      String databaseName = databaseNames[i];
      if (name.equalsIgnoreCase(databaseName)) {
        return i;
      }
    }

    return -1;
  }

  /**
   * @return the SQL Server instance
   */
  public String getSqlServerInstance() {
    // This is also covered/persisted by JDBC option MS SQL Server / instancename / <somevalue>
    // We want to return <somevalue>
    // --> MSSQL.instancename
    return getExtraOptions().get(getPluginId() + ".instance");
  }

  /**
   * @param instanceName the SQL Server instance
   */
  public void setSqlServerInstance(String instanceName) {
    // This is also covered/persisted by JDBC option MS SQL Server / instancename / <somevalue>
    // We want to return set <somevalue>
    // --> MSSQL.instancename
    if ((instanceName != null) && (instanceName.length() > 0)) {
      addExtraOption(getPluginId(), "instance", instanceName);
    }
  }

  /**
   * @return true if the Microsoft SQL server uses two decimals (..) to separate schema and table
   *     (default==false).
   */
  public boolean isUsingDoubleDecimalAsSchemaTableSeparator() {
    return iDatabase.isUsingDoubleDecimalAsSchemaTableSeparator();
  }

  /**
   * @param useDoubleDecimalSeparator true if we want the database to stream results (normally this
   *     is an option just for MySQL).
   */
  public void setUsingDoubleDecimalAsSchemaTableSeparator(boolean useDoubleDecimalSeparator) {
    iDatabase.setUsingDoubleDecimalAsSchemaTableSeparator(useDoubleDecimalSeparator);
  }

  /**
   * @return true if this database needs a transaction to perform a query (auto-commit turned off).
   */
  public boolean isRequiringTransactionsOnQueries() {
    return iDatabase.isRequiringTransactionsOnQueries();
  }

  public String testConnection(IVariables variables) {

    StringBuilder report = new StringBuilder();

    // If the plug-in needs to provide connection information, we ask the IDatabase...
    //
    try {
      IDatabaseFactory factory = getDatabaseFactory();
      return factory.getConnectionTestReport(variables, this);
    } catch (ClassNotFoundException e) {
      report
          .append(
              BaseMessages.getString(
                  PKG, "BaseDatabaseMeta.TestConnectionReportNotImplemented.Message"))
          .append(Const.CR);
      report.append(BaseMessages.getString(PKG, CONST_CONNECTION_ERROR, getName()) + e + Const.CR);
      report.append(Const.getStackTracker(e) + Const.CR);
    } catch (Exception e) {
      report.append(BaseMessages.getString(PKG, CONST_CONNECTION_ERROR, getName()) + e + Const.CR);
      report.append(Const.getStackTracker(e) + Const.CR);
    }
    return report.toString();
  }

  public DatabaseTestResults testConnectionSuccess(IVariables variables) {

    StringBuilder report = new StringBuilder();
    DatabaseTestResults databaseTestResults = new DatabaseTestResults();

    // If the plug-in needs to provide connection information, we ask the IDatabase...
    //
    try {
      IDatabaseFactory factory = getDatabaseFactory();
      databaseTestResults = factory.getConnectionTestResults(variables, this);
    } catch (ClassNotFoundException e) {
      report
          .append(
              BaseMessages.getString(
                  PKG, "BaseDatabaseMeta.TestConnectionReportNotImplemented.Message"))
          .append(Const.CR);
      report.append(BaseMessages.getString(PKG, CONST_CONNECTION_ERROR, getName()) + e + Const.CR);
      report.append(Const.getStackTracker(e) + Const.CR);
      databaseTestResults.setMessage(report.toString());
      databaseTestResults.setSuccess(false);
    } catch (Exception e) {
      report.append(BaseMessages.getString(PKG, CONST_CONNECTION_ERROR, getName()) + e + Const.CR);
      report.append(Const.getStackTracker(e) + Const.CR);
      databaseTestResults.setMessage(report.toString());
      databaseTestResults.setSuccess(false);
    }
    return databaseTestResults;
  }

  public IDatabaseFactory getDatabaseFactory() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(DatabasePluginType.class, iDatabase.getPluginId());
    if (plugin == null) {
      throw new HopDatabaseException(
          "database type with plugin id [" + iDatabase.getPluginId() + "] couldn't be found!");
    }

    ClassLoader loader = registry.getClassLoader(plugin);

    Class<?> clazz = Class.forName(iDatabase.getDatabaseFactoryName(), true, loader);
    return (IDatabaseFactory) clazz.getDeclaredConstructor().newInstance();
  }

  public String getPreferredSchemaName() {
    return iDatabase.getPreferredSchemaName();
  }

  public void setPreferredSchemaName(String preferredSchemaName) {
    iDatabase.setPreferredSchemaName(preferredSchemaName);
  }

  public boolean supportsSequenceNoMaxValueOption() {
    return iDatabase.isSupportsSequenceNoMaxValueOption();
  }

  public boolean requiresCreateTablePrimaryKeyAppend() {
    return iDatabase.isRequiresCreateTablePrimaryKeyAppend();
  }

  public boolean requiresCastToVariousForIsNull() {
    return iDatabase.isRequiresCastToVariousForIsNull();
  }

  public boolean isDisplaySizeTwiceThePrecision() {
    return iDatabase.isDisplaySizeTwiceThePrecision();
  }

  public boolean supportsPreparedStatementMetadataRetrieval() {
    return iDatabase.isSupportsPreparedStatementMetadataRetrieval();
  }

  public boolean isSystemTable(String tableName) {
    return iDatabase.isSystemTable(tableName);
  }

  private boolean supportsNewLinesInSql() {
    return iDatabase.isSupportsNewLinesInSql();
  }

  public String getSqlListOfSchemas() {
    return iDatabase.getSqlListOfSchemas();
  }

  public int getMaxColumnsInIndex() {
    return iDatabase.getMaxColumnsInIndex();
  }

  public boolean supportsErrorHandlingOnBatchUpdates() {
    return iDatabase.IsSupportsErrorHandlingOnBatchUpdates();
  }

  /**
   * Get the SQL to insert a new empty unknown record in a dimension.
   *
   * @param schemaTable the schema-table name to insert into
   * @param keyField The key field
   * @param versionField the version field
   * @return the SQL to insert the unknown record into the SCD.
   */
  public String getSqlInsertAutoIncUnknownDimensionRow(
      String schemaTable, String keyField, String versionField) {
    return iDatabase.getSqlInsertAutoIncUnknownDimensionRow(schemaTable, keyField, versionField);
  }

  /**
   * @return true if this is a relational database you can explore. Return false for SAP, PALO, etc.
   */
  public boolean isExplorable() {
    return iDatabase.isExplorable();
  }

  /**
   * @return The SQL on this database to get a list of sequences.
   */
  public String getSqlListOfSequences() {
    return iDatabase.getSqlListOfSequences();
  }

  public String quoteSqlString(String string) {
    return iDatabase.quoteSqlString(string);
  }

  /**
   * @see IDatabase#generateColumnAlias(int, String)
   */
  public String generateColumnAlias(int columnIndex, String suggestedName) {
    return iDatabase.generateColumnAlias(columnIndex, suggestedName);
  }

  public boolean isMySqlVariant() {
    return iDatabase.isMySqlVariant();
  }

  public Object getValueFromResultSet(ResultSet rs, IValueMeta val, int i)
      throws HopDatabaseException {
    return iDatabase.getValueFromResultSet(rs, val, i);
  }

  /**
   * Marker used to determine if the DatabaseMeta should be allowed to be modified/saved. It does
   * NOT prevent object modification.
   *
   * @return
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Sets the marker used to determine if the DatabaseMeta should be allowed to be modified/saved.
   * Setting to true does NOT prevent object modification.
   */
  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public String getSequenceNoMaxValueOption() {
    return iDatabase.getSequenceNoMaxValueOption();
  }

  /**
   * @return true if the database supports autoGeneratedKeys
   */
  public boolean supportsAutoGeneratedKeys() {
    return iDatabase.isSupportsAutoGeneratedKeys();
  }

  /**
   * Customizes the IValueMeta defined in the base
   *
   * @return String the create table statement
   */
  public String getCreateTableStatement() {
    return iDatabase.getCreateTableStatement();
  }

  /**
   * Forms the drop table statement specific for a certain RDBMS.
   *
   * @param tableName Name of the table to drop
   * @return Drop table statement specific for the current database
   */
  public String getDropTableIfExistsStatement(String tableName) {
    return iDatabase.getDropTableIfExistsStatement(tableName);
  }

  /** For testing */
  protected ILogChannel getGeneralLogger() {
    return LogChannel.GENERAL;
  }

  /** For testing */
  protected IDatabase getDbInterface(String typeCode) throws HopDatabaseException {
    return getIDatabase(typeCode);
  }
}
