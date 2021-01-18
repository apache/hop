// CHECKSTYLE:FileLength:OFF
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Counter;
import org.apache.hop.core.Counters;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.DbCacheEntry;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Database handles the process of connecting to, reading from, writing to and updating databases.
 * The database specific parameters are defined in DatabaseInfo.
 *
 * @author Matt
 * @since 05-04-2003
 */
public class Database implements IVariables, ILoggingObject {
  private static final Class<?> PKG = Database.class; // For Translator

  private static final Map<String, Set<String>> registeredDrivers = new HashMap<>();

  private DatabaseMeta databaseMeta;

  private static final String DATA_SERVICES_PLUGIN_ID = "HopThin";

  private int rowlimit;
  private int commitsize;

  private Connection connection;

  private Statement selStmt;
  private PreparedStatement pstmt;
  private PreparedStatement prepStatementLookup;
  private PreparedStatement prepStatementUpdate;
  private PreparedStatement prepStatementInsert;
  private PreparedStatement pstmtSeq;
  private CallableStatement cstmt;

  // private ResultSetMetaData rsmd;
  private DatabaseMetaData dbmd;

  private IRowMeta rowMeta;

  private int written;

  private ILogChannel log;
  private ILoggingObject parentLoggingObject;
  private static final String[] TABLE_TYPES_TO_GET = {"TABLE", "VIEW"};
  private static final String TABLES_META_DATA_TABLE_NAME = "TABLE_NAME";

  /**
   * Number of times a connection was opened using this object. Only used in the context of a
   * database connection map
   */
  private volatile int opened;

  /** The copy is equal to opened at the time of creation. */
  private volatile int copy;

  private String connectionGroup;
  private String partitionId;

  private IVariables variables = new Variables();

  private LogLevel logLevel = DefaultLogLevel.getLogLevel();

  private String containerObjectId;

  private int nrExecutedCommits;

  private static List<IValueMeta> valueMetaPluginClasses;

  static {
    try {
      valueMetaPluginClasses = ValueMetaFactory.getValueMetaPluginClasses();
      Collections.sort(
          valueMetaPluginClasses,
          (o1, o2) -> {
            // Reverse the sort list
            return (Integer.valueOf(o1.getType()).compareTo(Integer.valueOf(o2.getType()))) * -1;
          });
    } catch (Exception e) {
      throw new RuntimeException("Unable to get list of instantiated value meta plugin classes", e);
    }
  }

  /**
   * Construct a new Database Connection
   *  @param parentObject The parent
   * @param variables
   * @param databaseMeta The Database Connection Info to construct the connection with.
   */
  public Database( ILoggingObject parentObject, IVariables variables, DatabaseMeta databaseMeta ) {
    this.parentLoggingObject = parentObject;
    this.variables = variables;
    this.databaseMeta = databaseMeta;

    log = new LogChannel(this, parentObject);
    this.containerObjectId = log.getContainerObjectId();
    this.logLevel = log.getLogLevel();
    if (parentObject != null) {
      log.setGatheringMetrics(parentObject.isGatheringMetrics());
    }

    pstmt = null;
    rowMeta = null;
    dbmd = null;

    rowlimit = 0;

    written = 0;

    opened = copy = 0;

    if (log.isDetailed()) {
      log.logDetailed("New database connection defined");
    }
  }

  /**
   * This implementation is NullPointerException subject, and may not follow fundamental equals
   * contract.
   *
   * <p>Databases equality is based on {@link DatabaseMeta} equality.
   */
  @Override
  public boolean equals(Object obj) {
    Database other = (Database) obj;
    return this.databaseMeta.equals(other.databaseMeta);
  }

  /**
   * Allows for the injection of a "life" connection, generated by a piece of software outside of
   * Hop.
   *
   * @param connection
   */
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  /** @return Returns the connection. */
  public Connection getConnection() {
    return connection;
  }

  /**
   * Set the maximum number of records to retrieve from a query.
   *
   * @param rows
   */
  public void setQueryLimit(int rows) {
    rowlimit = rows;
  }

  /** @return Returns the prepStatementInsert. */
  public PreparedStatement getPrepStatementInsert() {
    return prepStatementInsert;
  }

  /** @return Returns the prepStatementLookup. */
  public PreparedStatement getPrepStatementLookup() {
    return prepStatementLookup;
  }

  /** @return Returns the prepStatementUpdate. */
  public PreparedStatement getPrepStatementUpdate() {
    return prepStatementUpdate;
  }

  /**
   * Open the database connection.
   *
   * @throws HopDatabaseException if something went wrong.
   */
  public void connect() throws HopDatabaseException {
    connect(null);
  }

  /**
   * Open the database connection.
   *
   * @param partitionId the partition ID in the cluster to connect to.
   * @throws HopDatabaseException if something went wrong.
   */
  public void connect(String partitionId) throws HopDatabaseException {
    connect(null, partitionId);
  }

  public synchronized void connect(String group, String partitionId) throws HopDatabaseException {
    try {

      log.snap(Metrics.METRIC_DATABASE_CONNECT_START, databaseMeta.getName());

      // Before anything else, let's see if we already have a connection defined
      // for this group/partition!
      // The group is called after the thread-name of the pipeline or workflow
      // that is running
      // The name of that thread name is expected to be unique (it is in Hop)
      // So the deal is that if there is another thread using that, we go for
      // it.
      //
      if (!Utils.isEmpty(group)) {
        this.connectionGroup = group;
        this.partitionId = partitionId;

        // Try to find the connection for the group
        Database lookup =
            DatabaseConnectionMap.getInstance().getOrStoreIfAbsent(group, partitionId, this);
        if (lookup == null) {
          // There was no mapped value before
          lookup = this;
        }
        lookup.shareConnectionWith(partitionId, this);
      } else {
        // Proceed with a normal connect
        normalConnect(partitionId);
      }

      try {
        ExtensionPointHandler.callExtensionPoint(log, this, HopExtensionPoint.DatabaseConnected.id, this );
      } catch (HopException e) {
        throw new HopDatabaseException(e);
      }

    } finally {
      log.snap(Metrics.METRIC_DATABASE_CONNECT_STOP, databaseMeta.getName());
    }
  }

  private synchronized void shareConnectionWith(String partitionId, Database anotherDb)
      throws HopDatabaseException {
    // inside synchronized block we can increment 'opened' directly
    this.opened++;

    if (this.connection == null) {
      normalConnect(partitionId);
      this.copy = this.opened;

      // If we have a connection group or transaction ID, disable auto commit!
      //
      setAutoCommit(false);
    }

    anotherDb.connection = this.connection;
    anotherDb.copy = this.opened;
  }

  /**
   * Open the database connection. The algorithm is:
   *
   * <ol>
   *   <li>If <code>databaseMeta.isUsingConnectionPool()</code>, then the connection's datasource is
   *       looked up in the pool
   *   <li>otherwise, the connection is established via {@linkplain DriverManager}
   * </ol>
   *
   * @param partitionId the partition ID in the cluster to connect to.
   * @throws HopDatabaseException if something went wrong.
   */
  public void normalConnect(String partitionId) throws HopDatabaseException {
    if (databaseMeta == null) {
      throw new HopDatabaseException("No valid database connection defined!");
    }

    try {

      // Connect to the database
      //
      connectUsingClass(databaseMeta.getDriverClass(this), partitionId);

      // See if we need to execute extra SQL statements...
      //
      String sql = resolve(databaseMeta.getConnectSql());

      // only execute if the SQL is not empty, null and is not just a bunch of
      // spaces, tabs, CR etc.
      if (!Utils.isEmpty(sql) && !Const.onlySpaces(sql)) {
        execStatements(sql);
        if (log.isDetailed()) {
          log.logDetailed("Executed connect time SQL statements:" + Const.CR + sql);
        }
      }
    } catch (Exception e) {
      throw new HopDatabaseException("Error occurred while trying to connect to the database", e);
    }
  }

  /**
   * Connect using the correct classname
   *
   * @param classname for example "org.gjt.mm.mysql.Driver"
   * @return true if the connect was successful, false if something went wrong.
   */
  private void connectUsingClass(String classname, String partitionId) throws HopDatabaseException {
    // Install and load the jdbc Driver
    IPlugin plugin =
        PluginRegistry.getInstance()
            .getPlugin(DatabasePluginType.class, databaseMeta.getIDatabase());

    try {
      synchronized ( DriverManager.class) {
        ClassLoader classLoader = PluginRegistry.getInstance().getClassLoader(plugin);
        Class<?> driverClass = classLoader.loadClass(classname);

        // Only need DelegatingDriver for drivers not from our classloader
        if (driverClass.getClassLoader() != this.getClass().getClassLoader()) {
          String pluginId =
              PluginRegistry.getInstance()
                  .getPluginId(DatabasePluginType.class, databaseMeta.getIDatabase());
          Set<String> registeredDriversFromPlugin = registeredDrivers.get(pluginId);
          if (registeredDriversFromPlugin == null) {
            registeredDriversFromPlugin = new HashSet<>();
            registeredDrivers.put(pluginId, registeredDriversFromPlugin);
          }
          // Prevent registering multiple delegating drivers for same class, plugin
          if (!registeredDriversFromPlugin.contains(driverClass.getCanonicalName())) {
            DriverManager.registerDriver(new DelegatingDriver((Driver) driverClass.newInstance()));
            registeredDriversFromPlugin.add(driverClass.getCanonicalName());
          }
        } else {
          // Trigger static register block in driver class
          Class.forName(classname);
        }
      }
    } catch (NoClassDefFoundError | ClassNotFoundException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG,
              "Database.Exception.UnableToFindClassMissingDriver",
              classname,
              plugin.getName()),
          e);
    } catch (Exception e) {
      throw new HopDatabaseException("Exception while loading class", e);
    }

    try {
      String url = resolve(databaseMeta.getURL(this));

      String username = resolve(databaseMeta.getUsername());
      String password =
          Encr.decryptPasswordOptionallyEncrypted(
              resolve(databaseMeta.getPassword()));

      Properties properties = databaseMeta.getConnectionProperties(this);

      if (databaseMeta.supportsOptionsInURL()) {
        if (!Utils.isEmpty(username) || !Utils.isEmpty(password)) {
          // Allow for empty username with given password, in this case username must be given with
          // one variables
          properties.put("user", Const.NVL(username, " "));
          properties.put("password", Const.NVL(password, ""));
          if (databaseMeta.getIDatabase().isMsSqlServerNativeVariant()) {
            // Handle MSSQL Instance name. Would rather this was handled in the dialect
            // but cannot (without refactor) get to variablespace for variable substitution from
            // a BaseDatabaseMeta subclass.
            String instance = resolve(databaseMeta.getSqlServerInstance());
            if (!Utils.isEmpty(instance)) {
              url += ";instanceName=" + instance;
            }
          }
          connection = DriverManager.getConnection(url, properties);
        } else {
          // Perhaps the username is in the URL or no username is required...
          connection = DriverManager.getConnection(url, properties);
        }
      } else {
        if (!Utils.isEmpty(username)) {
          properties.put("user", username);
        }
        if (!Utils.isEmpty(password)) {
          properties.put("password", password);
        }

        connection = DriverManager.getConnection(url, properties);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Error connecting to database: (using class " + classname + ")", e);
    } catch (Throwable e) {
      throw new HopDatabaseException(
          "Error connecting to database: (using class " + classname + ")", e);
    }
  }

  /** Disconnect from the database and close all open prepared statements. */
  public synchronized void disconnect() {
    if (connection == null) {
      return; // Nothing to do...
    }
    try {
      if (connection.isClosed()) {
        return; // Nothing to do...
      }
    } catch (SQLException ex) {
      // cannot do anything about this but log it
      log.logError("Error checking closing connection:" + Const.CR + ex.getMessage());
      log.logError(Const.getStackTracker(ex));
    }

    if (pstmt != null) {
      try {
        pstmt.close();
      } catch (SQLException ex) {
        // cannot do anything about this but log it
        log.logError("Error closing statement:" + Const.CR + ex.getMessage());
        log.logError(Const.getStackTracker(ex));
      }
      pstmt = null;
    }
    if (prepStatementLookup != null) {
      try {
        prepStatementLookup.close();
      } catch (SQLException ex) {
        // cannot do anything about this but log it
        log.logError("Error closing lookup statement:" + Const.CR + ex.getMessage());
        log.logError(Const.getStackTracker(ex));
      }
      prepStatementLookup = null;
    }
    if (prepStatementInsert != null) {
      try {
        prepStatementInsert.close();
      } catch (SQLException ex) {
        // cannot do anything about this but log it
        log.logError("Error closing insert statement:" + Const.CR + ex.getMessage());
        log.logError(Const.getStackTracker(ex));
      }
      prepStatementInsert = null;
    }
    if (prepStatementUpdate != null) {
      try {
        prepStatementUpdate.close();
      } catch (SQLException ex) {
        // cannot do anything about this but log it
        log.logError("Error closing update statement:" + Const.CR + ex.getMessage());
        log.logError(Const.getStackTracker(ex));
      }
      prepStatementUpdate = null;
    }
    if (pstmtSeq != null) {
      try {
        pstmtSeq.close();
      } catch (SQLException ex) {
        // cannot do anything about this but log it
        log.logError("Error closing seq statement:" + Const.CR + ex.getMessage());
        log.logError(Const.getStackTracker(ex));
      }
      pstmtSeq = null;
    }

    // See if there are other transforms using this connection in a connection
    // group.
    // If so, we will hold commit & connection close until then.
    //
    if (!Utils.isEmpty(connectionGroup)) {
      return;
    } else {
      if (!isAutoCommit()) {
        // Do we really still need this commit??
        try {
          commit();
        } catch (HopDatabaseException ex) {
          // cannot do anything about this but log it
          log.logError("Error committing:" + Const.CR + ex.getMessage());
          log.logError(Const.getStackTracker(ex));
        }
      }
    }
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, this, HopExtensionPoint.DatabaseDisconnected.id, this );
    } catch (HopException e) {
      log.logError("Error disconnecting from database:" + Const.CR + e.getMessage());
      log.logError(Const.getStackTracker(e));
    } finally {
      // Always close the connection, irrespective of what happens above...
      try {
        closeConnectionOnly();
      } catch (
          HopDatabaseException ignoredKde) { // The only exception thrown from closeConnectionOnly()
        // cannot do anything about this but log it
        log.logError(
            "Error disconnecting from database - closeConnectionOnly failed:"
                + Const.CR
                + ignoredKde.getMessage());
        log.logError(Const.getStackTracker(ignoredKde));
      }
    }
  }

  /**
   * Only for unique connections usage, typically you use disconnect() to disconnect() from the
   * database.
   *
   * @throws HopDatabaseException in case there is an error during connection close.
   */
  public synchronized void closeConnectionOnly() throws HopDatabaseException {
    try {
      if (connection != null) {
        connection.close();
        connection = null;
      }

      if (log.isDetailed()) {
        log.logDetailed("Connection to database closed!");
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Error disconnecting from database '" + toString() + "'", e);
    }
  }

  /**
   * Cancel the open/running queries on the database connection
   *
   * @throws HopDatabaseException
   */
  public void cancelQuery() throws HopDatabaseException {
    // Canceling statements only if we're not streaming results on MySQL with
    // the v3 driver
    //
    if (databaseMeta.isMySqlVariant()
        && databaseMeta.isStreamingResults()
        && getDatabaseMetaData().getDriverMajorVersion() == 3) {
      return;
    }

    cancelStatement(pstmt);
    cancelStatement(selStmt);
  }

  /**
   * Cancel an open/running SQL statement
   *
   * @param statement the statement to cancel
   * @throws HopDatabaseException
   */
  public void cancelStatement(Statement statement) throws HopDatabaseException {
    try {
      if (statement != null) {
        statement.cancel();
      }
      if (log.isDebug()) {
        log.logDebug("Statement canceled!");
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException("Error cancelling statement", ex);
    }
  }

  /**
   * Specify after how many rows a commit needs to occur when inserting or updating values.
   *
   * @param commsize The number of rows to wait before doing a commit on the connection.
   */
  public void setCommit(int commsize) {
    commitsize = commsize;
    String onOff = (commitsize <= 0 ? "on" : "off");
    try {
      connection.setAutoCommit(commitsize <= 0);
      if (log.isDetailed()) {
        log.logDetailed("Auto commit " + onOff);
      }
    } catch (Exception e) {
      if (log.isDebug()) {
        log.logDebug(
            "Can't turn auto commit "
                + onOff
                + Const.CR
                + Const.getSimpleStackTrace(e)
                + Const.CR
                + Const.getStackTracker(e));
      }
    }
  }

  public void setAutoCommit(boolean useAutoCommit) throws HopDatabaseException {
    try {
      connection.setAutoCommit(useAutoCommit);
    } catch (SQLException e) {
      if (useAutoCommit) {
        throw new HopDatabaseException(
            BaseMessages.getString(PKG, "Database.Exception.UnableToEnableAutoCommit", toString()));
      } else {
        throw new HopDatabaseException(
            BaseMessages.getString(
                PKG, "Database.Exception.UnableToDisableAutoCommit", toString()));
      }
    }
  }

  /** Perform a commit the connection if this is supported by the database */
  public void commit() throws HopDatabaseException {
    commit(false);
  }

  public void commit(boolean force) throws HopDatabaseException {
    try {
      // Don't do the commit, wait until the end of the pipeline.
      // When the last database copy (opened counter) is about to be closed, we
      // do a commit
      // There is one catch, we need to catch the rollback
      // The pipeline will stop everything and then we'll do the rollback.
      // The flag is in "performRollback", private only
      //
      if (!Utils.isEmpty(connectionGroup) && !force) {
        return;
      }
      if (getDatabaseMetaData().supportsTransactions()) {
        if (log.isDebug()) {
          log.logDebug("Commit on database connection [" + toString() + "]");
        }
        connection.commit();
        nrExecutedCommits++;
      } else {
        if (log.isDetailed()) {
          log.logDetailed("No commit possible on database connection [" + toString() + "]");
        }
      }
    } catch (Exception e) {
      if (databaseMeta.supportsEmptyTransactions()) {
        throw new HopDatabaseException("Error comitting connection", e);
      }
    }
  }

  /**
   * this is a copy of {@link #commit(boolean)} - but delegates exception handling to caller. Can be
   * possibly be removed in future.
   *
   * @param force
   * @throws HopDatabaseException
   * @throws SQLException
   */
  @Deprecated
  private void commitInternal(boolean force) throws HopDatabaseException, SQLException {
    if (!Utils.isEmpty(connectionGroup) && !force) {
      return;
    }
    if (getDatabaseMetaData().supportsTransactions()) {
      if (log.isDebug()) {
        log.logDebug("Commit on database connection [" + toString() + "]");
      }
      connection.commit();
      nrExecutedCommits++;
    } else {
      if (log.isDetailed()) {
        log.logDetailed("No commit possible on database connection [" + toString() + "]");
      }
    }
  }

  public void rollback() throws HopDatabaseException {
    rollback(false);
  }

  public void rollback(boolean force) throws HopDatabaseException {
    try {
      if (!Utils.isEmpty(connectionGroup) && !force) {
        return; // Will be handled by Pipeline --> endProcessing()
      }
      if (getDatabaseMetaData().supportsTransactions()) {
        if (connection != null) {
          if (log.isDebug()) {
            log.logDebug("Rollback on database connection [" + toString() + "]");
          }
          connection.rollback();
        }
      } else {
        if (log.isDetailed()) {
          log.logDetailed("No rollback possible on database connection [" + toString() + "]");
        }
      }

    } catch (SQLException e) {
      throw new HopDatabaseException("Error performing rollback on connection", e);
    }
  }

  /**
   * Prepare inserting values into a table, using the fields & values in a Row
   *
   * @param rowMeta The row metadata to determine which values need to be inserted
   * @param tableName The name of the table in which we want to insert rows
   * @throws HopDatabaseException if something went wrong.
   */
  public void prepareInsert(IRowMeta rowMeta, String tableName) throws HopDatabaseException {
    prepareInsert(rowMeta, null, tableName);
  }

  /**
   * Prepare inserting values into a table, using the fields & values in a Row
   *
   * @param rowMeta The metadata row to determine which values need to be inserted
   * @param schemaName The name of the schema in which we want to insert rows
   * @param tableName The name of the table in which we want to insert rows
   * @throws HopDatabaseException if something went wrong.
   */
  public void prepareInsert(IRowMeta rowMeta, String schemaName, String tableName)
      throws HopDatabaseException {
    if (rowMeta.size() == 0) {
      throw new HopDatabaseException("No fields in row, can't insert!");
    }

    String ins = getInsertStatement(schemaName, tableName, rowMeta);

    if (log.isDetailed()) {
      log.logDetailed("Preparing statement: " + Const.CR + ins);
    }
    prepStatementInsert = prepareSql(ins);
  }

  /**
   * Prepare a statement to be executed on the database. (does not return generated keys)
   *
   * @param sql The SQL to be prepared
   * @return The PreparedStatement object.
   * @throws HopDatabaseException
   */
  public PreparedStatement prepareSql(String sql) throws HopDatabaseException {
    return prepareSql(sql, false);
  }

  /**
   * Prepare a statement to be executed on the database.
   *
   * @param sql The SQL to be prepared
   * @param returnKeys set to true if you want to return generated keys from an insert statement
   * @return The PreparedStatement object.
   * @throws HopDatabaseException
   */
  public PreparedStatement prepareSql(String sql, boolean returnKeys) throws HopDatabaseException {
    IDatabase iDatabase = databaseMeta.getIDatabase();
    boolean supportsAutoGeneratedKeys = iDatabase.supportsAutoGeneratedKeys();

    try {
      if (returnKeys && supportsAutoGeneratedKeys) {
        return connection.prepareStatement(
            databaseMeta.stripCR(sql), Statement.RETURN_GENERATED_KEYS);
      } else {
        return connection.prepareStatement(databaseMeta.stripCR(sql));
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException("Couldn't prepare statement:" + Const.CR + sql, ex);
    }
  }

  public void closeLookup() throws HopDatabaseException {
    closePreparedStatement(pstmt);
    pstmt = null;
  }

  public void closePreparedStatement(PreparedStatement ps) throws HopDatabaseException {
    if (ps != null) {
      try {
        ps.close();
      } catch (SQLException e) {
        throw new HopDatabaseException("Error closing prepared statement", e);
      }
    }
  }

  public void closeInsert() throws HopDatabaseException {
    if (prepStatementInsert != null) {
      try {
        prepStatementInsert.close();
        prepStatementInsert = null;
      } catch (SQLException e) {
        throw new HopDatabaseException("Error closing insert prepared statement.", e);
      }
    }
  }

  public void closeUpdate() throws HopDatabaseException {
    if (prepStatementUpdate != null) {
      try {
        prepStatementUpdate.close();
        prepStatementUpdate = null;
      } catch (SQLException e) {
        throw new HopDatabaseException("Error closing update prepared statement.", e);
      }
    }
  }

  public void setValues(IRowMeta rowMeta, Object[] data) throws HopDatabaseException {
    setValues(rowMeta, data, pstmt);
  }

  public void setValues(RowMetaAndData row) throws HopDatabaseException {
    setValues(row.getRowMeta(), row.getData());
  }

  public void setValuesInsert(IRowMeta rowMeta, Object[] data) throws HopDatabaseException {
    setValues(rowMeta, data, prepStatementInsert);
  }

  public void setValuesInsert(RowMetaAndData row) throws HopDatabaseException {
    setValues(row.getRowMeta(), row.getData(), prepStatementInsert);
  }

  public void setValuesUpdate(IRowMeta rowMeta, Object[] data) throws HopDatabaseException {
    setValues(rowMeta, data, prepStatementUpdate);
  }

  public void setValuesLookup(IRowMeta rowMeta, Object[] data) throws HopDatabaseException {
    setValues(rowMeta, data, prepStatementLookup);
  }

  public void setProcValues(
      IRowMeta rowMeta, Object[] data, int[] argnrs, String[] argdir, boolean result)
      throws HopDatabaseException {
    int pos;

    if (result) {
      pos = 2;
    } else {
      pos = 1;
    }

    for (int i = 0; i < argnrs.length; i++) {
      if (argdir[i].equalsIgnoreCase("IN") || argdir[i].equalsIgnoreCase("INOUT")) {
        IValueMeta valueMeta = rowMeta.getValueMeta(argnrs[i]);
        Object value = data[argnrs[i]];

        setValue(cstmt, valueMeta, value, pos);
        pos++;
      } else {
        pos++; // next parameter when OUT
      }
    }
  }

  public void setValue(PreparedStatement ps, IValueMeta v, Object object, int pos)
      throws HopDatabaseException {

    v.setPreparedStatementValue(databaseMeta, ps, pos, object);
  }

  public void setValues(RowMetaAndData row, PreparedStatement ps) throws HopDatabaseException {
    setValues(row.getRowMeta(), row.getData(), ps);
  }

  public void setValues(IRowMeta rowMeta, Object[] data, PreparedStatement ps)
      throws HopDatabaseException {
    // now set the values in the row!
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta v = rowMeta.getValueMeta(i);
      Object object = data[i];

      try {
        setValue(ps, v, object, i + 1);
      } catch (HopDatabaseException e) {
        throw new HopDatabaseException("offending row : " + rowMeta, e);
      }
    }
  }

  /**
   * Sets the values of the preparedStatement pstmt.
   *
   * @param rowMeta
   * @param data
   */
  public void setValues(
      IRowMeta rowMeta, Object[] data, PreparedStatement ps, int ignoreThisValueIndex)
      throws HopDatabaseException {
    // now set the values in the row!
    int index = 0;
    for (int i = 0; i < rowMeta.size(); i++) {
      if (i != ignoreThisValueIndex) {
        IValueMeta v = rowMeta.getValueMeta(i);
        Object object = data[i];

        try {
          setValue(ps, v, object, index + 1);
          index++;
        } catch (HopDatabaseException e) {
          throw new HopDatabaseException("offending row : " + rowMeta, e);
        }
      }
    }
  }

  /**
   * @param ps The prepared insert statement to use
   * @return The generated keys in auto-increment fields
   * @throws HopDatabaseException in case something goes wrong retrieving the keys.
   */
  public RowMetaAndData getGeneratedKeys(PreparedStatement ps) throws HopDatabaseException {
    ResultSet keys = null;
    try {
      keys = ps.getGeneratedKeys(); // 1 row of keys
      ResultSetMetaData resultSetMetaData = keys.getMetaData();
      if (resultSetMetaData == null) {
        resultSetMetaData = ps.getMetaData();
      }
      IRowMeta rowMeta;
      if (resultSetMetaData == null) {
        rowMeta = new RowMeta();
        rowMeta.addValueMeta(new ValueMetaInteger("ai-key"));
      } else {
        rowMeta = getRowInfo(resultSetMetaData, false, false);
      }

      return new RowMetaAndData(rowMeta, getRow(keys, resultSetMetaData, rowMeta));
    } catch (Exception ex) {
      throw new HopDatabaseException("Unable to retrieve key(s) from auto-increment field(s)", ex);
    } finally {
      if (keys != null) {
        try {
          keys.close();
        } catch (SQLException e) {
          throw new HopDatabaseException("Unable to close resultset of auto-generated keys", e);
        }
      }
    }
  }

  public Long getNextSequenceValue(String sequenceName, String keyfield)
      throws HopDatabaseException {
    return getNextSequenceValue(null, sequenceName, keyfield);
  }

  public Long getNextSequenceValue(String schemaName, String sequenceName, String keyfield)
      throws HopDatabaseException {
    Long retval = null;

    String schemaSequence =
        databaseMeta.getQuotedSchemaTableCombination(this, schemaName, sequenceName);

    try {
      if (pstmtSeq == null) {
        pstmtSeq =
            connection.prepareStatement(
                databaseMeta.getSeqNextvalSql(databaseMeta.stripCR(schemaSequence)));
      }
      ResultSet rs = null;
      try {
        rs = pstmtSeq.executeQuery();
        if (rs.next()) {
          retval = Long.valueOf(rs.getLong(1));
        }
      } finally {
        if (rs != null) {
          rs.close();
        }
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Unable to get next value for sequence : " + schemaSequence, ex);
    }

    return retval;
  }

  public void insertRow(String tableName, IRowMeta fields, Object[] data)
      throws HopDatabaseException {
    insertRow(null, tableName, fields, data);
  }

  public void insertRow(String schemaName, String tableName, IRowMeta fields, Object[] data)
      throws HopDatabaseException {
    prepareInsert(fields, schemaName, tableName);
    setValuesInsert(fields, data);
    insertRow();
    closeInsert();
  }

  public String getInsertStatement(String tableName, IRowMeta fields) {
    return getInsertStatement(null, tableName, fields);
  }

  public String getInsertStatement(String schemaName, String tableName, IRowMeta fields) {
    StringBuilder ins = new StringBuilder(128);

    String schemaTable = databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);
    ins.append("INSERT INTO ").append(schemaTable).append(" (");

    // now add the names in the row:
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        ins.append(", ");
      }
      String name = fields.getValueMeta(i).getName();
      ins.append(databaseMeta.quoteField(name));
    }
    ins.append(") VALUES (");

    // Add placeholders...
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        ins.append(", ");
      }
      ins.append(" ?");
    }
    ins.append(')');

    return ins.toString();
  }

  public void insertRow() throws HopDatabaseException {
    insertRow(prepStatementInsert);
  }

  public void insertRow(boolean batch) throws HopDatabaseException {
    insertRow(prepStatementInsert, batch);
  }

  public void updateRow() throws HopDatabaseException {
    insertRow(prepStatementUpdate);
  }

  public void insertRow(PreparedStatement ps) throws HopDatabaseException {
    insertRow(ps, false);
  }

  /**
   * Insert a row into the database using a prepared statement that has all values set.
   *
   * @param ps The prepared statement
   * @param batch True if you want to use batch inserts (size = commit size)
   * @return true if the rows are safe: if batch of rows was sent to the database OR if a commit was
   *     done.
   * @throws HopDatabaseException
   */
  public boolean insertRow(PreparedStatement ps, boolean batch) throws HopDatabaseException {
    return insertRow(ps, batch, true);
  }

  public boolean getUseBatchInsert(boolean batch) throws HopDatabaseException {
    try {
      return batch
          && getDatabaseMetaData().supportsBatchUpdates()
          && databaseMeta.supportsBatchUpdates()
          && Utils.isEmpty(connectionGroup);
    } catch (SQLException e) {
      throw createHopDatabaseBatchException("Error determining whether to use batch", e);
    }
  }

  /**
   * Insert a row into the database using a prepared statement that has all values set.
   *
   * @param ps The prepared statement
   * @param batch True if you want to use batch inserts (size = commit size)
   * @param handleCommit True if you want to handle the commit here after the commit size (False
   *     e.g. in case the transform handles this, see TableOutput)
   * @return true if the rows are safe: if batch of rows was sent to the database OR if a commit was
   *     done.
   * @throws HopDatabaseException
   */
  public boolean insertRow(PreparedStatement ps, boolean batch, boolean handleCommit)
      throws HopDatabaseException {
    String debug = "insertRow start";
    boolean rowsAreSafe = false;
    boolean isBatchUpdate = false;

    try {
      // Unique connections and Batch inserts don't mix when you want to roll
      // back on certain databases.
      // That's why we disable the batch insert in that case.
      //
      boolean useBatchInsert = getUseBatchInsert(batch);

      //
      // Add support for batch inserts...
      //
      if (!isAutoCommit()) {
        if (useBatchInsert) {
          debug = "insertRow add batch";
          ps.addBatch(); // Add the batch, but don't forget to run the batch
        } else {
          debug = "insertRow exec update";
          ps.executeUpdate();
        }
      } else {
        ps.executeUpdate();
      }

      written++;

      if (handleCommit) { // some transforms handle the commit themselves (see e.g.
        // TableOutput transform)
        if (!isAutoCommit() && (written % commitsize) == 0) {
          if (useBatchInsert) {
            isBatchUpdate = true;
            debug = "insertRow executeBatch commit";
            ps.executeBatch();
            commit();
            ps.clearBatch();
          } else {
            debug = "insertRow normal commit";
            commit();
          }
          written = 0;
          rowsAreSafe = true;
        }
      }

      return rowsAreSafe;
    } catch (BatchUpdateException ex) {
      throw createHopDatabaseBatchException("Error updating batch", ex);
    } catch (SQLException ex) {
      if (isBatchUpdate) {
        throw createHopDatabaseBatchException("Error updating batch", ex);
      } else {
        throw new HopDatabaseException("Error inserting/updating row", ex);
      }
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Unexpected error inserting/updating row in part [" + debug + "]", e);
    }
  }

  /**
   * Clears batch of insert prepared statement
   *
   * @throws HopDatabaseException
   * @deprecated
   */
  @Deprecated
  public void clearInsertBatch() throws HopDatabaseException {
    clearBatch(prepStatementInsert);
  }

  public void clearBatch(PreparedStatement preparedStatement) throws HopDatabaseException {
    try {
      preparedStatement.clearBatch();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to clear batch for prepared statement", e);
    }
  }

  public void executeAndClearBatch(PreparedStatement preparedStatement)
      throws HopDatabaseException {
    try {
      if (written > 0 && getDatabaseMetaData().supportsBatchUpdates()) {
        preparedStatement.executeBatch();
      }

      written = 0;
      preparedStatement.clearBatch();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to clear batch for prepared statement", e);
    }
  }

  public void insertFinished(boolean batch) throws HopDatabaseException {
    insertFinished(prepStatementInsert, batch);
    prepStatementInsert = null;
  }

  /**
   * Close the passed prepared statement. This object's "written" property is passed to the method
   * that does the execute and commit.
   *
   * @param ps
   * @param batch
   * @throws HopDatabaseException
   */
  public void emptyAndCommit(PreparedStatement ps, boolean batch) throws HopDatabaseException {
    emptyAndCommit(ps, batch, written);
  }

  /**
   * Close the prepared statement of the insert statement.
   *
   * @param ps The prepared statement to empty and close.
   * @param batch true if you are using batch processing
   * @param batchCounter The number of rows on the batch queue
   * @throws HopDatabaseException
   */
  public void emptyAndCommit(PreparedStatement ps, boolean batch, int batchCounter)
      throws HopDatabaseException {
    boolean isBatchUpdate = false;
    try {
      if (ps != null) {
        if (!isAutoCommit()) {
          // Execute the batch or just perform a commit.
          if (batch && getDatabaseMetaData().supportsBatchUpdates() && batchCounter > 0) {
            // The problem with the batch counters is that you can't just
            // execute the current batch.
            // Certain databases have a problem if you execute the batch and if
            // there are no statements in it.
            // You can't just catch the exception either because you would have
            // to roll back on certain databases before you can then continue to
            // do anything.
            // That leaves the task of keeping track of the number of rows up to
            // our responsibility.
            isBatchUpdate = true;
            ps.executeBatch();
            commit();
            ps.clearBatch();
          } else {
            commit();
          }
        }

        // Let's not forget to close the prepared statement.
        //
        ps.close();
      }
    } catch (BatchUpdateException ex) {
      throw createHopDatabaseBatchException("Error updating batch", ex);
    } catch (SQLException ex) {
      if (isBatchUpdate) {
        throw createHopDatabaseBatchException("Error updating batch", ex);
      } else {
        throw new HopDatabaseException("Unable to empty ps and commit connection.", ex);
      }
    }
  }

  public static HopDatabaseBatchException createHopDatabaseBatchException(
      String message, SQLException ex) {
    HopDatabaseBatchException kdbe = new HopDatabaseBatchException(message, ex);
    if (ex instanceof BatchUpdateException) {
      kdbe.setUpdateCounts(((BatchUpdateException) ex).getUpdateCounts());
    } else {
      // Null update count forces rollback of batch
      kdbe.setUpdateCounts(null);
    }
    List<Exception> exceptions = new ArrayList<>();
    SQLException nextException = ex.getNextException();
    SQLException oldException = null;

    // This construction is specifically done for some JDBC drivers, these
    // drivers
    // always return the same exception on getNextException() (and thus go
    // into an infinite loop).
    // So it's not "equals" but != (comments from Sven Boden).
    while ((nextException != null) && (oldException != nextException)) {
      exceptions.add(nextException);
      oldException = nextException;
      nextException = nextException.getNextException();
    }
    kdbe.setExceptionsList(exceptions);
    return kdbe;
  }

  /**
   * Close the prepared statement of the insert statement.
   *
   * @param ps The prepared statement to empty and close.
   * @param batch true if you are using batch processing (typically true for this method)
   * @throws HopDatabaseException
   * @deprecated use emptyAndCommit() instead (pass in the number of rows left in the batch)
   */
  @Deprecated
  public void insertFinished(PreparedStatement ps, boolean batch) throws HopDatabaseException {
    boolean isBatchUpdate = false;
    try {
      if (ps != null) {
        if (!isAutoCommit()) {
          // Execute the batch or just perform a commit.
          if (batch && getDatabaseMetaData().supportsBatchUpdates()) {
            // The problem with the batch counters is that you can't just
            // execute the current batch.
            // Certain databases have a problem if you execute the batch and if
            // there are no statements in it.
            // You can't just catch the exception either because you would have
            // to roll back on certain databases before you can then continue to
            // do anything.
            // That leaves the task of keeping track of the number of rows up to
            // our responsibility.
            isBatchUpdate = true;
            ps.executeBatch();
            commit();
          } else {
            commit();
          }
        }

        // Let's not forget to close the prepared statement.
        //
        ps.close();
      }
    } catch (BatchUpdateException ex) {
      throw createHopDatabaseBatchException("Error updating batch", ex);
    } catch (SQLException ex) {
      if (isBatchUpdate) {
        throw createHopDatabaseBatchException("Error updating batch", ex);
      } else {
        throw new HopDatabaseException(
            "Unable to commit connection after having inserted rows.", ex);
      }
    }
  }

  /**
   * Execute an SQL statement on the database connection (has to be open)
   *
   * @param sql The SQL to execute
   * @return a Result object indicating the number of lines read, deleted, inserted, updated, ...
   * @throws HopDatabaseException in case anything goes wrong.
   */
  public Result execStatement(String sql) throws HopDatabaseException {
    return execStatement(sql, null, null);
  }

  public Result execStatement(String rawsql, IRowMeta params, Object[] data)
      throws HopDatabaseException {
    Result result = new Result();

    // Replace existing code with a class that removes comments from the raw
    // SQL.
    // The SqlCommentScrubber respects single-quoted strings, so if a
    // double-dash or a multiline comment appears
    // in a single-quoted string, it will be treated as a string instead of
    // comments.
    String sql = databaseMeta.getIDatabase().createSqlScriptParser().removeComments(rawsql).trim();
    try {
      boolean resultSet;
      int count;
      if (params != null) {
        PreparedStatement prepStmt = connection.prepareStatement(databaseMeta.stripCR(sql));
        setValues(params, data, prepStmt); // set the parameters!
        resultSet = prepStmt.execute();
        count = prepStmt.getUpdateCount();
        prepStmt.close();
      } else {
        String sqlStripped = databaseMeta.stripCR(sql);
        // log.logDetailed("Executing SQL Statement: ["+sqlStripped+"]");
        Statement stmt = connection.createStatement();
        resultSet = stmt.execute(sqlStripped);
        count = stmt.getUpdateCount();
        stmt.close();
      }
      String upperSql = sql.toUpperCase();
      if (!resultSet) {
        // if the result is a resultset, we don't do anything with it!
        // You should have called something else!
        // log.logDetailed("What to do with ResultSet??? (count="+count+")");
        if (count > 0) {
          if (upperSql.startsWith("INSERT")) {
            result.setNrLinesOutput(count);
          } else if (upperSql.startsWith("UPDATE")) {
            result.setNrLinesUpdated(count);
          } else if (upperSql.startsWith("DELETE")) {
            result.setNrLinesDeleted(count);
          }
        }
      }

      // See if a cache needs to be cleared...
      if (upperSql.startsWith("ALTER TABLE")
          || upperSql.startsWith("DROP TABLE")
          || upperSql.startsWith("CREATE TABLE")) {
        DbCache.getInstance().clear(databaseMeta.getName());
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException("Couldn't execute SQL: " + sql + Const.CR, ex);
    } catch (Exception e) {
      throw new HopDatabaseException("Unexpected error executing SQL: " + Const.CR, e);
    }

    return result;
  }

  /**
   * Execute a series of SQL statements, separated by ;
   *
   * <p>We are already connected...
   *
   * <p>Multiple statements have to be split into parts We use the ";" to separate statements...
   *
   * <p>We keep the results in Result object from Workflows
   *
   * @param script The SQL script to be execute
   * @return A result with counts of the number or records updates, inserted, deleted or read.
   * @throws HopDatabaseException In case an error occurs
   */
  public Result execStatements(String script) throws HopDatabaseException {
    return execStatements(script, null, null);
  }

  /**
   * Execute a series of SQL statements, separated by ;
   *
   * <p>We are already connected...
   *
   * <p>Multiple statements have to be split into parts We use the ";" to separate statements...
   *
   * <p>We keep the results in Result object from Workflows
   *
   * @param script The SQL script to be execute
   * @param params Parameters Meta
   * @param data Parameters value
   * @return A result with counts of the number or records updates, inserted, deleted or read.
   * @throws HopDatabaseException In case an error occurs
   */
  public Result execStatements(String script, IRowMeta params, Object[] data)
      throws HopDatabaseException {
    Result result = new Result();

    SqlScriptParser sqlScriptParser = databaseMeta.getIDatabase().createSqlScriptParser();
    List<String> statements = sqlScriptParser.split(script);
    int nrstats = 0;

    if (statements != null) {
      for (String stat : statements) {
        // Deleting all the single-line and multi-line comments from the string
        stat = sqlScriptParser.removeComments(stat);

        if (!Const.onlySpaces(stat)) {
          String sql = Const.trim(stat);
          if (sql.toUpperCase().startsWith("SELECT")) {
            // A Query
            if (log.isDetailed()) {
              log.logDetailed("launch SELECT statement: " + Const.CR + sql);
            }

            nrstats++;
            ResultSet rs = null;
            try {
              rs = openQuery(sql, params, data);
              if (rs != null) {
                Object[] row = getRow(rs);
                while (row != null) {
                  result.setNrLinesRead(result.getNrLinesRead() + 1);
                  if (log.isDetailed()) {
                    log.logDetailed(rowMeta.getString(row));
                  }
                  row = getRow(rs);
                }

              } else {
                if (log.isDebug()) {
                  log.logDebug("Error executing query: " + Const.CR + sql);
                }
              }
            } catch (HopValueException e) {
              throw new HopDatabaseException(e); // just pass the error
              // upwards.
            } finally {
              try {
                if (rs != null) {
                  rs.close();
                }
              } catch (SQLException ex) {
                if (log.isDebug()) {
                  log.logDebug("Error closing query: " + Const.CR + sql);
                }
              }
            }
          } else {
            // any kind of statement
            if (log.isDetailed()) {
              log.logDetailed("launch DDL statement: " + Const.CR + sql);
            }

            // A DDL statement
            nrstats++;
            Result res = execStatement(sql, params, data);
            result.add(res);
          }
        }
      }
    }

    if (log.isDetailed()) {
      log.logDetailed(nrstats + " statement" + (nrstats == 1 ? "" : "s") + " executed");
    }

    return result;
  }

  public ResultSet openQuery(String sql) throws HopDatabaseException {
    return openQuery(sql, null, null);
  }

  /**
   * Open a query on the database with a set of parameters stored in a Hop Row
   *
   * @param sql The SQL to launch with question marks (?) as placeholders for the parameters
   * @param params The parameters or null if no parameters are used.
   * @return A JDBC ResultSet
   * @throws HopDatabaseException when something goes wrong with the query.
   * @data the parameter data to open the query with
   */
  public ResultSet openQuery(String sql, IRowMeta params, Object[] data)
      throws HopDatabaseException {
    return openQuery(sql, params, data, ResultSet.FETCH_FORWARD);
  }

  public ResultSet openQuery(String sql, IRowMeta params, Object[] data, int fetchMode)
      throws HopDatabaseException {
    return openQuery(sql, params, data, fetchMode, false);
  }

  public ResultSet openQuery(
      String sql, IRowMeta params, Object[] data, int fetchMode, boolean lazyConversion)
      throws HopDatabaseException {
    ResultSet res;

    // Create a Statement
    try {
      log.snap(Metrics.METRIC_DATABASE_OPEN_QUERY_START, databaseMeta.getName());
      if (params != null) {
        log.snap(Metrics.METRIC_DATABASE_PREPARE_SQL_START, databaseMeta.getName());
        pstmt =
            connection.prepareStatement(
                databaseMeta.stripCR(sql), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        log.snap(Metrics.METRIC_DATABASE_PREPARE_SQL_STOP, databaseMeta.getName());

        log.snap(Metrics.METRIC_DATABASE_SQL_VALUES_START, databaseMeta.getName());
        setValues(params, data); // set the dates etc!
        log.snap(Metrics.METRIC_DATABASE_SQL_VALUES_STOP, databaseMeta.getName());

        if (canWeSetFetchSize(pstmt)) {
          int maxRows = pstmt.getMaxRows();
          int fs = Const.FETCH_SIZE <= maxRows ? maxRows : Const.FETCH_SIZE;
          if (databaseMeta.isMySqlVariant()) {
            setMysqlFetchSize(pstmt, fs, maxRows);
          } else {
            pstmt.setFetchSize(fs);
          }

          pstmt.setFetchDirection(fetchMode);
        }

        if (rowlimit > 0 && databaseMeta.supportsSetMaxRows()) {
          pstmt.setMaxRows(rowlimit);
        }

        log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_START, databaseMeta.getName());
        res = pstmt.executeQuery();
        log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_STOP, databaseMeta.getName());
      } else {
        log.snap(Metrics.METRIC_DATABASE_CREATE_SQL_START, databaseMeta.getName());
        selStmt = connection.createStatement();
        log.snap(Metrics.METRIC_DATABASE_CREATE_SQL_STOP, databaseMeta.getName());
        if (canWeSetFetchSize(selStmt)) {
          int fs =
              Const.FETCH_SIZE <= selStmt.getMaxRows() ? selStmt.getMaxRows() : Const.FETCH_SIZE;
          if (databaseMeta.getIDatabase().isMySqlVariant() && databaseMeta.isStreamingResults()) {
            selStmt.setFetchSize(Integer.MIN_VALUE);
          } else {
            selStmt.setFetchSize(fs);
          }
          selStmt.setFetchDirection(fetchMode);
        }
        if (rowlimit > 0 && databaseMeta.supportsSetMaxRows()) {
          selStmt.setMaxRows(rowlimit);
        }

        log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_START, databaseMeta.getName());
        res = selStmt.executeQuery(databaseMeta.stripCR(sql));
        log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_STOP, databaseMeta.getName());
      }

      // MySQL Hack only. It seems too much for the cursor type of operation on
      // MySQL, to have another cursor opened
      // to get the length of a String field. So, on MySQL, we ingore the length
      // of Strings in result rows.
      //
      rowMeta = getRowInfo(res.getMetaData(), databaseMeta.isMySqlVariant(), lazyConversion);
    } catch (SQLException ex) {
      throw new HopDatabaseException("An error occurred executing SQL: " + Const.CR + sql, ex);
    } catch (Exception e) {
      throw new HopDatabaseException("An error occurred executing SQL:" + Const.CR + sql, e);
    } finally {
      log.snap(Metrics.METRIC_DATABASE_OPEN_QUERY_STOP, databaseMeta.getName());
    }

    return res;
  }

  private boolean canWeSetFetchSize(Statement statement) throws SQLException {
    return databaseMeta.isFetchSizeSupported()
        && (statement.getMaxRows() > 0
            || databaseMeta.getIDatabase().isPostgresVariant()
            || (databaseMeta.isMySqlVariant() && databaseMeta.isStreamingResults()));
  }

  public ResultSet openQuery(PreparedStatement ps, IRowMeta params, Object[] data)
      throws HopDatabaseException {
    ResultSet res;

    // Create a Statement
    try {
      log.snap(Metrics.METRIC_DATABASE_OPEN_QUERY_START, databaseMeta.getName());

      log.snap(Metrics.METRIC_DATABASE_SQL_VALUES_START, databaseMeta.getName());
      setValues(params, data, ps); // set the parameters!
      log.snap(Metrics.METRIC_DATABASE_SQL_VALUES_STOP, databaseMeta.getName());

      if (canWeSetFetchSize(ps)) {
        int maxRows = ps.getMaxRows();
        int fs = Const.FETCH_SIZE <= maxRows ? maxRows : Const.FETCH_SIZE;
        // mysql have some restriction on fetch size assignment
        if (databaseMeta.isMySqlVariant()) {
          setMysqlFetchSize(ps, fs, maxRows);
        } else {
          // other databases seems not.
          ps.setFetchSize(fs);
        }

        ps.setFetchDirection(ResultSet.FETCH_FORWARD);
      }

      if (rowlimit > 0 && databaseMeta.supportsSetMaxRows()) {
        ps.setMaxRows(rowlimit);
      }

      log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_START, databaseMeta.getName());
      res = ps.executeQuery();
      log.snap(Metrics.METRIC_DATABASE_EXECUTE_SQL_STOP, databaseMeta.getName());

      // MySQL Hack only. It seems too much for the cursor type of operation on
      // MySQL, to have another cursor opened
      // to get the length of a String field. So, on MySQL, we ignore the length
      // of Strings in result rows.
      //
      log.snap(Metrics.METRIC_DATABASE_GET_ROW_META_START, databaseMeta.getName());
      rowMeta = getRowInfo(res.getMetaData(), databaseMeta.isMySqlVariant(), false);
      log.snap(Metrics.METRIC_DATABASE_GET_ROW_META_STOP, databaseMeta.getName());
    } catch (SQLException ex) {
      throw new HopDatabaseException("ERROR executing query", ex);
    } catch (Exception e) {
      throw new HopDatabaseException("ERROR executing query", e);
    } finally {
      log.snap(Metrics.METRIC_DATABASE_OPEN_QUERY_STOP, databaseMeta.getName());
    }

    return res;
  }

  void setMysqlFetchSize(PreparedStatement ps, int fs, int getMaxRows)
      throws SQLException, HopDatabaseException {
    if (databaseMeta.isStreamingResults() && getDatabaseMetaData().getDriverMajorVersion() == 3) {
      ps.setFetchSize(Integer.MIN_VALUE);
    } else if (fs <= getMaxRows) {
      // PDI-11373 do not set fetch size more than max rows can returns
      ps.setFetchSize(fs);
    }
  }

  /**
   * Returns a RowMeta describing the fields of a table expression.
   *
   * <p>Note that this implementation makes use of a SQL statement in order to populate the
   * ValueMeta object in the RowMeta it returns. This is sometimes necessary when the caller needs
   * the ValueMeta values to be properly casted.
   *
   * <p>In cases where a simple list of columns is required, it is preferable to use {@link
   * #getTableFieldsMeta(String, String)}. This other method will not use a SQL query and will
   * populate whatever information it can using @link {@link DatabaseMetaData#getColumns(String,
   * String, String, String)}.
   *
   * @param tableName This is the properly quoted, and schema prefixed table name.
   */
  public IRowMeta getTableFields(String tableName) throws HopDatabaseException {
    return getQueryFields(databaseMeta.getSqlQueryFields(tableName), false);
  }

  public IRowMeta getQueryFields(String sql, boolean param) throws HopDatabaseException {
    return getQueryFields(sql, param, null, null);
  }

  /**
   * See if the table specified exists by reading
   *
   * @param tableName The name of the table to check.<br>
   *     This is supposed to be the properly quoted name of the table or the complete schema-table
   *     name combination.
   * @return true if the table exists, false if it doesn't.
   * @deprecated Deprecated in favor of {@link #checkTableExists(String, String)}
   */
  public boolean checkTableExists(String tableName) throws HopDatabaseException {
    try {
      if (log.isDebug()) {
        log.logDebug("Checking if table [" + tableName + "] exists!");
      }
      // Just try to read from the table.
      String sql = databaseMeta.getSqlTableExists(tableName);
      try {
        getOneRow(sql);
        return true;
      } catch (HopDatabaseException e) {
        return false;
      }
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Unable to check if table ["
              + tableName
              + "] exists on connection ["
              + databaseMeta.getName()
              + "]",
          e);
    }
  }

  /**
   * See if the table specified exists.
   *
   * <p>This is a smarter implementation of {@link #checkTableExists(String)} where metadata is used
   * first and we only use statements when absolutely necessary.
   *
   * <p>Contrary to previous versions of similar duplicated methods, this implementation does not
   * require quoted identifiers.
   *
   * @param tableName The unquoted name of the table to check.<br>
   *     This is NOT the properly quoted name of the table or the complete schema-table name
   *     combination.
   * @param schema The unquoted name of the schema.
   * @return true if the table exists, false if it doesn't.
   */
  public boolean checkTableExists(String schema, String tableName) throws HopDatabaseException {

    if (useJdbcMeta()) {
      return checkTableExistsByDbMeta(schema, tableName);
    } else {
      return checkTableExists(
          databaseMeta.getQuotedSchemaTableCombination(this, schema, tableName));
    }
  }

  /**
   * See if the table specified exists by getting db metadata.
   *
   * @param tableName The name of the table to check.<br>
   *     This is supposed to be the properly quoted name of the table or the complete schema-table
   *     name combination.
   * @return true if the table exists, false if it doesn't.
   * @throws HopDatabaseException
   * @deprecated Deprecated in favor of {@link #checkTableExists(String, String)}
   */
  @Deprecated
  public boolean checkTableExistsByDbMeta(String schema, String tableName)
      throws HopDatabaseException {
    boolean isTableExist = false;
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG, "Database.Info.CheckingIfTableExistsInDbMetaData", tableName));
    }
    try (ResultSet resTables = getTableMetaData(schema, tableName)) {
      while (resTables.next()) {
        String resTableName = resTables.getString(TABLES_META_DATA_TABLE_NAME);
        if (tableName.equalsIgnoreCase(resTableName)) {
          if (log.isDebug()) {
            log.logDebug(BaseMessages.getString(PKG, "Database.Info.TableFound", tableName));
          }
          isTableExist = true;
          break;
        }
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(
              PKG, "Database.Error.UnableToCheckExistingTable", tableName, databaseMeta.getName()),
          e);
    }
    return isTableExist;
  }

  /**
   * Retrieves the table description matching the schema and table name.
   *
   * @param schema the schema name pattern
   * @param table the table name pattern
   * @return table description row set
   * @throws HopDatabaseException if DatabaseMetaData is null or some database error occurs
   */
  private ResultSet getTableMetaData(String schema, String table) throws HopDatabaseException {
    ResultSet tables = null;
    if (getDatabaseMetaData() == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetDbMeta"));
    }
    try {
      tables = getDatabaseMetaData().getTables(null, schema, table, TABLE_TYPES_TO_GET);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetTableNames"), e);
    }
    if (tables == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetTableNames"));
    }
    return tables;
  }

  /**
   * Retrieves the columns metadata matching the schema and table name.
   *
   * @param schema the schema name pattern
   * @param table the table name pattern
   * @throws HopDatabaseException if DatabaseMetaData is null or some database error occurs
   */
  private ResultSet getColumnsMetaData(String schema, String table) throws HopDatabaseException {
    ResultSet columns = null;
    if (getDatabaseMetaData() == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetDbMeta"));
    }
    try {
      columns = getDatabaseMetaData().getColumns(null, schema, table, null);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetTableNames"), e);
    }
    if (columns == null) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Error.UnableToGetTableNames"));
    }
    return columns;
  }

  /**
   * See if the column specified exists by reading the metadata first, execution last.
   *
   * <p>This is a smarter implementation of {@link #checkTableExists(String)} where metadata is used
   * first and we only use statements when absolutely necessary.
   *
   * <p>Contrary to previous versions of similar duplicated methods, this implementation does not
   * require quoted identifiers.
   *
   * @param schemaname The name of the schema to check.
   * @param tableName The name of the table to check.
   * @param columnName The name of the column to check.
   * @return true if the table exists, false if it doesn't.
   */
  public boolean checkColumnExists(String schemaname, String tableName, String columnName)
      throws HopDatabaseException {
    if (useJdbcMeta()) {
      return checkColumnExistsByDbMeta(schemaname, tableName, columnName);
    } else {
      return checkColumnExists(
          databaseMeta.quoteField(columnName),
          databaseMeta.getQuotedSchemaTableCombination(this, schemaname, tableName));
    }
  }

  public boolean checkColumnExistsByDbMeta(String schemaname, String tableName, String columnName)
      throws HopDatabaseException {
    if (log.isDebug()) {
      log.logDebug("Checking if column [" + columnName + "] exists in table [" + tableName + "] !");
    }

    // First try the metadata
    try {
      ResultSet columns = getColumnsMetaData(schemaname, tableName);
      while (columns.next()) {
        if (columnName.equals(columns.getString("COLUMN_NAME"))) {
          return true;
        }
      }
      return false;
    } catch (HopDatabaseException | SQLException e) {
      // That's ok. We will use a prepared statement.
      throw new HopDatabaseException("Metadata check failed. Fallback to statement check.");
    }
  }

  /**
   * See if the column specified exists by reading
   *
   * @param columnName The name of the column to check.
   * @param tableName The name of the table to check.<br>
   *     This is supposed to be the properly quoted name of the table or the complete schema-table
   *     name combination.
   * @return true if the table exists, false if it doesn't.
   * @deprecated Deprecated in favor of the smarter {@link #checkColumnExists(String, String,
   *     String)}
   */
  @Deprecated
  public boolean checkColumnExists(String columnName, String tableName)
      throws HopDatabaseException {
    try {
      if (log.isDebug()) {
        log.logDebug(
            "Checking if column [" + columnName + "] exists in table [" + tableName + "] !");
      }

      // Just try to read from the table.
      String sql = databaseMeta.getSqlColumnExists(columnName, tableName);

      try {
        getOneRow(sql);
        return true;
      } catch (HopDatabaseException e) {
        return false;
      }
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Unable to check if column ["
              + columnName
              + "] exists in table ["
              + tableName
              + "] on connection ["
              + databaseMeta.getName()
              + "]",
          e);
    }
  }

  /**
   * Check whether the sequence exists, Oracle only!
   *
   * @param sequenceName The name of the sequence
   * @return true if the sequence exists.
   */
  public boolean checkSequenceExists(String sequenceName) throws HopDatabaseException {
    return checkSequenceExists(null, sequenceName);
  }

  /**
   * Check whether the sequence exists, Oracle only!
   *
   * @param sequenceName The name of the sequence
   * @return true if the sequence exists.
   */
  public boolean checkSequenceExists(String schemaName, String sequenceName)
      throws HopDatabaseException {
    boolean retval = false;

    if (!databaseMeta.supportsSequences()) {
      return retval;
    }

    String schemaSequence =
        databaseMeta.getQuotedSchemaTableCombination(this, schemaName, sequenceName);
    try {
      //
      // Get the info from the data dictionary...
      //
      String sql = databaseMeta.getSqlSequenceExists(schemaSequence);
      ResultSet res = openQuery(sql);
      if (res != null) {
        Object[] row = getRow(res);
        if (row != null) {
          retval = true;
        }
        closeQuery(res);
      }
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Unexpected error checking whether or not sequence [" + schemaSequence + "] exists", e);
    }

    return retval;
  }

  /**
   * Check if an index on certain fields in a table exists.
   *
   * @param tableName The table on which the index is checked
   * @param idxFields The fields on which the indexe is checked
   * @return True if the index exists
   */
  public boolean checkIndexExists(String tableName, String[] idxFields)
      throws HopDatabaseException {
    return checkIndexExists(null, tableName, idxFields);
  }

  /**
   * Check if an index on certain fields in a table exists.
   *
   * @param schemaName The schema on which the index is checked
   * @param tableName The table on which the index is checked
   * @param idxFields The fields on which the indexe is checked
   * @return True if the index exists
   */
  public boolean checkIndexExists(String schemaName, String tableName, String[] idxFields)
      throws HopDatabaseException {
    String schemaTable = databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);
    if (!checkTableExists(schemaTable)) {
      return false;
    }

    if (log.isDebug()) {
      log.logDebug(
          "CheckIndexExists() table = " + schemaTable + " type = " + databaseMeta.getPluginId());
    }

    return databaseMeta.getIDatabase().checkIndexExists(this, schemaName, schemaTable, idxFields);
  }

  public String getCreateIndexStatement(
      String tableName,
      String indexname,
      String[] idxFields,
      boolean tk,
      boolean unique,
      boolean bitmap,
      boolean semiColon) {
    return getCreateIndexStatement(
        null, tableName, indexname, idxFields, tk, unique, bitmap, semiColon);
  }

  public String getCreateIndexStatement(
      String schemaname,
      String tableName,
      String indexname,
      String[] idxFields,
      boolean tk,
      boolean unique,
      boolean bitmap,
      boolean semiColon) {
    String crIndex = "";
    IDatabase iDatabase = databaseMeta.getIDatabase();

    // Exasol does not support explicit handling of indexes
    if (iDatabase.isExasolVariant()) {
      return "";
    }

    crIndex += "CREATE ";

    if (unique || (tk && iDatabase.isSybaseVariant())) {
      crIndex += "UNIQUE ";
    }

    if (bitmap && databaseMeta.supportsBitmapIndex()) {
      crIndex += "BITMAP ";
    }

    crIndex += "INDEX " + databaseMeta.quoteField(indexname) + " ";
    crIndex += "ON ";
    // assume table has already been quoted (and possibly includes schema)
    crIndex += tableName;
    crIndex += "(";
    for (int i = 0; i < idxFields.length; i++) {
      if (i > 0) {
        crIndex += ", ";
      }
      crIndex += databaseMeta.quoteField(idxFields[i]);
    }
    crIndex += ")" + Const.CR;

    crIndex += iDatabase.getIndexTablespaceDDL(variables, databaseMeta);

    if (semiColon) {
      crIndex += ";" + Const.CR;
    }

    return crIndex;
  }

  public String getCreateSequenceStatement(
      String sequence, long startAt, long incrementBy, long maxValue, boolean semiColon) {
    return getCreateSequenceStatement(
        null,
        sequence,
        Long.toString(startAt),
        Long.toString(incrementBy),
        Long.toString(maxValue),
        semiColon);
  }

  public String getCreateSequenceStatement(
      String sequence, String startAt, String incrementBy, String maxValue, boolean semiColon) {
    return getCreateSequenceStatement(null, sequence, startAt, incrementBy, maxValue, semiColon);
  }

  public String getCreateSequenceStatement(
      String schemaName,
      String sequence,
      long startAt,
      long incrementBy,
      long maxValue,
      boolean semiColon) {
    return getCreateSequenceStatement(
        schemaName,
        sequence,
        Long.toString(startAt),
        Long.toString(incrementBy),
        Long.toString(maxValue),
        semiColon);
  }

  public String getCreateSequenceStatement(
      String schemaName,
      String sequenceName,
      String startAt,
      String incrementBy,
      String maxValue,
      boolean semiColon) {
    String crSeq = "";

    if (Utils.isEmpty(sequenceName)) {
      return crSeq;
    }

    if (databaseMeta.supportsSequences()) {
      String schemaSequence =
          databaseMeta.getQuotedSchemaTableCombination(this, schemaName, sequenceName);
      crSeq += "CREATE SEQUENCE " + schemaSequence + " " + Const.CR; // Works
      // for
      // both
      // Oracle
      // and
      // PostgreSQL
      // :-)
      crSeq += "START WITH " + startAt + " " + Const.CR;
      crSeq += "INCREMENT BY " + incrementBy + " " + Const.CR;
      if (maxValue != null) {
        // "-1" means there is no maxvalue, must be handles different by DB2 /
        // AS400
        //
        if (databaseMeta.supportsSequenceNoMaxValueOption() && maxValue.trim().equals("-1")) {
          IDatabase iDatabase = databaseMeta.getIDatabase();
          crSeq += iDatabase.getSequenceNoMaxValueOption() + Const.CR;
        } else {
          // set the max value
          crSeq += "MAXVALUE " + maxValue + Const.CR;
        }
      }

      if (semiColon) {
        crSeq += ";" + Const.CR;
      }
    }

    return crSeq;
  }

  /**
   * Returns a RowMeta describing the fields of a table.
   *
   * <p>This is a lighter implementation of {@link #getTableFields(String)} where metadata is used
   * first and we only use statements when absolutely necessary.
   *
   * <p>Note that the ValueMeta returned here will not contain any actual values and as such, this
   * method should be used whenever a simple list of columns is required, and we're not planning on
   * looking at the actual data.
   *
   * <p>Contrary to previous versions of similar duplicated methods, this implementation does not
   * require quoted identifiers.
   *
   * @param schemaName The unquoted schema name. Can be null.
   * @param tableName The unquoted table name. Cannot be null.
   */
  public IRowMeta getTableFieldsMeta(String schemaName, String tableName)
      throws HopDatabaseException {
    if (useJdbcMeta()) {
      return getTableFieldsMetaByDbMeta(schemaName, tableName);
    } else {
      String tableSchema =
          databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);
      String sql = databaseMeta.getSqlQueryFields(tableSchema);
      return getQueryFields(sql, false);
    }
  }

  public IRowMeta getTableFieldsMetaByDbMeta(String schemaName, String tableName)
      throws HopDatabaseException {
    try {
      // Cleanup a bit. In JDBC metadata, we want null names for
      // wildcards, not empty strings.
      if ("".equals(schemaName)) {
        schemaName = null;
      }
      if ("".equals(tableName)) {
        tableName = null;
      }

      IRowMeta fields = null;
      DbCache dbcache = DbCache.getInstance();
      DbCacheEntry entry = null;

      if (dbcache != null) {
        // Cache key must not match the other implementation where
        // valuemeta is properly casted. We're not caching values here,
        // just metadata.
        entry =
            new DbCacheEntry(
                databaseMeta.getName(),
                "LIGHTWEIGHT_SALT"
                    .concat(schemaName == null ? "nullSchema" : schemaName)
                    .concat(tableName == null ? "nullTable" : tableName));

        fields = dbcache.get(entry);

        if (fields != null) {
          return fields;
        }
      }
      if (connection == null) {
        return null; // Cache test without connect.
      }

      // First get the fields through metadata
      ResultSet rm = connection.getMetaData().getColumns(null, schemaName, tableName, null);

      if (fields == null) {
        fields = new RowMeta();
      }

      while (rm.next()) {
        IValueMeta valueMeta = null;
        for (IValueMeta valueMetaClass : valueMetaPluginClasses) {
          try {
            IValueMeta v = valueMetaClass.getMetadataPreview( this, databaseMeta, rm );
            if (v != null) {
              valueMeta = v;
              break;
            }
          } catch (HopDatabaseException e) {
            // That's ok. The VMI impl doesn't like this data type.
            if (log.isDebug()) {
              log.logDebug("Skipping IValueMeta:" + valueMetaClass.getClass().getName(), e);
            }
          }
        }
        fields.addValueMeta(valueMeta);
      }

      // Store in cache!!
      if (dbcache != null && entry != null) {
        if (fields != null) {
          dbcache.put(entry, fields);
        }
      }

      return fields;
    } catch (Exception e) {
      throw new HopDatabaseException("Failed to fetch fields from jdbc meta ", e);
    }
  }

  public IRowMeta getQueryFields(String sql, boolean param, IRowMeta inform, Object[] data)
      throws HopDatabaseException {
    IRowMeta fields;
    DbCache dbcache = DbCache.getInstance();

    DbCacheEntry entry = null;

    // Check the cache first!
    //
    if (dbcache != null) {
      entry = new DbCacheEntry(databaseMeta.getName(), sql);
      fields = dbcache.get(entry);
      if (fields != null) {
        return fields;
      }
    }
    if (connection == null) {
      return null; // Cache test without connect.
    }

    // No cache entry found

    // The new method of retrieving the query fields fails on Oracle because
    // they failed to implement the getMetaData method on a prepared statement.
    // (!!!)
    // Even recent drivers like 10.2 fail because of it.
    //
    // There might be other databases that don't support it (we have no
    // knowledge of this at the time of writing).
    // If we discover other RDBMSs, we will create an interface for it.
    // For now, we just try to get the field layout on the re-bound in the
    // exception block below.
    //
    try {
      if (databaseMeta.supportsPreparedStatementMetadataRetrieval()) {
        // On with the regular program.
        //
        fields = getQueryFieldsFromPreparedStatement(sql);
      } else {
        if (isDataServiceConnection()) {
          fields = getQueryFieldsFromDatabaseMetaData(sql);
        } else {
          fields = getQueryFieldsFromDatabaseMetaData();
        }
      }
    } catch (Exception e) {
      /*
       * databaseMeta.getDatabaseType()==DatabaseMeta.TYPE_DATABASE_SYBASEIQ ) {
       */
      fields = getQueryFieldsFallback(sql, param, inform, data);
    }

    // Store in cache!!
    if (dbcache != null && entry != null) {
      if (fields != null) {
        dbcache.put(entry, fields);
      }
    }

    return fields;
  }

  private boolean isDataServiceConnection() {
    return DATA_SERVICES_PLUGIN_ID.equals(databaseMeta.getPluginId());
  }

  public IRowMeta getQueryFieldsFromPreparedStatement(String sql) throws Exception {
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement =
          connection.prepareStatement(
              databaseMeta.stripCR(sql), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      preparedStatement.setMaxRows(1);
      ResultSetMetaData rsmd = preparedStatement.getMetaData();
      return getRowInfo(rsmd, false, false);
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      if (preparedStatement != null) {
        try {
          preparedStatement.close();
        } catch (SQLException e) {
          throw new HopDatabaseException(
              "Unable to close prepared statement after determining SQL layout", e);
        }
      }
    }
  }

  public IRowMeta getQueryFieldsFromDatabaseMetaData() throws Exception {
    return this.getQueryFieldsFromDatabaseMetaData(null);
  }

  private IRowMeta getQueryFieldsFromDatabaseMetaData(String sql) throws Exception {

    ResultSet columns =
        connection
            .getMetaData()
            .getColumns("", "", StringUtils.isNotBlank(sql) ? sql : databaseMeta.getName(), "");
    IRowMeta rowMeta = new RowMeta();
    while (columns.next()) {
      IValueMeta valueMeta = null;
      String name = columns.getString("COLUMN_NAME");
      String type = columns.getString("SOURCE_DATA_TYPE");
      int size = columns.getInt("COLUMN_SIZE");
      if (type.equals("Integer") || type.equals("Long")) {
        valueMeta = new ValueMetaInteger();
      } else if (type.equals("BigDecimal") || type.equals("BigNumber")) {
        valueMeta = new ValueMetaBigNumber();
      } else if (type.equals("Double") || type.equals("Number")) {
        valueMeta = new ValueMetaNumber();
      } else if (type.equals("String")) {
        valueMeta = new ValueMetaString();
      } else if (type.equals("Date")) {
        valueMeta = new ValueMetaDate();
      } else if (type.equals("Boolean")) {
        valueMeta = new ValueMetaBoolean();
      } else if (type.equals("Binary")) {
        valueMeta = new ValueMetaBinary();
      } else if (type.equals("Timestamp")) {
        valueMeta = new ValueMetaTimestamp();
      } else if (type.equals("Internet Address")) {
        valueMeta = new ValueMetaInternetAddress();
      }
      if (valueMeta != null) {
        valueMeta.setName(name);
        valueMeta.setComments(name);
        valueMeta.setLength(size);
        valueMeta.setOriginalColumnTypeName(type);

        valueMeta.setConversionMask(columns.getString("SOURCE_MASK"));
        valueMeta.setDecimalSymbol(columns.getString("SOURCE_DECIMAL_SYMBOL"));
        valueMeta.setGroupingSymbol(columns.getString("SOURCE_GROUPING_SYMBOL"));
        valueMeta.setCurrencySymbol(columns.getString("SOURCE_CURRENCY_SYMBOL"));

        rowMeta.addValueMeta(valueMeta);
      } else {
        log.logBasic(
            "Database.getQueryFields() IValueMeta mapping not resolved for the column " + name);
        rowMeta = null;
        break;
      }
    }
    if (rowMeta != null && !rowMeta.isEmpty()) {
      return rowMeta;
    } else {
      throw new Exception("Error in Database.getQueryFields()");
    }
  }

  public IRowMeta getQueryFieldsFallback(String sql, boolean param, IRowMeta inform, Object[] data)
      throws HopDatabaseException {
    IRowMeta fields;

    try {
      if ((inform == null
              // Hack for MSSQL jtds 1.2 when using xxx NOT IN yyy we have to use a
              // prepared statement (see BugID 3214)
              && databaseMeta.getIDatabase().isMsSqlServerVariant())
          || databaseMeta.getIDatabase().supportsResultSetMetadataRetrievalOnly()) {
        selStmt =
            connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        try {
          if (databaseMeta.isFetchSizeSupported() && selStmt.getMaxRows() >= 1) {
            if (databaseMeta.getIDatabase().isMySqlVariant()) {
              selStmt.setFetchSize(Integer.MIN_VALUE);
            } else {
              selStmt.setFetchSize(1);
            }
          }
          if (databaseMeta.supportsSetMaxRows()) {
            selStmt.setMaxRows(1);
          }

          ResultSet r = selStmt.executeQuery(databaseMeta.stripCR(sql));
          try {
            fields = getRowInfo(r.getMetaData(), false, false);
          } finally { // avoid leaking resources
            r.close();
          }
        } finally { // avoid leaking resources
          selStmt.close();
          selStmt = null;
        }
      } else {
        PreparedStatement ps = connection.prepareStatement(databaseMeta.stripCR(sql));
        try {
          if (param) {
            IRowMeta par = inform;

            if (par == null || par.isEmpty()) {
              par = getParameterMetaData(ps);
            }

            if (par == null || par.isEmpty()) {
              par = getParameterMetaData(sql, inform, data);
            }

            setValues(par, data, ps);
          }
          ResultSet r = ps.executeQuery();
          try {
            //
            // See PDI-14893
            // If we're in this private fallback method, it's because the databasemeta returns false
            // for
            // supportsPreparedStatementMetadataRetrieval() or because we got an exception trying to
            // do
            // it the other way. In either case, there is no reason for us to ever try getting the
            // prepared
            // statement's metadata. The right answer is to directly get the resultset metadata.
            //
            // ResultSetMetaData metadata = ps.getMetaData();
            // If the PreparedStatement can't get us the metadata, try using the ResultSet's
            // metadata
            // if ( metadata == null ) {
            //  metadata = r.getMetaData();
            // }
            ResultSetMetaData metadata = r.getMetaData();
            fields = getRowInfo(metadata, false, false);
          } finally { // should always use a try/finally to avoid leaks
            r.close();
          }
        } finally { // should always use a try/finally to avoid leaks
          ps.close();
        }
      }
    } catch (Exception ex) {
      throw new HopDatabaseException("Couldn't get field info from [" + sql + "]" + Const.CR, ex);
    }

    return fields;
  }

  public void closeQuery(ResultSet res) throws HopDatabaseException {
    // close everything involved in the query!
    try {
      if (res != null) {
        res.close();
      }
      if (selStmt != null) {
        selStmt.close();
        selStmt = null;
      }
      if (pstmt != null) {
        pstmt.close();
        pstmt = null;
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException("Couldn't close query: resultset or prepared statements", ex);
    }
  }

  /**
   * Build the row using ResultSetMetaData rsmd
   *
   * @param rm The resultset metadata to inquire
   * @param ignoreLength true if you want to ignore the length (workaround for MySQL bug/problem)
   * @param lazyConversion true if lazy conversion needs to be enabled where possible
   */
  private IRowMeta getRowInfo(ResultSetMetaData rm, boolean ignoreLength, boolean lazyConversion)
      throws HopDatabaseException {
    try {
      log.snap(Metrics.METRIC_DATABASE_GET_ROW_META_START, databaseMeta.getName());

      if (rm == null) {
        throw new HopDatabaseException(
            "No result set metadata available to retrieve row metadata!");
      }

      IRowMeta rowMeta = new RowMeta();

      try {
        int nrcols = rm.getColumnCount();
        for (int i = 1; i <= nrcols; i++) {
          IValueMeta valueMeta = getValueFromSqlType(rm, i, ignoreLength, lazyConversion);
          rowMeta.addValueMeta(valueMeta);
        }
        return rowMeta;
      } catch (SQLException ex) {
        throw new HopDatabaseException("Error getting row information from database: ", ex);
      }
    } finally {
      log.snap(Metrics.METRIC_DATABASE_GET_ROW_META_STOP, databaseMeta.getName());
    }
  }

  private IValueMeta getValueFromSqlType(
      ResultSetMetaData rm, int i, boolean ignoreLength, boolean lazyConversion)
      throws HopDatabaseException, SQLException {
    // TODO If we do lazy conversion, we need to find out about the encoding
    //

    // Extract the name from the result set meta data...
    //
    String name;
    if (databaseMeta.isMySqlVariant()) {
      name = databaseMeta.getIDatabase().getLegacyColumnName(getDatabaseMetaData(), rm, i);
    } else {
      name = new String(rm.getColumnName(i));
    }

    // Check the name, sometimes it's empty.
    //
    if (Utils.isEmpty(name) || Const.onlySpaces(name)) {
      name = "Field" + (i + 1);
    }

    // Ask all the value meta types if they want to handle the SQL type.
    // The first to reply something gets the workflow...
    //
    IValueMeta valueMeta = null;
    for (IValueMeta valueMetaClass : valueMetaPluginClasses) {
      IValueMeta v =
          valueMetaClass.getValueFromSqlType( this,
            databaseMeta, name, rm, i, ignoreLength, lazyConversion );
      if (v != null) {
        valueMeta = v;
        break;
      }
    }

    if (valueMeta != null) {
      return valueMeta;
    }

    throw new HopDatabaseException(
        "Unable to handle database column '"
            + name
            + "', on column index "
            + i
            + " : not a handled data type");
  }

  public boolean absolute(ResultSet rs, int position) throws HopDatabaseException {
    try {
      return rs.absolute(position);
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to move resultset to position " + position, e);
    }
  }

  public boolean relative(ResultSet rs, int rows) throws HopDatabaseException {
    try {
      return rs.relative(rows);
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to move the resultset forward " + rows + " rows", e);
    }
  }

  public void afterLast(ResultSet rs) throws HopDatabaseException {
    try {
      rs.afterLast();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to move resultset to after the last position", e);
    }
  }

  public void first(ResultSet rs) throws HopDatabaseException {
    try {
      rs.first();
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to move resultset to the first position", e);
    }
  }

  /**
   * Get a row from the resultset. Do not use lazy conversion
   *
   * @param rs The resultset to get the row from
   * @return one row or null if no row was found on the resultset or if an error occurred.
   */
  public Object[] getRow(ResultSet rs) throws HopDatabaseException {
    return getRow(rs, false);
  }

  /**
   * Get a row from the resultset.
   *
   * @param rs The resultset to get the row from
   * @param lazyConversion set to true if strings need to have lazy conversion enabled
   * @return one row or null if no row was found on the resultset or if an error occurred.
   */
  public Object[] getRow(ResultSet rs, boolean lazyConversion) throws HopDatabaseException {
    if (rowMeta == null) {
      ResultSetMetaData rsmd = null;
      try {
        rsmd = rs.getMetaData();
      } catch (SQLException e) {
        throw new HopDatabaseException("Unable to retrieve metadata from resultset", e);
      }

      rowMeta = getRowInfo(rsmd, false, lazyConversion);
    }

    return getRow(rs, null, rowMeta);
  }

  /**
   * Get a row from the resultset.
   *
   * @param rs The resultset to get the row from
   * @return one row or null if no row was found on the resultset or if an error occurred.
   */
  public Object[] getRow(ResultSet rs, ResultSetMetaData dummy, IRowMeta rowInfo)
      throws HopDatabaseException {
    long startTime = System.currentTimeMillis();

    try {

      int nrcols = rowInfo.size();
      Object[] data = RowDataUtil.allocateRowData(nrcols);

      if (rs.next()) {
        for (int i = 0; i < nrcols; i++) {
          IValueMeta val = rowInfo.getValueMeta(i);

          data[i] = databaseMeta.getValueFromResultSet(rs, val, i);
        }
      } else {
        data = null;
      }

      return data;
    } catch (Exception ex) {
      throw new HopDatabaseException("Couldn't get row from result set", ex);
    } finally {
      if (log.isGatheringMetrics()) {
        long time = System.currentTimeMillis() - startTime;
        log.snap(Metrics.METRIC_DATABASE_GET_ROW_SUM_TIME, databaseMeta.getName(), time);
        log.snap(Metrics.METRIC_DATABASE_GET_ROW_MIN_TIME, databaseMeta.getName(), time);
        log.snap(Metrics.METRIC_DATABASE_GET_ROW_MAX_TIME, databaseMeta.getName(), time);
        log.snap(Metrics.METRIC_DATABASE_GET_ROW_COUNT, databaseMeta.getName());
      }
    }
  }

  public void printSqlException(SQLException ex) {
    log.logError("==> SQLException: ");
    while (ex != null) {
      log.logError("Message:   " + ex.getMessage());
      log.logError("SQLState:  " + ex.getSQLState());
      log.logError("ErrorCode: " + ex.getErrorCode());
      ex = ex.getNextException();
      log.logError("");
    }
  }

  public void setLookup(
      String table,
      String[] codes,
      String[] condition,
      String[] gets,
      String[] rename,
      String orderby)
      throws HopDatabaseException {
    setLookup(table, codes, condition, gets, rename, orderby, false);
  }

  public void setLookup(
      String schema,
      String table,
      String[] codes,
      String[] condition,
      String[] gets,
      String[] rename,
      String orderby)
      throws HopDatabaseException {
    setLookup(schema, table, codes, condition, gets, rename, orderby, false);
  }

  public void setLookup(
      String tableName,
      String[] codes,
      String[] condition,
      String[] gets,
      String[] rename,
      String orderby,
      boolean checkForMultipleResults)
      throws HopDatabaseException {
    setLookup(null, tableName, codes, condition, gets, rename, orderby, checkForMultipleResults);
  }

  // Lookup certain fields in a table
  public void setLookup(
      String schemaName,
      String tableName,
      String[] codes,
      String[] condition,
      String[] gets,
      String[] rename,
      String orderby,
      boolean checkForMultipleResults)
      throws HopDatabaseException {
    try {
      log.snap(Metrics.METRIC_DATABASE_SET_LOOKUP_START, databaseMeta.getName());

      String table = databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);

      String sql = "SELECT ";

      for (int i = 0; i < gets.length; i++) {
        if (i != 0) {
          sql += ", ";
        }
        sql += databaseMeta.quoteField(gets[i]);
        if (rename != null && rename[i] != null && !gets[i].equalsIgnoreCase(rename[i])) {
          sql += " AS " + databaseMeta.quoteField(rename[i]);
        }
      }

      sql += " FROM " + table + " WHERE ";

      for (int i = 0; i < codes.length; i++) {
        if (i != 0) {
          sql += " AND ";
        }
        sql += databaseMeta.quoteField(codes[i]);
        if ("BETWEEN".equalsIgnoreCase(condition[i])) {
          sql += " BETWEEN ? AND ? ";
        } else if ("IS NULL".equalsIgnoreCase(condition[i])
            || "IS NOT NULL".equalsIgnoreCase(condition[i])) {
          sql += " " + condition[i] + " ";
        } else {
          sql += " " + condition[i] + " ? ";
        }
      }

      if (orderby != null && orderby.length() != 0) {
        sql += " ORDER BY " + orderby;
      }

      try {
        if (log.isDetailed()) {
          log.logDetailed("Setting preparedStatement to [" + sql + "]");
        }
        prepStatementLookup = connection.prepareStatement(databaseMeta.stripCR(sql));
        if (!checkForMultipleResults && databaseMeta.supportsSetMaxRows()) {
          prepStatementLookup.setMaxRows(1); // alywas get only 1 line back!
        }
      } catch (SQLException ex) {
        throw new HopDatabaseException("Unable to prepare statement for update [" + sql + "]", ex);
      }
    } finally {
      log.snap(Metrics.METRIC_DATABASE_SET_LOOKUP_STOP, databaseMeta.getName());
    }
  }

  public boolean prepareUpdate(String table, String[] codes, String[] condition, String[] sets) {
    return prepareUpdate(null, table, codes, condition, sets);
  }

  // Lookup certain fields in a table
  public boolean prepareUpdate(
      String schemaName, String tableName, String[] codes, String[] condition, String[] sets) {
    try {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_UPDATE_START, databaseMeta.getName());

      StringBuilder sql = new StringBuilder(128);

      String schemaTable =
          databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);

      sql.append("UPDATE ").append(schemaTable).append(Const.CR).append("SET ");

      for (int i = 0; i < sets.length; i++) {
        if (i != 0) {
          sql.append(",   ");
        }
        sql.append(databaseMeta.quoteField(sets[i]));
        sql.append(" = ?").append(Const.CR);
      }

      sql.append("WHERE ");

      for (int i = 0; i < codes.length; i++) {
        if (i != 0) {
          sql.append("AND   ");
        }
        sql.append(databaseMeta.quoteField(codes[i]));
        if ("BETWEEN".equalsIgnoreCase(condition[i])) {
          sql.append(" BETWEEN ? AND ? ");
        } else if ("IS NULL".equalsIgnoreCase(condition[i])
            || "IS NOT NULL".equalsIgnoreCase(condition[i])) {
          sql.append(' ').append(condition[i]).append(' ');
        } else {
          sql.append(' ').append(condition[i]).append(" ? ");
        }
      }

      try {
        String s = sql.toString();
        if (log.isDetailed()) {
          log.logDetailed("Setting update preparedStatement to [" + s + "]");
        }
        prepStatementUpdate = connection.prepareStatement(databaseMeta.stripCR(s));
      } catch (SQLException ex) {
        printSqlException(ex);
        return false;
      }

      return true;
    } finally {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_UPDATE_STOP, databaseMeta.getName());
    }
  }

  /**
   * Prepare a delete statement by giving it the tableName, fields and conditions to work with.
   *
   * @param table The table-name to delete in
   * @param codes
   * @param condition
   * @return true when everything went OK, false when something went wrong.
   */
  public boolean prepareDelete(String table, String[] codes, String[] condition) {
    return prepareDelete(null, table, codes, condition);
  }

  /**
   * Prepare a delete statement by giving it the tableName, fields and conditions to work with.
   *
   * @param schemaName the schema-name to delete in
   * @param tableName The table-name to delete in
   * @param codes
   * @param condition
   * @return true when everything went OK, false when something went wrong.
   */
  public boolean prepareDelete(
      String schemaName, String tableName, String[] codes, String[] condition) {
    try {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_DELETE_START, databaseMeta.getName());

      String sql;

      String table = databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);
      sql = "DELETE FROM " + table + Const.CR;
      sql += "WHERE ";

      for (int i = 0; i < codes.length; i++) {
        if (i != 0) {
          sql += "AND   ";
        }
        sql += codes[i];
        if ("BETWEEN".equalsIgnoreCase(condition[i])) {
          sql += " BETWEEN ? AND ? ";
        } else if ("IS NULL".equalsIgnoreCase(condition[i])
            || "IS NOT NULL".equalsIgnoreCase(condition[i])) {
          sql += " " + condition[i] + " ";
        } else {
          sql += " " + condition[i] + " ? ";
        }
      }

      try {
        if (log.isDetailed()) {
          log.logDetailed("Setting update preparedStatement to [" + sql + "]");
        }
        prepStatementUpdate = connection.prepareStatement(databaseMeta.stripCR(sql));
      } catch (SQLException ex) {
        printSqlException(ex);
        return false;
      }

      return true;
    } finally {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_DELETE_STOP, databaseMeta.getName());
    }
  }

  public void setProcLookup(
      String proc, String[] arg, String[] argdir, int[] argtype, String returnvalue, int returntype)
      throws HopDatabaseException {
    try {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_DBPROC_START, databaseMeta.getName());
      String sql;
      int pos = 0;

      sql = "{ ";
      if (returnvalue != null && returnvalue.length() != 0) {
        sql += "? = ";
      }
      sql += "call " + proc + " ";

      if (arg.length > 0) {
        sql += "(";
      }

      for (int i = 0; i < arg.length; i++) {
        if (i != 0) {
          sql += ", ";
        }
        sql += " ?";
      }

      if (arg.length > 0) {
        sql += ")";
      }

      sql += "}";

      try {
        if (log.isDetailed()) {
          log.logDetailed("DBA setting callableStatement to [" + sql + "]");
        }
        cstmt = connection.prepareCall(sql);
        pos = 1;
        if (!Utils.isEmpty(returnvalue)) {
          switch (returntype) {
            case IValueMeta.TYPE_NUMBER:
              cstmt.registerOutParameter(pos, java.sql.Types.DOUBLE);
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              cstmt.registerOutParameter(pos, java.sql.Types.DECIMAL);
              break;
            case IValueMeta.TYPE_INTEGER:
              cstmt.registerOutParameter(pos, java.sql.Types.BIGINT);
              break;
            case IValueMeta.TYPE_STRING:
              cstmt.registerOutParameter(pos, java.sql.Types.VARCHAR);
              break;
            case IValueMeta.TYPE_DATE:
              cstmt.registerOutParameter(pos, java.sql.Types.TIMESTAMP);
              break;
            case IValueMeta.TYPE_BOOLEAN:
              cstmt.registerOutParameter(pos, java.sql.Types.BOOLEAN);
              break;
            default:
              break;
          }
          pos++;
        }
        for (int i = 0; i < arg.length; i++) {
          if (argdir[i].equalsIgnoreCase("OUT") || argdir[i].equalsIgnoreCase("INOUT")) {
            switch (argtype[i]) {
              case IValueMeta.TYPE_NUMBER:
                cstmt.registerOutParameter(i + pos, java.sql.Types.DOUBLE);
                break;
              case IValueMeta.TYPE_BIGNUMBER:
                cstmt.registerOutParameter(i + pos, java.sql.Types.DECIMAL);
                break;
              case IValueMeta.TYPE_INTEGER:
                cstmt.registerOutParameter(i + pos, java.sql.Types.BIGINT);
                break;
              case IValueMeta.TYPE_STRING:
                cstmt.registerOutParameter(i + pos, java.sql.Types.VARCHAR);
                break;
              case IValueMeta.TYPE_DATE:
                cstmt.registerOutParameter(i + pos, java.sql.Types.TIMESTAMP);
                break;
              case IValueMeta.TYPE_BOOLEAN:
                cstmt.registerOutParameter(i + pos, java.sql.Types.BOOLEAN);
                break;
              default:
                break;
            }
          }
        }
      } catch (SQLException ex) {
        throw new HopDatabaseException("Unable to prepare database procedure call", ex);
      }
    } finally {
      log.snap(Metrics.METRIC_DATABASE_PREPARE_DBPROC_STOP, databaseMeta.getName());
    }
  }

  public Object[] getLookup() throws HopDatabaseException {
    return getLookup(prepStatementLookup, false);
  }

  public Object[] getLookup(boolean failOnMultipleResults) throws HopDatabaseException {
    return getLookup(failOnMultipleResults, false);
  }

  public Object[] getLookup(boolean failOnMultipleResults, boolean lazyConversion)
      throws HopDatabaseException {
    return getLookup(prepStatementLookup, failOnMultipleResults, lazyConversion);
  }

  public Object[] getLookup(PreparedStatement ps) throws HopDatabaseException {
    // we assume this is external PreparedStatement and we may need to re-create rowMeta
    // so we just reset it to null and it will be re-created on processRow call
    rowMeta = null;
    return getLookup(ps, false);
  }

  public Object[] getLookup(PreparedStatement ps, boolean failOnMultipleResults)
      throws HopDatabaseException {
    return getLookup(ps, failOnMultipleResults, false);
  }

  public Object[] getLookup(
      PreparedStatement ps, boolean failOnMultipleResults, boolean lazyConversion)
      throws HopDatabaseException {
    ResultSet res = null;
    try {
      log.snap(Metrics.METRIC_DATABASE_GET_LOOKUP_START, databaseMeta.getName());
      res = ps.executeQuery();

      Object[] ret = getRow(res, lazyConversion);

      if (failOnMultipleResults) {
        if (ret != null && res.next()) {
          // if the previous row was null, there's no reason to try res.next()
          // again.
          // on DB2 this will even cause an exception (because of the buggy DB2
          // JDBC driver).
          throw new HopDatabaseException(
              "Only 1 row was expected as a result of a lookup, and at least 2 were found!");
        }
      }
      return ret;
    } catch (SQLException ex) {
      throw new HopDatabaseException("Error looking up row in database", ex);
    } finally {
      try {
        if (res != null) {
          res.close(); // close resultset!
        }
      } catch (SQLException e) {
        throw new HopDatabaseException("Unable to close resultset after looking up data", e);
      } finally {
        log.snap(Metrics.METRIC_DATABASE_GET_LOOKUP_STOP, databaseMeta.getName());
      }
    }
  }

  public DatabaseMetaData getDatabaseMetaData() throws HopDatabaseException {
    if (dbmd == null) {
      try {
        log.snap(Metrics.METRIC_DATABASE_GET_DBMETA_START, databaseMeta.getName());

        if (connection == null) {
          throw new HopDatabaseException(
              BaseMessages.getString(
                  PKG, "Database.Exception.EmptyConnectionError", databaseMeta.getDatabaseName()));
        }

        dbmd = connection.getMetaData(); // Only get the metadata once!
      } catch (Exception e) {
        throw new HopDatabaseException(
            BaseMessages.getString(PKG, "Database.Exception.UnableToGetMetadata"), e);
      } finally {
        log.snap(Metrics.METRIC_DATABASE_GET_DBMETA_STOP, databaseMeta.getName());
      }
    }
    return dbmd;
  }

  public String getDDL(String tableName, IRowMeta fields) throws HopDatabaseException {
    return getDDL(tableName, fields, null, false, null, true);
  }

  public String getDDL(
      String tableName, IRowMeta fields, String tk, boolean useAutoIncrement, String pk)
      throws HopDatabaseException {
    return getDDL(tableName, fields, tk, useAutoIncrement, pk, true);
  }

  public String getDDL(
      String tableName,
      IRowMeta fields,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon)
      throws HopDatabaseException {
    String retval;

    // First, check for reserved SQL in the input row r...
    databaseMeta.quoteReservedWords(fields);
    String quotedTk = tk != null ? databaseMeta.quoteField(tk) : null;

    if (checkTableExists(tableName)) {
      retval = getAlterTableStatement(tableName, fields, quotedTk, useAutoIncrement, pk, semicolon);
    } else {
      retval =
          getCreateTableStatement(tableName, fields, quotedTk, useAutoIncrement, pk, semicolon);
    }

    return retval;
  }

  /**
   * Generates SQL
   *
   * @param tableName the table name or schema/table combination: this needs to be quoted properly
   *     in advance.
   * @param fields the fields
   * @param tk the name of the technical key field
   * @param useAutoIncrement true if we need to use auto-increment fields for a primary key
   * @param pk the name of the primary/technical key field
   * @param semicolon append semicolon to the statement
   * @return the SQL needed to create the specified table and fields.
   */
  public String getCreateTableStatement(
      String tableName,
      IRowMeta fields,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    StringBuilder retval = new StringBuilder();
    IDatabase iDatabase = databaseMeta.getIDatabase();
    retval.append(iDatabase.getCreateTableStatement());

    retval.append(tableName + Const.CR);
    retval.append("(").append(Const.CR);
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        retval.append(", ");
      } else {
        retval.append("  ");
      }

      IValueMeta v = fields.getValueMeta(i);
      retval.append(databaseMeta.getFieldDefinition(v, tk, pk, useAutoIncrement));
    }
    // At the end, before the closing of the statement, we might need to add
    // some constraints...
    // Technical keys
    if (tk != null) {
      if (databaseMeta.requiresCreateTablePrimaryKeyAppend()) {
        retval.append(", PRIMARY KEY (").append(tk).append(")").append(Const.CR);
      }
    }

    // Primary keys
    if (pk != null) {
      if (databaseMeta.requiresCreateTablePrimaryKeyAppend()) {
        retval.append(", PRIMARY KEY (").append(pk).append(")").append(Const.CR);
      }
    }
    retval.append(")").append(Const.CR);

    retval.append(databaseMeta.getIDatabase().getDataTablespaceDDL(variables, databaseMeta));

    if (pk == null && tk == null && databaseMeta.getIDatabase().isNeoviewVariant()) {
      retval.append("NO PARTITION"); // use this as a default when no pk/tk is
      // there, otherwise you get an error
    }

    if (semicolon) {
      retval.append(";");
    }

    return retval.toString();
  }

  public String getAlterTableStatement(
      String tableName,
      IRowMeta fields,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon)
      throws HopDatabaseException {
    String retval = "";

    // Get the fields that are in the table now:
    IRowMeta tabFields = getTableFields(tableName);

    // Don't forget to quote these as well...
    databaseMeta.quoteReservedWords(tabFields);

    // Find the missing fields
    IRowMeta missing = new RowMeta();
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v = fields.getValueMeta(i);
      // Not found?
      if (tabFields.searchValueMeta(v.getName()) == null) {
        missing.addValueMeta(v); // nope --> Missing!
      }
    }

    if (missing.size() != 0) {
      for (int i = 0; i < missing.size(); i++) {
        IValueMeta v = missing.getValueMeta(i);
        retval += databaseMeta.getAddColumnStatement(tableName, v, tk, useAutoIncrement, pk, true);
      }
    }

    // Find the surplus fields
    IRowMeta surplus = new RowMeta();
    for (int i = 0; i < tabFields.size(); i++) {
      IValueMeta v = tabFields.getValueMeta(i);
      // Found in table, not in input ?
      if (fields.searchValueMeta(v.getName()) == null) {
        surplus.addValueMeta(v); // yes --> surplus!
      }
    }

    if (surplus.size() != 0) {
      for (int i = 0; i < surplus.size(); i++) {
        IValueMeta v = surplus.getValueMeta(i);
        retval += databaseMeta.getDropColumnStatement(tableName, v, tk, useAutoIncrement, pk, true);
      }
    }

    //
    // OK, see if there are fields for which we need to modify the type...
    // (length, precision)
    //
    IRowMeta modify = new RowMeta();
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta desiredField = fields.getValueMeta(i);
      IValueMeta currentField = tabFields.searchValueMeta(desiredField.getName());
      if (desiredField != null && currentField != null) {
        String desiredDDL = databaseMeta.getFieldDefinition(desiredField, tk, pk, useAutoIncrement);
        String currentDDL = databaseMeta.getFieldDefinition(currentField, tk, pk, useAutoIncrement);

        boolean mod = !desiredDDL.equalsIgnoreCase(currentDDL);
        if (mod) {
          modify.addValueMeta(desiredField);
        }
      }
    }

    if (modify.size() > 0) {
      for (int i = 0; i < modify.size(); i++) {
        IValueMeta v = modify.getValueMeta(i);
        retval +=
            databaseMeta.getModifyColumnStatement(tableName, v, tk, useAutoIncrement, pk, true);
      }
    }

    return retval;
  }

  public void truncateTable(String tableName) throws HopDatabaseException {
    if (Utils.isEmpty(connectionGroup)) {
      String truncateStatement = databaseMeta.getTruncateTableStatement(this, null, tableName);
      if (truncateStatement == null) {
        throw new HopDatabaseException(
            "Truncate table not supported by " + databaseMeta.getIDatabase().getPluginName());
      }
      execStatement(truncateStatement);
    } else {
      execStatement("DELETE FROM " + databaseMeta.quoteField(tableName));
    }
  }

  public void truncateTable(String schema, String tableName) throws HopDatabaseException {
    if (Utils.isEmpty(connectionGroup)) {
      String truncateStatement = databaseMeta.getTruncateTableStatement(this, schema, tableName);
      if (truncateStatement == null) {
        throw new HopDatabaseException(
            "Truncate table not supported by " + databaseMeta.getIDatabase().getPluginName());
      }
      execStatement(truncateStatement);
    } else {
      execStatement(
          "DELETE FROM " + databaseMeta.getQuotedSchemaTableCombination(this, schema, tableName));
    }
  }

  /**
   * Execute a query and return at most one row from the resultset
   *
   * @param sql The SQL for the query
   * @return one Row with data or null if nothing was found.
   */
  public RowMetaAndData getOneRow(String sql) throws HopDatabaseException {
    ResultSet rs = openQuery(sql);
    if (rs != null) {
      Object[] row = getRow(rs); // One row only;

      try {
        rs.close();
      } catch (Exception e) {
        throw new HopDatabaseException("Unable to close resultset", e);
      }

      if (pstmt != null) {
        try {
          pstmt.close();
        } catch (Exception e) {
          throw new HopDatabaseException("Unable to close prepared statement pstmt", e);
        }
        pstmt = null;
      }
      if (selStmt != null) {
        try {
          selStmt.close();
        } catch (Exception e) {
          throw new HopDatabaseException("Unable to close prepared statement sel_stmt", e);
        }
        selStmt = null;
      }
      return new RowMetaAndData(rowMeta, row);
    } else {
      throw new HopDatabaseException("error opening resultset for query: " + sql);
    }
  }

  public RowMeta getMetaFromRow(Object[] row, ResultSetMetaData md)
      throws SQLException, HopDatabaseException {
    RowMeta meta = new RowMeta();

    for (int i = 0; i < md.getColumnCount(); i++) {
      IValueMeta valueMeta = getValueFromSqlType(md, i + 1, true, false);
      meta.addValueMeta(valueMeta);
    }

    return meta;
  }

  public RowMetaAndData getOneRow(String sql, IRowMeta param, Object[] data)
      throws HopDatabaseException {
    ResultSet rs = openQuery(sql, param, data);
    if (rs != null) {
      Object[] row = getRow(rs); // One value: a number;

      rowMeta = null;
      RowMeta tmpMeta = null;
      try {

        ResultSetMetaData md = rs.getMetaData();
        tmpMeta = getMetaFromRow(row, md);

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          rs.close();
        } catch (Exception e) {
          throw new HopDatabaseException("Unable to close resultset", e);
        }

        if (pstmt != null) {
          try {
            pstmt.close();
          } catch (Exception e) {
            throw new HopDatabaseException("Unable to close prepared statement pstmt", e);
          }
          pstmt = null;
        }
        if (selStmt != null) {
          try {
            selStmt.close();
          } catch (Exception e) {
            throw new HopDatabaseException("Unable to close prepared statement sel_stmt", e);
          }
          selStmt = null;
        }
      }

      return new RowMetaAndData(tmpMeta, row);
    } else {
      return null;
    }
  }

  public IRowMeta getParameterMetaData(PreparedStatement ps) {
    IRowMeta par = new RowMeta();
    try {
      ParameterMetaData pmd = ps.getParameterMetaData();
      for (int i = 1; i <= pmd.getParameterCount(); i++) {
        String name = "par" + i;
        int sqltype = pmd.getParameterType(i);
        int length = pmd.getPrecision(i);
        int precision = pmd.getScale(i);
        IValueMeta val;

        switch (sqltype) {
          case java.sql.Types.CHAR:
          case java.sql.Types.VARCHAR:
            val = new ValueMetaString(name);
            break;
          case java.sql.Types.BIGINT:
          case java.sql.Types.INTEGER:
          case java.sql.Types.NUMERIC:
          case java.sql.Types.SMALLINT:
          case java.sql.Types.TINYINT:
            val = new ValueMetaInteger(name);
            break;
          case java.sql.Types.DECIMAL:
          case java.sql.Types.DOUBLE:
          case java.sql.Types.FLOAT:
          case java.sql.Types.REAL:
            val = new ValueMetaNumber(name);
            break;
          case java.sql.Types.DATE:
          case java.sql.Types.TIME:
          case java.sql.Types.TIMESTAMP:
            val = new ValueMetaDate(name);
            break;
          case java.sql.Types.BOOLEAN:
          case java.sql.Types.BIT:
            val = new ValueMetaBoolean(name);
            break;
          default:
            val = new ValueMetaNone(name);
            break;
        }

        if (val.isNumeric() && (length > 18 || precision > 18)) {
          val = new ValueMetaBigNumber(name);
        }

        par.addValueMeta(val);
      }
    } catch (AbstractMethodError e) {
      // Oops: probably the database or JDBC doesn't support it.
      return null;
    } catch (SQLException e) {
      return null;
    } catch (Exception e) {
      return null;
    }

    return par;
  }

  public int countParameters(String sql) {
    int q = 0;
    boolean quoteOpened = false;
    boolean dquoteOpened = false;

    for (int x = 0; x < sql.length(); x++) {
      char c = sql.charAt(x);

      switch (c) {
        case '\'':
          quoteOpened = !quoteOpened;
          break;
        case '"':
          dquoteOpened = !dquoteOpened;
          break;
        case '?':
          if (!quoteOpened && !dquoteOpened) {
            q++;
          }
          break;
        default:
          break;
      }
    }

    return q;
  }

  // Get the fields back from an SQL query
  public IRowMeta getParameterMetaData(String sql, IRowMeta inform, Object[] data) {
    // The database couldn't handle it: try manually!
    int q = countParameters(sql);

    IRowMeta par = new RowMeta();

    if (inform != null && q == inform.size()) {
      for (int i = 0; i < q; i++) {
        IValueMeta inf = inform.getValueMeta(i);
        IValueMeta v = inf.clone();
        par.addValueMeta(v);
      }
    } else {
      for (int i = 0; i < q; i++) {
        IValueMeta v = new ValueMetaNumber("name" + i);
        par.addValueMeta(v);
      }
    }

    return par;
  }

  public synchronized Long getNextValue(
    String schemaName, String tableName, String valKey )
      throws HopDatabaseException {
    Long nextValue = null;

    String schemaTable = databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);

    String lookup = schemaTable + "." + databaseMeta.quoteField(valKey);

    // Try to find the previous sequence value...
    //
    Counter counter = Counters.getInstance().getCounter( lookup );
    if (counter == null) {
      RowMetaAndData rmad =
          getOneRow("SELECT MAX(" + databaseMeta.quoteField(valKey) + ") FROM " + schemaTable);
      if (rmad != null) {
        long previous;
        try {
          Long tmp = rmad.getRowMeta().getInteger(rmad.getData(), 0);

          // A "select max(x)" on a table with no matching rows will return
          // null.
          if (tmp != null) {
            previous = tmp.longValue();
          } else {
            previous = 0L;
          }
        } catch (HopValueException e) {
          throw new HopDatabaseException(
              "Error getting the first long value from the max value returned from table : "
                  + schemaTable);
        }
        counter = new Counter(previous + 1, 1);
        nextValue = Long.valueOf(counter.getAndNext());

        Counters.getInstance().setCounter( lookup, counter );
      } else {
        throw new HopDatabaseException("Couldn't find maximum key value from table " + schemaTable);
      }
    } else {
      nextValue = Long.valueOf(counter.getAndNext());
    }

    return nextValue;
  }

  @Override
  public String toString() {
    if (databaseMeta != null) {
      return databaseMeta.getName();
    } else {
      return "-";
    }
  }

  public boolean isSystemTable(String tableName) {
    return databaseMeta.isSystemTable(tableName);
  }

  /**
   * Reads the result of an SQL query into an ArrayList
   *
   * @param sql The SQL to launch
   * @param limit <=0 means unlimited, otherwise this specifies the maximum number of rows read.
   * @return An ArrayList of rows.
   * @throws HopDatabaseException if something goes wrong.
   */
  public List<Object[]> getRows(String sql, int limit) throws HopDatabaseException {
    return getRows(sql, limit, null);
  }

  /**
   * Reads the result of an SQL query into an ArrayList
   *
   * @param sql The SQL to launch
   * @param limit <=0 means unlimited, otherwise this specifies the maximum number of rows read.
   * @param monitor The progress monitor to update while getting the rows.
   * @return An ArrayList of rows.
   * @throws HopDatabaseException if something goes wrong.
   */
  public List<Object[]> getRows(String sql, int limit, IProgressMonitor monitor)
      throws HopDatabaseException {

    return getRows(sql, null, null, ResultSet.FETCH_FORWARD, false, limit, monitor);
  }

  /**
   * Reads the result of an SQL query into an ArrayList.
   *
   * @param sql The SQL to launch
   * @param params The types of any parameters to be passed to the query
   * @param data The values of any parameters to be passed to the query
   * @param fetchMode The fetch mode for the query (ResultSet.FETCH_FORWARD, e.g.)
   * @param lazyConversion Whether to perform lazy conversion of the values
   * @param limit <=0 means unlimited, otherwise this specifies the maximum number of rows read.
   * @param monitor The progress monitor to update while getting the rows.
   * @return An ArrayList of rows.
   * @throws HopDatabaseException if something goes wrong.
   */
  public List<Object[]> getRows(
      String sql,
      IRowMeta params,
      Object[] data,
      int fetchMode,
      boolean lazyConversion,
      int limit,
      IProgressMonitor monitor)
      throws HopDatabaseException {
    if (monitor != null) {
      monitor.setTaskName("Opening query...");
    }
    ResultSet rset = openQuery(sql, params, data, fetchMode, lazyConversion);

    return getRows(rset, limit, monitor);
  }

  /**
   * Reads the result of a ResultSet into an ArrayList
   *
   * @param rset the ResultSet to read out
   * @param limit <=0 means unlimited, otherwise this specifies the maximum number of rows read.
   * @param monitor The progress monitor to update while getting the rows.
   * @return An ArrayList of rows.
   * @throws HopDatabaseException if something goes wrong.
   */
  public List<Object[]> getRows(ResultSet rset, int limit, IProgressMonitor monitor)
      throws HopDatabaseException {
    try {
      List<Object[]> result = new ArrayList<>();
      boolean stop = false;
      int i = 0;

      if (rset != null) {
        if (monitor != null && limit > 0) {
          monitor.beginTask("Reading rows...", limit);
        }
        while ((limit <= 0 || i < limit) && !stop) {
          Object[] row = getRow(rset);
          if (row != null) {
            result.add(row);
            i++;
          } else {
            stop = true;
          }
          if (monitor != null && limit > 0) {
            monitor.worked(1);
          }
          if (monitor != null && monitor.isCanceled()) {
            break;
          }
        }
        closeQuery(rset);
        if (monitor != null) {
          monitor.done();
        }
      }

      return result;
    } catch (Exception e) {
      throw new HopDatabaseException("Unable to get list of rows from ResultSet : ", e);
    }
  }

  public List<Object[]> getFirstRows(String tableName, int limit) throws HopDatabaseException {
    return getFirstRows(tableName, limit, null);
  }

  /**
   * Get the first rows from a table (for preview)
   *
   * @param tableName The table name (or schema/table combination): this needs to be quoted properly
   * @param limit limit <=0 means unlimited, otherwise this specifies the maximum number of rows
   *     read.
   * @param monitor The progress monitor to update while getting the rows.
   * @return An ArrayList of rows.
   * @throws HopDatabaseException in case something goes wrong
   */
  public List<Object[]> getFirstRows(String tableName, int limit, IProgressMonitor monitor)
      throws HopDatabaseException {
    String sql = "SELECT";
    if (databaseMeta.getIDatabase().isNeoviewVariant()) {
      sql += " [FIRST " + limit + "]";
    } else if (databaseMeta.getIDatabase().isSybaseIQVariant()) {
      // improve support for Sybase IQ
      sql += " TOP " + limit + " ";
    }
    sql += " * FROM " + tableName;

    if (limit > 0) {
      sql += databaseMeta.getLimitClause(limit);
    }

    return getRows(sql, limit, monitor);
  }

  public IRowMeta getReturnRowMeta() {
    return rowMeta;
  }

  public String[] getTableTypes() throws HopDatabaseException {
    try {
      ArrayList<String> types = new ArrayList<>();

      ResultSet rstt = getDatabaseMetaData().getTableTypes();
      while (rstt.next()) {
        String ttype = rstt.getString("TABLE_TYPE");
        types.add(ttype);
      }

      return types.toArray(new String[types.size()]);
    } catch (SQLException e) {
      throw new HopDatabaseException("Unable to get table types from database!", e);
    }
  }

  public String[] getTablenames() throws HopDatabaseException {
    return getTablenames(false);
  }

  public String[] getTablenames(boolean includeSchema) throws HopDatabaseException {
    return getTablenames(null, includeSchema);
  }

  public String[] getTablenames(String schemanamein, boolean includeSchema)
      throws HopDatabaseException {
    return getTablenames(schemanamein, includeSchema, null);
  }

  public String[] getTablenames(
      String schemanamein, boolean includeSchema, Map<String, String> props)
      throws HopDatabaseException {
    Map<String, Collection<String>> tableMap = getTableMap(schemanamein, props);
    List<String> res = new ArrayList<>();
    for (String schema : tableMap.keySet()) {
      Collection<String> tables = tableMap.get(schema);
      for (String table : tables) {
        if (includeSchema) {
          res.add(databaseMeta.getQuotedSchemaTableCombination(this, schema, table));
        } else {
          res.add(databaseMeta.getQuotedSchemaTableCombination(this, null, table));
        }
      }
    }
    return res.toArray(new String[res.size()]);
  }

  public Map<String, Collection<String>> getTableMap() throws HopDatabaseException {
    return getTableMap(null);
  }

  public Map<String, Collection<String>> getTableMap(String schemanamein)
      throws HopDatabaseException {
    return getTableMap(schemanamein, null);
  }

  public Map<String, Collection<String>> getTableMap(String schemanamein, Map<String, String> props)
      throws HopDatabaseException {
    String schemaname = schemanamein;
    if (schemaname == null) {
      if (databaseMeta.useSchemaNameForTableList()) {
        schemaname = resolve(databaseMeta.getUsername()).toUpperCase();
      }
    }
    Map<String, Collection<String>> tableMap = new HashMap<>();
    ResultSet alltables = null;
    try {
      alltables =
          getDatabaseMetaData().getTables(null, schemaname, null, databaseMeta.getTableTypes());
      while (alltables.next()) {
        String cat = "";
        try {
          cat = alltables.getString("TABLE_CAT");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting tables for field TABLE_CAT (ignored): " + e.toString());
          }
        }

        String schema = "";
        try {
          schema = alltables.getString("TABLE_SCHEM");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting tables for field TABLE_SCHEM (ignored): " + e.toString());
          }
        }

        if (Utils.isEmpty(schema)) {
          schema = cat;
        }

        String table = alltables.getString(TABLES_META_DATA_TABLE_NAME);

        if (log.isRowLevel()) {
          log.logRowlevel(
              toString(),
              "got table from meta-data: "
                  + databaseMeta.getQuotedSchemaTableCombination(this, schema, table));
        }

        // Check for any extra properties that might require validation
        if (props != null && !props.isEmpty()) {
          for (Map.Entry<String, String> prop : props.entrySet()) {
            String propName = prop.getKey();

            String tableProperty = alltables.getString(propName);
            if (tableProperty != null) {
              String propValue = prop.getValue();

              if (tableProperty.equals(propValue)) {
                multimapPut(schema, table, tableMap);
              }
            }
          }
        } else {
          multimapPut(schema, table, tableMap);
        }
      }
    } catch (SQLException e) {
      log.logError("Error getting tablenames from schema [" + schemaname + "]");
    } finally {
      try {
        if (alltables != null) {
          alltables.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException(
            "Error closing resultset after getting views from schema [" + schemaname + "]", e);
      }
    }

    if (log.isDetailed()) {
      log.logDetailed("read :" + multimapSize(tableMap) + " table names from db meta-data.");
    }

    return tableMap;
  }

  public String[] getViews() throws HopDatabaseException {
    return getViews(false);
  }

  public String[] getViews(boolean includeSchema) throws HopDatabaseException {
    return getViews(null, includeSchema);
  }

  public String[] getViews(String schemanamein, boolean includeSchema) throws HopDatabaseException {
    Map<String, Collection<String>> viewMap = getViewMap(schemanamein);
    List<String> res = new ArrayList<>();
    for (String schema : viewMap.keySet()) {
      Collection<String> views = viewMap.get(schema);
      for (String view : views) {
        if (includeSchema) {
          res.add(databaseMeta.getQuotedSchemaTableCombination(this, schema, view));
        } else {
          res.add(view);
        }
      }
    }
    return res.toArray(new String[res.size()]);
  }

  public Map<String, Collection<String>> getViewMap() throws HopDatabaseException {
    return getViewMap(null);
  }

  public Map<String, Collection<String>> getViewMap(String schemanamein)
      throws HopDatabaseException {
    if (!databaseMeta.supportsViews()) {
      return Collections.emptyMap();
    }

    String schemaname = schemanamein;
    if (schemaname == null) {
      if (databaseMeta.useSchemaNameForTableList()) {
        schemaname = resolve(databaseMeta.getUsername()).toUpperCase();
      }
    }

    Map<String, Collection<String>> viewMap = new HashMap<>();
    ResultSet allviews = null;
    try {
      allviews =
          getDatabaseMetaData().getTables(null, schemaname, null, databaseMeta.getViewTypes());
      while (allviews.next()) {
        String cat = "";
        try {
          cat = allviews.getString("TABLE_CAT");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting views for field TABLE_CAT (ignored): " + e.toString());
          }
        }

        String schema = "";
        try {
          schema = allviews.getString("TABLE_SCHEM");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting views for field TABLE_SCHEM (ignored): " + e.toString());
          }
        }

        if (Utils.isEmpty(schema)) {
          schema = cat;
        }

        String table = allviews.getString(TABLES_META_DATA_TABLE_NAME);

        if (log.isRowLevel()) {
          log.logRowlevel(
              toString(),
              "got view from meta-data: "
                  + databaseMeta.getQuotedSchemaTableCombination(this, schema, table));
        }
        multimapPut(schema, table, viewMap);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Error getting views from schema [" + schemaname + "]", e);
    } finally {
      try {
        if (allviews != null) {
          allviews.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException(
            "Error closing resultset after getting views from schema [" + schemaname + "]", e);
      }
    }

    if (log.isDetailed()) {
      log.logDetailed("read :" + multimapSize(viewMap) + " views from db meta-data.");
    }

    return viewMap;
  }

  public String[] getSynonyms() throws HopDatabaseException {
    return getSynonyms(false);
  }

  public String[] getSynonyms(boolean includeSchema) throws HopDatabaseException {
    return getSynonyms(null, includeSchema);
  }

  public String[] getSynonyms(String schemanamein, boolean includeSchema)
      throws HopDatabaseException {
    Map<String, Collection<String>> synonymMap = getSynonymMap(schemanamein);
    List<String> res = new ArrayList<>();
    for (String schema : synonymMap.keySet()) {
      Collection<String> synonyms = synonymMap.get(schema);
      for (String synonym : synonyms) {
        if (includeSchema) {
          res.add(databaseMeta.getQuotedSchemaTableCombination(this, schema, synonym));
        } else {
          res.add(synonym);
        }
      }
    }
    return res.toArray(new String[res.size()]);
  }

  public Map<String, Collection<String>> getSynonymMap() throws HopDatabaseException {
    return getSynonymMap(null);
  }

  public Map<String, Collection<String>> getSynonymMap(String schemanamein)
      throws HopDatabaseException {
    if (!databaseMeta.supportsSynonyms()) {
      return Collections.emptyMap();
    }

    String schemaname = schemanamein;
    if (schemaname == null) {
      if (databaseMeta.useSchemaNameForTableList()) {
        schemaname = resolve(databaseMeta.getUsername()).toUpperCase();
      }
    }
    Map<String, Collection<String>> synonymMap = new HashMap<>();
    // ArrayList<String> names = new ArrayList<>();
    ResultSet alltables = null;
    try {
      alltables =
          getDatabaseMetaData().getTables(null, schemaname, null, databaseMeta.getSynonymTypes());
      while (alltables.next()) {
        String cat = "";
        try {
          cat = alltables.getString("TABLE_CAT");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting synonyms for field TABLE_CAT (ignored): " + e.toString());
          }
        }

        String schema = "";
        try {
          schema = alltables.getString("TABLE_SCHEM");
        } catch (Exception e) {
          // ignore
          if (log.isDebug()) {
            log.logDebug("Error getting synonyms for field TABLE_SCHEM (ignored): " + e.toString());
          }
        }

        if (Utils.isEmpty(schema)) {
          schema = cat;
        }

        String table = alltables.getString(TABLES_META_DATA_TABLE_NAME);

        if (log.isRowLevel()) {
          log.logRowlevel(
              toString(),
              "got synonym from meta-data: "
                  + databaseMeta.getQuotedSchemaTableCombination(this, schema, table));
        }
        multimapPut(schema, table, synonymMap);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Error getting synonyms from schema [" + schemaname + "]", e);
    } finally {
      try {
        if (alltables != null) {
          alltables.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException(
            "Error closing resultset after getting synonyms from schema [" + schemaname + "]", e);
      }
    }

    if (log.isDetailed()) {
      log.logDetailed("read :" + multimapSize(synonymMap) + " synonyms from db meta-data.");
    }

    return synonymMap;
  }

  private <K, V> void multimapPut(final K key, final V value, final Map<K, Collection<V>> map) {
    Collection<V> valueCollection = map.get(key);
    if (valueCollection == null) {
      valueCollection = new HashSet<>();
    }
    valueCollection.add(value);
    map.put(key, valueCollection);
  }

  private <K, V> int multimapSize(final Map<K, Collection<V>> map) {
    int count = 0;
    for (Collection<V> valueCollection : map.values()) {
      count += valueCollection.size();
    }
    return count;
  }

  public String[] getSchemas() throws HopDatabaseException {
    ArrayList<String> catalogList = new ArrayList<>();
    ResultSet catalogResultSet = null;
    try {
      catalogResultSet = getDatabaseMetaData().getSchemas();
      // Grab all the catalog names and put them in an array list
      while (catalogResultSet != null && catalogResultSet.next()) {
        catalogList.add(catalogResultSet.getString(1));
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Error getting schemas!", e);
    } finally {
      try {
        if (catalogResultSet != null) {
          catalogResultSet.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException("Error closing resultset after getting schemas!", e);
      }
    }

    if (log.isDetailed()) {
      log.logDetailed("read :" + catalogList.size() + " schemas from db meta-data.");
    }

    return catalogList.toArray(new String[catalogList.size()]);
  }

  public String[] getCatalogs() throws HopDatabaseException {
    ArrayList<String> catalogList = new ArrayList<>();
    ResultSet catalogResultSet = null;
    try {
      catalogResultSet = getDatabaseMetaData().getCatalogs();
      // Grab all the catalog names and put them in an array list
      while (catalogResultSet != null && catalogResultSet.next()) {
        catalogList.add(catalogResultSet.getString(1));
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Error getting catalogs!", e);
    } finally {
      try {
        if (catalogResultSet != null) {
          catalogResultSet.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException("Error closing resultset after getting catalogs!", e);
      }
    }

    if (log.isDetailed()) {
      log.logDetailed("read :" + catalogList.size() + " catalogs from db meta-data.");
    }

    return catalogList.toArray(new String[catalogList.size()]);
  }

  public String[] getProcedures() throws HopDatabaseException {
    String sql = databaseMeta.getSqlListOfProcedures();
    if (sql != null) {
      List<Object[]> procs = getRows(sql, 1000);
      String[] str = new String[procs.size()];
      for (int i = 0; i < procs.size(); i++) {
        str[i] = procs.get(i)[0].toString();
      }
      return str;
    } else {
      ResultSet rs = null;
      try {
        DatabaseMetaData dbmd = getDatabaseMetaData();
        rs = dbmd.getProcedures(null, null, null);
        List<Object[]> rows = getRows(rs, 0, null);
        String[] result = new String[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
          Object[] row = rows.get(i);
          String procCatalog = rowMeta.getString(row, "PROCEDURE_CAT", null);
          String procSchema = rowMeta.getString(row, "PROCEDURE_SCHEM", null);
          String procName = rowMeta.getString(row, "PROCEDURE_NAME", "");

          StringBuilder name = new StringBuilder("");
          if (procCatalog != null) {
            name.append(procCatalog).append(".");
          }
          if (procSchema != null) {
            name.append(procSchema).append(".");
          }

          name.append(procName);

          result[i] = name.toString();
        }
        return result;
      } catch (Exception e) {
        throw new HopDatabaseException(
            "Unable to get list of procedures from database meta-data: ", e);
      } finally {
        if (rs != null) {
          try {
            rs.close();
          } catch (Exception e) {
            // ignore the error.
          }
        }
      }
    }
  }

  public boolean isAutoCommit() {
    return commitsize <= 0;
  }

  /** @return Returns the databaseMeta. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * Lock a tables in the database for write operations
   *
   * @param tableNames The tables to lock. These need to be the appropriately quoted fully qualified
   *     (schema+table) names.
   * @throws HopDatabaseException
   */
  public void lockTables(String[] tableNames) throws HopDatabaseException {
    if (Utils.isEmpty(tableNames)) {
      return;
    }

    // Get the SQL to lock the (quoted) tables
    //
    String sql = databaseMeta.getSqlLockTables(tableNames);
    if (sql != null) {
      execStatements(sql);
    }
  }

  /**
   * Unlock certain tables in the database for write operations
   *
   * @param tableNames The tables to unlock
   * @throws HopDatabaseException
   */
  public void unlockTables(String[] tableNames) throws HopDatabaseException {
    if (Utils.isEmpty(tableNames)) {
      return;
    }

    // Quote table names too...
    //
    String[] quotedTableNames = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      quotedTableNames[i] = databaseMeta.getQuotedSchemaTableCombination(this, null, tableNames[i]);
    }

    // Get the SQL to unlock the (quoted) tables
    //
    String sql = databaseMeta.getSqlUnlockTables(quotedTableNames);
    if (sql != null) {
      execStatement(sql);
    }
  }

  /** @return the opened */
  public int getOpened() {
    return opened;
  }

  /** @param opened the opened to set */
  public synchronized void setOpened(int opened) {
    this.opened = opened;
  }

  /** @return the connectionGroup */
  public String getConnectionGroup() {
    return connectionGroup;
  }

  /** @param connectionGroup the connectionGroup to set */
  public void setConnectionGroup(String connectionGroup) {
    this.connectionGroup = connectionGroup;
  }

  /** @return the partitionId */
  public String getPartitionId() {
    return partitionId;
  }

  /** @param partitionId the partitionId to set */
  public void setPartitionId(String partitionId) {
    this.partitionId = partitionId;
  }

  /** @return the copy */
  public int getCopy() {
    return copy;
  }

  /** @param copy the copy to set */
  public synchronized void setCopy(int copy) {
    this.copy = copy;
  }

  @Override
  public void copyFrom( IVariables variables) {
    variables.copyFrom(variables);
  }

  @Override
  public String resolve( String aString) {
    return variables.resolve(aString);
  }

  @Override
  public String[] resolve( String[] aString) {
    return variables.resolve(aString);
  }

  @Override
  public String resolve( String aString, IRowMeta rowMeta, Object[] rowData)
      throws HopValueException {
    return variables.resolve(aString, rowMeta, rowData);
  }

  @Override
  public IVariables getParentVariables() {
    return variables.getParentVariables();
  }

  @Override
  public void setParentVariables( IVariables parent) {
    variables.setParentVariables(parent);
  }

  @Override
  public String getVariable(String variableName, String defaultValue) {
    return variables.getVariable(variableName, defaultValue);
  }

  @Override
  public String getVariable(String variableName) {
    return variables.getVariable(variableName);
  }

  @Override
  public boolean getVariableBoolean( String variableName, boolean defaultValue) {
    if (!Utils.isEmpty(variableName)) {
      String value = resolve(variableName);
      if (!Utils.isEmpty(value)) {
        return ValueMetaBase.convertStringToBoolean(value);
      }
    }
    return defaultValue;
  }

  @Override
  public void initializeFrom( IVariables parent) {
    variables.initializeFrom(parent);
  }

  @Override
  public String[] getVariableNames() {
    return variables.getVariableNames();
  }

  @Override
  public void setVariable(String variableName, String variableValue) {
    variables.setVariable(variableName, variableValue);
  }

  @Override
  public void shareWith( IVariables variables) {
    this.variables = variables;
  }

  @Override
  public void setVariables( Map<String, String> map ) {
    variables.setVariables( map );
  }

  public RowMetaAndData callProcedure(
      String[] arg, String[] argdir, int[] argtype, String resultname, int resulttype)
      throws HopDatabaseException {
    RowMetaAndData ret;
    try {
      boolean moreResults = cstmt.execute();
      ret = new RowMetaAndData();
      int pos = 1;
      if (resultname != null && resultname.length() != 0) {
        IValueMeta vMeta = ValueMetaFactory.createValueMeta(resultname, resulttype);
        Object v = null;
        switch (resulttype) {
          case IValueMeta.TYPE_BOOLEAN:
            v = Boolean.valueOf(cstmt.getBoolean(pos));
            break;
          case IValueMeta.TYPE_NUMBER:
            v = Double.valueOf(cstmt.getDouble(pos));
            break;
          case IValueMeta.TYPE_BIGNUMBER:
            v = cstmt.getBigDecimal(pos);
            break;
          case IValueMeta.TYPE_INTEGER:
            v = Long.valueOf(cstmt.getLong(pos));
            break;
          case IValueMeta.TYPE_STRING:
            v = cstmt.getString(pos);
            break;
          case IValueMeta.TYPE_BINARY:
            if (databaseMeta.supportsGetBlob()) {
              Blob blob = cstmt.getBlob(pos);
              if (blob != null) {
                v = blob.getBytes(1L, (int) blob.length());
              } else {
                v = null;
              }
            } else {
              v = cstmt.getBytes(pos);
            }
            break;
          case IValueMeta.TYPE_DATE:
            if (databaseMeta.supportsTimeStampToDateConversion()) {
              v = cstmt.getTimestamp(pos);
            } else {
              v = cstmt.getDate(pos);
            }
            break;
          default:
            break;
        }
        ret.addValue(vMeta, v);
        pos++;
      }
      for (int i = 0; i < arg.length; i++) {
        if (argdir[i].equalsIgnoreCase("OUT") || argdir[i].equalsIgnoreCase("INOUT")) {
          IValueMeta vMeta = ValueMetaFactory.createValueMeta(arg[i], argtype[i]);
          Object v = null;
          switch (argtype[i]) {
            case IValueMeta.TYPE_BOOLEAN:
              v = Boolean.valueOf(cstmt.getBoolean(pos + i));
              break;
            case IValueMeta.TYPE_NUMBER:
              v = Double.valueOf(cstmt.getDouble(pos + i));
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              v = cstmt.getBigDecimal(pos + i);
              break;
            case IValueMeta.TYPE_INTEGER:
              v = Long.valueOf(cstmt.getLong(pos + i));
              break;
            case IValueMeta.TYPE_STRING:
              v = cstmt.getString(pos + i);
              break;
            case IValueMeta.TYPE_BINARY:
              if (databaseMeta.supportsGetBlob()) {
                Blob blob = cstmt.getBlob(pos + i);
                if (blob != null) {
                  v = blob.getBytes(1L, (int) blob.length());
                } else {
                  v = null;
                }
              } else {
                v = cstmt.getBytes(pos + i);
              }
              break;
            case IValueMeta.TYPE_DATE:
              if (databaseMeta.supportsTimeStampToDateConversion()) {
                v = cstmt.getTimestamp(pos + i);
              } else {
                v = cstmt.getDate(pos + i);
              }
              break;
            default:
              break;
          }
          ret.addValue(vMeta, v);
        }
      }
      ResultSet rs = null;
      int updateCount = -1;

      // CHE: Iterate through the result sets and update counts
      // to receive all error messages from within the stored procedure.
      // This is only the first transform to ensure that the stored procedure
      // is properly executed. A future extension would be to return all
      // result sets and update counts properly.

      do {
        rs = null;
        try {
          // Save the result set
          if (moreResults) {
            rs = cstmt.getResultSet();

          } else {
            // Save the update count if it is available (> -1)
            updateCount = cstmt.getUpdateCount();
          }

          moreResults = cstmt.getMoreResults();

        } finally {
          if (rs != null) {
            rs.close();
            rs = null;
          }
        }

      } while (moreResults || (updateCount > -1));

      return ret;
    } catch (Exception ex) {
      throw new HopDatabaseException("Unable to call procedure", ex);
    }
  }

  public void closeProcedureStatement() throws HopDatabaseException {
    // CHE: close the callable statement involved in the stored
    // procedure call!
    try {
      if (cstmt != null) {
        cstmt.close();
        cstmt = null;
      }
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Exception.ErrorClosingCallableStatement"), ex);
    }
  }

  /**
   * Return SQL CREATION statement for a Table
   *
   * @param tableName The table to create
   * @throws HopDatabaseException
   */
  public String getDDLCreationTable(String tableName, IRowMeta fields) throws HopDatabaseException {
    String retval;

    // First, check for reserved SQL in the input row r...
    databaseMeta.quoteReservedWords(fields);
    String quotedTk = databaseMeta.quoteField(null);
    retval = getCreateTableStatement(tableName, fields, quotedTk, false, null, true);

    return retval;
  }

  /**
   * Return SQL TRUNCATE statement for a Table
   *
   * @param schema The schema
   * @param tableName The table to create
   * @throws HopDatabaseException
   */
  public String getDDLTruncateTable(String schema, String tableName) throws HopDatabaseException {
    if (Utils.isEmpty(connectionGroup)) {
      String truncateStatement = databaseMeta.getTruncateTableStatement(this, schema, tableName);
      if (truncateStatement == null) {
        throw new HopDatabaseException(
            "Truncate table not supported by " + databaseMeta.getIDatabase().getPluginName());
      }
      return truncateStatement;
    } else {
      return ("DELETE FROM "
          + databaseMeta.getQuotedSchemaTableCombination(this, schema, tableName));
    }
  }

  /**
   * Return SQL statement (INSERT INTO TableName ...
   *
   * @param schemaName tableName The schema
   * @param tableName
   * @param fields
   * @param dateFormat date format of field
   * @throws HopDatabaseException
   */
  public String getSqlOutput(
      String schemaName, String tableName, IRowMeta fields, Object[] r, String dateFormat)
      throws HopDatabaseException {
    StringBuilder ins = new StringBuilder(128);

    try {
      String schemaTable =
          databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);
      ins.append("INSERT INTO ").append(schemaTable).append('(');

      // now add the names in the row:
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          ins.append(", ");
        }
        String name = fields.getValueMeta(i).getName();
        ins.append(databaseMeta.quoteField(name));
      }
      ins.append(") VALUES (");

      java.text.SimpleDateFormat[] fieldDateFormatters =
          new java.text.SimpleDateFormat[fields.size()];

      // new add values ...
      for (int i = 0; i < fields.size(); i++) {
        IValueMeta valueMeta = fields.getValueMeta(i);
        Object valueData = r[i];

        if (i > 0) {
          ins.append(",");
        }

        // Check for null values...
        //
        if (valueMeta.isNull(valueData)) {
          ins.append("null");
        } else {
          // Normal cases...
          //
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_BOOLEAN:
            case IValueMeta.TYPE_STRING:
              String string = valueMeta.getString(valueData);
              // Have the database dialect do the quoting.
              // This also adds the single quotes around the string (thanks to
              // PostgreSQL)
              //
              string = databaseMeta.quoteSqlString(string);
              ins.append(string);
              break;
            case IValueMeta.TYPE_DATE:
              Date date = fields.getDate(r, i);

              if (Utils.isEmpty(dateFormat)) {
                if (databaseMeta.getIDatabase().isOracleVariant()) {
                  if (fieldDateFormatters[i] == null) {
                    fieldDateFormatters[i] = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                  }
                  ins.append("TO_DATE('")
                      .append(fieldDateFormatters[i].format(date))
                      .append("', 'YYYY/MM/DD HH24:MI:SS')");
                } else {
                  ins.append("'" + fields.getString(r, i) + "'");
                }
              } else {
                try {
                  java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat(dateFormat);
                  ins.append("'" + formatter.format(fields.getDate(r, i)) + "'");
                } catch (Exception e) {
                  throw new HopDatabaseException("Error : ", e);
                }
              }
              break;
            default:
              ins.append(fields.getString(r, i));
              break;
          }
        }
      }
      ins.append(')');
    } catch (Exception e) {
      throw new HopDatabaseException(e);
    }
    return ins.toString();
  }

  public Savepoint setSavepoint() throws HopDatabaseException {
    try {
      return connection.setSavepoint();
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Exception.UnableToSetSavepoint"), e);
    }
  }

  public Savepoint setSavepoint(String savePointName) throws HopDatabaseException {
    try {
      return connection.setSavepoint(savePointName);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Exception.UnableToSetSavepointName", savePointName),
          e);
    }
  }

  public void releaseSavepoint(Savepoint savepoint) throws HopDatabaseException {
    try {
      connection.releaseSavepoint(savepoint);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Exception.UnableToReleaseSavepoint"), e);
    }
  }

  public void rollback(Savepoint savepoint) throws HopDatabaseException {
    try {
      connection.rollback(savepoint);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "Database.Exception.UnableToRollbackToSavepoint"), e);
    }
  }

  public Object getParentObject() {
    return parentLoggingObject;
  }

  /**
   * Return primary key column names ...
   *
   * @param tableName
   * @throws HopDatabaseException
   */
  public String[] getPrimaryKeyColumnNames(String tableName) throws HopDatabaseException {
    List<String> names = new ArrayList<>();
    ResultSet allkeys = null;
    try {
      allkeys = getDatabaseMetaData().getPrimaryKeys(null, null, tableName);
      while (allkeys.next()) {
        String keyname = allkeys.getString("PK_NAME");
        String colName = allkeys.getString("COLUMN_NAME");
        if (!names.contains(colName)) {
          names.add(colName);
        }
        if (log.isRowLevel()) {
          log.logRowlevel(toString(), "getting key : " + keyname + " on column " + colName);
        }
      }
    } catch (SQLException e) {
      log.logError(toString(), "Error getting primary keys columns from table [" + tableName + "]");
    } finally {
      try {
        if (allkeys != null) {
          allkeys.close();
        }
      } catch (SQLException e) {
        throw new HopDatabaseException(
            "Error closing connection while searching primary keys in table [" + tableName + "]",
            e);
      }
    }
    return names.toArray(new String[names.size()]);
  }

  /**
   * Return all sequence names from connection
   *
   * @return The sequences name list.
   * @throws HopDatabaseException
   */
  public String[] getSequences() throws HopDatabaseException {
    if (databaseMeta.supportsSequences()) {
      String sql = databaseMeta.getSqlListOfSequences();
      if (sql != null) {
        List<Object[]> seqs = getRows(sql, 0);
        String[] str = new String[seqs.size()];
        for (int i = 0; i < seqs.size(); i++) {
          str[i] = seqs.get(i)[0].toString();
        }
        return str;
      }
    } else {
      throw new HopDatabaseException("Sequences are only available for Oracle databases.");
    }
    return null;
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public String getLogChannelId() {
    return log.getLogChannelId();
  }

  @Override
  public String getObjectName() {
    return databaseMeta.getName();
  }

  @Override
  public String getObjectCopy() {
    return null;
  }

  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.DATABASE;
  }

  @Override
  public ILoggingObject getParent() {
    return parentLoggingObject;
  }

  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
    log.setLogLevel(logLevel);
  }

  /** @return the serverObjectId */
  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  /** @param containerObjectId the execution container Object id to set */
  public void setContainerObjectId(String containerObjectId) {
    this.containerObjectId = containerObjectId;
  }

  /** Stub */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  /** @return the nrExecutedCommits */
  public int getNrExecutedCommits() {
    return nrExecutedCommits;
  }

  /** @param nrExecutedCommits the nrExecutedCommits to set */
  public void setNrExecutedCommits(int nrExecutedCommits) {
    this.nrExecutedCommits = nrExecutedCommits;
  }

  /**
   * Execute an SQL statement inside a file on the database connection (has to be open)
   *
   * @param filename the file containing the SQL to execute
   * @param sendSinglestatement set to true if you want to send the whole file as a single
   *     statement. If false separate statements will be isolated and executed.
   * @return a Result object indicating the number of lines read, deleted, inserted, updated, ...
   * @throws HopDatabaseException in case anything goes wrong.
   * @sendSinglestatement send one statement
   */
  public Result execStatementsFromFile(String filename, boolean sendSinglestatement)
      throws HopException {
    FileObject sqlFile = null;
    InputStream is = null;
    InputStreamReader bis = null;
    try {
      if (Utils.isEmpty(filename)) {
        throw new HopException("Filename is missing!");
      }
      sqlFile = HopVfs.getFileObject(filename);
      if (!sqlFile.exists()) {
        throw new HopException("We can not find file [" + filename + "]!");
      }

      is = HopVfs.getInputStream(sqlFile);
      bis = new InputStreamReader(new BufferedInputStream(is, 500));
      StringBuilder lineStringBuilder = new StringBuilder(256);
      lineStringBuilder.setLength(0);

      BufferedReader buff = new BufferedReader(bis);
      String sLine = null;
      String sql = Const.CR;

      while ((sLine = buff.readLine()) != null) {
        if (Utils.isEmpty(sLine)) {
          sql = sql + Const.CR;
        } else {
          sql = sql + Const.CR + sLine;
        }
      }

      if (sendSinglestatement) {
        return execStatement(sql);
      } else {
        return execStatements(sql);
      }

    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      try {
        if (sqlFile != null) {
          sqlFile.close();
        }
        if (is != null) {
          is.close();
        }
        if (bis != null) {
          bis.close();
        }
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Override
  public boolean isGatheringMetrics() {
    return log != null && log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    if (log != null) {
      log.setGatheringMetrics(gatheringMetrics);
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log != null && log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    if (log != null) {
      log.setForcingSeparateLogging(forcingSeparateLogging);
    }
  }

  // Checks to see if the HOP_COMPATIBILITY_USE_JDBC_METADATA is set.  See PDI-17980 for more
  // details.
  private boolean useJdbcMeta() {
    String useJdbcMeta =
        this.variables.getVariable(Const.HOP_COMPATIBILITY_USE_JDBC_METADATA, "false");
    return Boolean.TRUE.toString().equals(useJdbcMeta);
  }
}
