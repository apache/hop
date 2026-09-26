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

package org.apache.hop.execution.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.execution.IExecutionSelector;
import org.apache.hop.execution.LastPeriod;
import org.apache.hop.execution.caching.BaseCachingExecutionInfoLocation;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Caches execution information and persists each top-level pipeline/workflow {@link CacheEntry} as
 * one row in a relational table: filter columns for queries, a JSON document written once (project
 * metadata and pipeline XML), and a state document that a local single-writer updates afterwards.
 */
@GuiPlugin(description = "Caching Database execution information location GUI elements")
@ExecutionInfoLocationPlugin(
    id = "caching-database-location",
    name = "Caching Database location",
    description =
        "Aggregates and caches execution information before storing in a relational database")
@Getter
@Setter
public class CachingDatabaseExecutionInfoLocation extends BaseCachingExecutionInfoLocation
    implements IExecutionInfoLocation {

  public static final Class<?> PKG = CachingDatabaseExecutionInfoLocation.class;

  private static final ObjectMapper JSON_MAPPER = HopJson.newMapper();

  public static final String PLUGIN_ID = "caching-database-location";
  public static final String DEFAULT_TABLE_NAME = "hop_executions";

  public static final String COL_ID = "id";
  public static final String COL_NAME = "name";
  public static final String COL_EXECUTION_TYPE = "execution_type";
  public static final String COL_PARENT_ID = "parent_id";
  public static final String COL_REGISTRATION_DATE = "registration_date";
  public static final String COL_EXECUTION_START_DATE = "execution_start_date";
  public static final String COL_EXECUTION_END_DATE = "execution_end_date";
  public static final String COL_FAILED = "failed";
  public static final String COL_STATUS_DESCRIPTION = "status_description";
  public static final String COL_DURATION_MS = "duration_ms";
  public static final String COL_JSON = "json";

  /**
   * Live state, child states and samples. {@link #COL_JSON} stays as inserted so a long run does
   * not bind the project metadata again.
   */
  public static final String COL_STATE_JSON = "state_json";

  @GuiWidgetElement(
      id = "connectionName",
      order = "010",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      metadata = DatabaseMeta.class,
      toolTip = "i18n::CachingDatabaseExecutionInfoLocation.Connection.Tooltip",
      label = "i18n::CachingDatabaseExecutionInfoLocation.Connection.Label")
  @HopMetadataProperty(key = "connection")
  protected String connectionName;

  @GuiWidgetElement(
      id = "schemaName",
      order = "020",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingDatabaseExecutionInfoLocation.SchemaName.Tooltip",
      label = "i18n::CachingDatabaseExecutionInfoLocation.SchemaName.Label")
  @HopMetadataProperty
  protected String schemaName;

  @GuiWidgetElement(
      id = "tableName",
      order = "030",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::CachingDatabaseExecutionInfoLocation.TableName.Tooltip",
      label = "i18n::CachingDatabaseExecutionInfoLocation.TableName.Label")
  @HopMetadataProperty
  protected String tableName = DEFAULT_TABLE_NAME;

  /** Loaded database metadata (from connection name or injected for tests). */
  protected transient DatabaseMeta databaseMeta;

  protected transient Database database;

  /** Guards the live {@link #database}. Not the connection itself: that object is replaced. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private final Object dbLock = new Object();

  /** True after {@link #close()} so a late timer tick cannot open another connection. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private volatile boolean databaseClosed = true;

  protected String actualConnectionName;
  protected String actualSchemaName;
  protected String actualTableName;

  /** False only when an existing table could not grow the state column. New DDL includes it. */
  private boolean stateJsonColumnAvailable = true;

  public CachingDatabaseExecutionInfoLocation() {
    super();
  }

  public CachingDatabaseExecutionInfoLocation(CachingDatabaseExecutionInfoLocation location) {
    super(location);
    this.connectionName = location.connectionName;
    this.schemaName = location.schemaName;
    this.tableName = location.tableName;
    this.databaseMeta = location.databaseMeta;
    this.actualConnectionName = location.actualConnectionName;
    this.actualSchemaName = location.actualSchemaName;
    this.actualTableName = location.actualTableName;
  }

  @Override
  public CachingDatabaseExecutionInfoLocation clone() {
    return new CachingDatabaseExecutionInfoLocation(this);
  }

  @Override
  public synchronized void initialize(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    actualConnectionName = variables != null ? variables.resolve(connectionName) : connectionName;
    actualSchemaName =
        variables != null
            ? variables.resolve(Const.NVL(schemaName, ""))
            : Const.NVL(schemaName, "");
    String resolvedTable =
        variables != null ? variables.resolve(Const.NVL(tableName, DEFAULT_TABLE_NAME)) : tableName;
    actualTableName = StringUtils.isEmpty(resolvedTable) ? DEFAULT_TABLE_NAME : resolvedTable;

    if (databaseMeta == null) {
      if (StringUtils.isEmpty(actualConnectionName)) {
        throw new HopException(
            "Caching database execution information location has no database connection configured");
      }
      if (metadataProvider == null) {
        throw new HopException(
            "No metadata provider available to load database connection '"
                + actualConnectionName
                + "'");
      }
      try {
        databaseMeta =
            metadataProvider.getSerializer(DatabaseMeta.class).load(actualConnectionName);
      } catch (Exception e) {
        throw new HopException(
            "Error loading database connection '" + actualConnectionName + "'", e);
      }
      if (databaseMeta == null) {
        throw new HopException(
            "Database connection '" + actualConnectionName + "' could not be found");
      }
    }

    synchronized (dbLock) {
      databaseClosed = false;
      discardDatabase();
      connectDatabase();
      ensureStateJsonColumn();
    }

    try {
      super.initialize(variables, metadataProvider);
    } catch (Exception e) {
      synchronized (dbLock) {
        databaseClosed = true;
        discardDatabase();
      }
      if (e instanceof HopException hopException) {
        throw hopException;
      }
      throw new HopException(
          "Error starting the caching database execution information location", e);
    }
    LogChannel.GENERAL.logBasic(
        "Caching database execution info location ready: connection="
            + Const.NVL(actualConnectionName, databaseMeta.getName())
            + ", table="
            + getQuotedSchemaTable());
  }

  @Override
  public synchronized void close() throws HopException {
    if (databaseClosed) {
      return;
    }
    try {
      super.close();
    } finally {
      // A failed flush leaves the dirty entries in the map. Keep the connection so close() can
      // retry them. Drop it once nothing unsaved remains.
      if (!hasDirtyCacheEntries()) {
        synchronized (dbLock) {
          databaseClosed = true;
          discardDatabase();
        }
      }
    }
  }

  private boolean hasDirtyCacheEntries() {
    for (CacheEntry entry : getCache().values()) {
      if (entry != null && entry.isDirty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Open an independent auto-commit connection. A workflow transaction connection is closed when
   * the workflow stops, which left the cache timer writing through that closed connection.
   */
  private void connectDatabase() throws HopException {
    Database db =
        new Database(
            new LoggingObject("CachingDatabaseExecutionInfoLocation"), variables, databaseMeta);
    db.setConnectionGroup(null);
    try {
      db.connect();
      db.setConnectionGroup(null);
      enableAutoCommit(db);
    } catch (Exception e) {
      try {
        db.setConnectionGroup(null);
        db.disconnect();
      } catch (Exception disconnectError) {
        // The original connect error is the one that matters.
      }
      throw new HopException(
          "Error connecting to database for execution information location using connection '"
              + Const.NVL(actualConnectionName, databaseMeta.getName())
              + "'",
          e);
    }
    database = db;
  }

  /**
   * Keep writes out of a workflow transaction. Drivers that reject the change keep the connection;
   * the connection group was already cleared, which is what stops the workflow from closing it.
   */
  private static void enableAutoCommit(Database db) {
    try {
      Connection connection = db.getConnection();
      if (connection != null && !connection.getAutoCommit()) {
        db.setAutoCommit(true);
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logBasic(
          "Execution information connection stays with the driver's commit mode: "
              + e.getMessage());
    }
  }

  /** Drop the current connection. A grouped connection is left for the workflow to close. */
  private void discardDatabase() {
    Database current = database;
    database = null;
    if (current == null || StringUtils.isNotEmpty(current.getConnectionGroup())) {
      return;
    }
    try {
      current.disconnect();
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error disconnecting database for execution information location", e);
    }
  }

  private void ensureConnected() throws HopException {
    if (isConnectionUsable(database) && !databaseClosed) {
      return;
    }
    synchronized (dbLock) {
      if (databaseClosed) {
        throw new HopException("Caching database execution information location is closed");
      }
      if (isConnectionUsable(database)) {
        return;
      }
      discardDatabase();
      connectDatabase();
    }
  }

  private static boolean isConnectionUsable(Database db) {
    if (db == null) {
      return false;
    }
    try {
      Connection connection = db.getConnection();
      return connection != null && !connection.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * PostgreSQL reports a client-side close as {@code 08003} / "This connection has been closed."
   */
  static boolean isClosedConnectionFailure(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof SQLException sqlException) {
        String state = sqlException.getSQLState();
        if ("08003".equals(state) || "08006".equals(state) || "57P01".equals(state)) {
          return true;
        }
      }
      String message = current.getMessage();
      if (message != null) {
        String lower = message.toLowerCase();
        if (lower.contains("connection")
            && (lower.contains("closed") || lower.contains("broken"))) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  @FunctionalInterface
  private interface DatabaseWork<T> {
    T run() throws Exception;
  }

  private <T> T callWithDatabase(DatabaseWork<T> work) throws HopException {
    return callWithDatabase(work, true);
  }

  private <T> T callWithDatabase(DatabaseWork<T> work, boolean allowRetry) throws HopException {
    try {
      ensureConnected();
      synchronized (dbLock) {
        ensureConnected();
        return work.run();
      }
    } catch (Exception e) {
      if (allowRetry && !databaseClosed && isClosedConnectionFailure(e)) {
        synchronized (dbLock) {
          discardDatabase();
        }
        return callWithDatabase(work, false);
      }
      if (e instanceof HopException hopException) {
        throw hopException;
      }
      throw new HopException("Error accessing the execution information database", e);
    }
  }

  @Override
  protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
    if (databaseClosed) {
      throw new HopException("Caching database execution information location is closed");
    }
    try {
      cacheEntry.calculateSummary();
      boolean lightUpdate = cacheEntry.isSingleWriter() && cacheEntry.isHeavyDocumentStored();
      if (lightUpdate) {
        callWithDatabase(
            () -> {
              if (rowExists(cacheEntry.getId())) {
                if (stateJsonColumnAvailable) {
                  updateLightState(cacheEntry);
                } else {
                  updateStatusColumns(cacheEntry);
                }
                return null;
              }
              // The row was removed. Write the document we still have.
              writeFullDocument(cacheEntry);
              return null;
            });
      } else {
        // Another process may have added children. A local single-writer inserts once and then
        // never reads the CLOB back.
        if (!cacheEntry.isSingleWriter()) {
          mergeChildrenFromDatabase(cacheEntry);
        }
        callWithDatabase(
            () -> {
              writeFullDocument(cacheEntry);
              return null;
            });
        if (cacheEntry.isSingleWriter()) {
          releaseHeavyDocument(cacheEntry);
          cacheEntry.setHeavyDocumentStored(true);
        }
      }

      cacheEntry.setDirty(false);
      cacheEntry.setLastWritten(new Date());
    } catch (Exception e) {
      throw new HopException(
          "Error writing cache entry to database table " + getQuotedSchemaTable(), e);
    }
  }

  /**
   * Multi-writer safe: union child maps from the existing row so concurrent processes do not wipe
   * samples/children written by others (same idea as Caching File).
   */
  private void mergeChildrenFromDatabase(CacheEntry cacheEntry) {
    if (cacheEntry == null || StringUtils.isEmpty(cacheEntry.getId())) {
      return;
    }
    try {
      CacheEntry existing = loadCacheEntry(cacheEntry.getId());
      if (existing == null) {
        return;
      }
      mergeMap(existing.getChildExecutions(), cacheEntry.getChildExecutions());
      mergeMap(existing.getChildExecutionStates(), cacheEntry.getChildExecutionStates());
      mergeMap(existing.getChildExecutionData(), cacheEntry.getChildExecutionData());
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Unable to merge on-database cache entry before persist (non-fatal): " + e.getMessage());
    }
  }

  private static <K, V> void mergeMap(Map<K, V> fromStore, Map<K, V> into) {
    if (fromStore == null || fromStore.isEmpty() || into == null) {
      return;
    }
    for (Map.Entry<K, V> e : fromStore.entrySet()) {
      into.putIfAbsent(e.getKey(), e.getValue());
    }
  }

  private boolean rowExists(String id) throws HopException {
    String sql =
        "SELECT 1 FROM "
            + getQuotedSchemaTable()
            + " WHERE "
            + databaseMeta.quoteField(COL_ID)
            + " = ?";
    IRowMeta paramMeta = new RowMeta();
    paramMeta.addValueMeta(new ValueMetaString(COL_ID, 100, -1));
    RowMetaAndData one = database.getOneRow(sql, paramMeta, new Object[] {id});
    return one != null && one.getData() != null;
  }

  private void writeFullDocument(CacheEntry cacheEntry) throws HopException {
    String json = serializeCacheEntry(cacheEntry);
    IRowMeta rowMeta = createDataRowMeta();
    // A full write is the source of truth, so drop any older state overlay.
    Object[] data = buildRowData(cacheEntry, json, null);
    upsertCacheEntry(rowMeta, data);
  }

  /**
   * Status columns and the live state document. The inserted JSON, including project metadata and
   * pipeline XML, is left untouched and is not read back.
   */
  private void updateLightState(CacheEntry cacheEntry) throws HopException {
    String stateJson = serializeWithoutHeavyDocument(cacheEntry);
    Object[] data = buildRowData(cacheEntry, null, stateJson);
    executeUpdate(lightUpdateFields(), data);
  }

  /** Status columns only, when the table has no state column. */
  private void updateStatusColumns(CacheEntry cacheEntry) throws HopException {
    Object[] data = buildRowData(cacheEntry, null, null);
    executeUpdate(statusUpdateFields(), data);
  }

  private String serializeWithoutHeavyDocument(CacheEntry cacheEntry) throws HopException {
    Execution execution = cacheEntry.getExecution();
    String metadata = null;
    String executorXml = null;
    if (execution != null) {
      metadata = execution.getMetadataJson();
      executorXml = execution.getExecutorXml();
      execution.setMetadataJson(null);
      execution.setExecutorXml(null);
    }
    try {
      return serializeCacheEntry(cacheEntry);
    } finally {
      if (execution != null) {
        execution.setMetadataJson(metadata);
        execution.setExecutorXml(executorXml);
      }
    }
  }

  private static String serializeCacheEntry(CacheEntry cacheEntry) throws HopException {
    try {
      return JSON_MAPPER.writeValueAsString(cacheEntry);
    } catch (Exception e) {
      throw new HopException("Error serializing cache entry '" + cacheEntry.getId() + "'", e);
    }
  }

  private static String[] statusUpdateFields() {
    return new String[] {
      COL_NAME,
      COL_EXECUTION_TYPE,
      COL_PARENT_ID,
      COL_REGISTRATION_DATE,
      COL_EXECUTION_START_DATE,
      COL_EXECUTION_END_DATE,
      COL_FAILED,
      COL_STATUS_DESCRIPTION,
      COL_DURATION_MS
    };
  }

  private String[] lightUpdateFields() {
    String[] status = statusUpdateFields();
    String[] fields = new String[status.length + 1];
    System.arraycopy(status, 0, fields, 0, status.length);
    fields[status.length] = COL_STATE_JSON;
    return fields;
  }

  private String[] fullUpdateFields() {
    String[] light = stateJsonColumnAvailable ? lightUpdateFields() : statusUpdateFields();
    String[] fields = new String[light.length + 1];
    System.arraycopy(light, 0, fields, 0, light.length);
    fields[light.length] = COL_JSON;
    return fields;
  }

  /** prepareUpdate binds SET fields first, then the WHERE value. */
  private void executeUpdate(String[] setFields, Object[] data) throws HopException {
    String[] codes = new String[] {COL_ID};
    String[] conditions = new String[] {"="};
    if (!database.prepareUpdate(actualSchemaName, actualTableName, codes, conditions, setFields)) {
      throw new HopException("Unable to prepare update for table " + getQuotedSchemaTable());
    }
    try {
      IRowMeta rowMeta = createDataRowMeta();
      IRowMeta updateMeta = new RowMeta();
      Object[] updateData = new Object[setFields.length + 1];
      for (int i = 0; i < setFields.length; i++) {
        int index = rowMeta.indexOfValue(setFields[i]);
        if (index < 0) {
          throw new HopException("Unknown execution information column '" + setFields[i] + "'");
        }
        updateData[i] = data[index];
        updateMeta.addValueMeta(rowMeta.getValueMeta(index));
      }
      int idIndex = rowMeta.indexOfValue(COL_ID);
      updateData[setFields.length] = data[idIndex];
      updateMeta.addValueMeta(rowMeta.getValueMeta(idIndex));
      database.setValuesUpdate(updateMeta, updateData);
      database.updateRow();
    } finally {
      database.closeUpdate();
    }
  }

  private static void releaseHeavyDocument(CacheEntry cacheEntry) {
    Execution execution = cacheEntry.getExecution();
    if (execution == null) {
      return;
    }
    execution.setMetadataJson(null);
    execution.setExecutorXml(null);
  }

  /** Upsert: if a row with the same id exists UPDATE, otherwise INSERT. */
  private void upsertCacheEntry(IRowMeta rowMeta, Object[] data) throws HopException {
    String id = (String) data[0];
    if (rowExists(id)) {
      executeUpdate(fullUpdateFields(), data);
    } else {
      database.insertRow(actualSchemaName, actualTableName, rowMeta, data);
    }
  }

  @Override
  protected CacheEntry loadCacheEntry(String executionId) throws HopException {
    try {
      String columns = databaseMeta.quoteField(COL_JSON);
      if (stateJsonColumnAvailable) {
        columns += ", " + databaseMeta.quoteField(COL_STATE_JSON);
      }
      String sql =
          "SELECT "
              + columns
              + " FROM "
              + getQuotedSchemaTable()
              + " WHERE "
              + databaseMeta.quoteField(COL_ID)
              + " = ?";
      IRowMeta paramMeta = new RowMeta();
      paramMeta.addValueMeta(new ValueMetaString(COL_ID, 100, -1));

      return callWithDatabase(
          () -> {
            RowMetaAndData row = database.getOneRow(sql, paramMeta, new Object[] {executionId});
            if (row == null || row.getData() == null || row.getData()[0] == null) {
              return null;
            }
            CacheEntry entry = JSON_MAPPER.readValue(row.getData()[0].toString(), CacheEntry.class);
            if (stateJsonColumnAvailable
                && row.getData().length > 1
                && row.getData()[1] != null
                && StringUtils.isNotEmpty(row.getData()[1].toString())) {
              applyStateOverlay(
                  entry, JSON_MAPPER.readValue(row.getData()[1].toString(), CacheEntry.class));
            }
            return entry;
          });
    } catch (Exception e) {
      throw new HopException(
          "Error loading execution information from database for executionId '" + executionId + "'",
          e);
    }
  }

  @Override
  public void deleteCacheEntry(CacheEntry cacheEntry) throws HopException {
    if (cacheEntry == null || StringUtils.isEmpty(cacheEntry.getId())) {
      return;
    }
    try {
      String sql =
          "DELETE FROM "
              + getQuotedSchemaTable()
              + " WHERE "
              + databaseMeta.quoteField(COL_ID)
              + " = ?";
      IRowMeta paramMeta = new RowMeta();
      paramMeta.addValueMeta(new ValueMetaString(COL_ID, 100, -1));
      callWithDatabase(
          () -> {
            database.execStatement(sql, paramMeta, new Object[] {cacheEntry.getId()});
            return null;
          });
    } catch (Exception e) {
      throw new HopException(
          "Error deleting execution information from database for id '" + cacheEntry.getId() + "'",
          e);
    }
  }

  @Override
  protected void retrieveIds(
      boolean includeChildren, Set<DatedId> ids, int limit, IExecutionSelector selector)
      throws HopException {
    final IExecutionSelector activeSelector = selector == null ? IExecutionSelector.ALL : selector;
    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT ")
          .append(databaseMeta.quoteField(COL_ID))
          .append(", ")
          .append(databaseMeta.quoteField(COL_EXECUTION_START_DATE))
          .append(" FROM ")
          .append(getQuotedSchemaTable());

      List<Object> params = new ArrayList<>();
      IRowMeta paramMeta = new RowMeta();
      List<String> where = new ArrayList<>();

      appendSelectorFilters(activeSelector, where, paramMeta, params);

      if (!where.isEmpty()) {
        sql.append(" WHERE ");
        sql.append(String.join(" AND ", where));
      }

      sql.append(" ORDER BY ")
          .append(databaseMeta.quoteField(COL_EXECUTION_START_DATE))
          .append(" DESC");

      if (limit > 0) {
        sql.append(databaseMeta.getLimitClause(limit));
      }

      callWithDatabase(
          () -> {
            List<DatedId> parentIds = new ArrayList<>();
            ResultSet rs = database.openQuery(sql.toString(), paramMeta, params.toArray());
            try {
              Object[] row = database.getRow(rs);
              while (row != null) {
                String id = row[0] != null ? row[0].toString() : null;
                Date startDate = null;
                if (row[1] instanceof Date date) {
                  startDate = date;
                } else if (row[1] != null) {
                  // Timestamp / other
                  startDate = (Date) row[1];
                }
                if (id != null) {
                  parentIds.add(new DatedId(id, startDate != null ? startDate : new Date(0L)));
                }
                row = database.getRow(rs);
              }
            } finally {
              database.closeQuery(rs);
            }

            ids.addAll(parentIds);
            if (includeChildren && !activeSelector.isSelectingParents()) {
              for (DatedId parentDatedId : parentIds) {
                CacheEntry entry = loadCacheEntry(parentDatedId.getId());
                if (entry != null) {
                  addChildIds(entry, ids, activeSelector);
                }
              }
            }
            return null;
          });
    } catch (Exception e) {
      throw new HopException(
          "Error finding execution ids from database table " + getQuotedSchemaTable(), e);
    }
  }

  private void appendSelectorFilters(
      IExecutionSelector selector, List<String> where, IRowMeta paramMeta, List<Object> params) {
    if (selector == IExecutionSelector.ALL) {
      return;
    }

    LastPeriod dateFilter = selector.startDateFilter();
    if (dateFilter != null && dateFilter != LastPeriod.NONE) {
      LocalDateTime start = dateFilter.calculateStartDate();
      Date startDate = Date.from(start.atZone(ZoneId.systemDefault()).toInstant());
      where.add(databaseMeta.quoteField(COL_EXECUTION_START_DATE) + " >= ?");
      paramMeta.addValueMeta(new ValueMetaDate(COL_EXECUTION_START_DATE));
      params.add(startDate);
    }

    if (selector.isSelectingPipelines()) {
      where.add(databaseMeta.quoteField(COL_EXECUTION_TYPE) + " = ?");
      paramMeta.addValueMeta(new ValueMetaString(COL_EXECUTION_TYPE, 32, -1));
      params.add("Pipeline");
    }
    if (selector.isSelectingWorkflows()) {
      where.add(databaseMeta.quoteField(COL_EXECUTION_TYPE) + " = ?");
      paramMeta.addValueMeta(new ValueMetaString(COL_EXECUTION_TYPE, 32, -1));
      params.add("Workflow");
    }
    if (selector.isSelectingFailed()) {
      where.add(databaseMeta.quoteField(COL_FAILED) + " = ?");
      paramMeta.addValueMeta(new ValueMetaBoolean(COL_FAILED));
      params.add(Boolean.TRUE);
    }
    if (selector.isSelectingFinished()) {
      where.add("LOWER(" + databaseMeta.quoteField(COL_STATUS_DESCRIPTION) + ") LIKE ?");
      paramMeta.addValueMeta(new ValueMetaString(COL_STATUS_DESCRIPTION, 128, -1));
      params.add("finished%");
    }
    if (selector.isSelectingRunning()) {
      where.add(
          "(LOWER("
              + databaseMeta.quoteField(COL_STATUS_DESCRIPTION)
              + ") LIKE ? OR LOWER("
              + databaseMeta.quoteField(COL_STATUS_DESCRIPTION)
              + ") LIKE ?)");
      paramMeta.addValueMeta(new ValueMetaString(COL_STATUS_DESCRIPTION, 128, -1));
      params.add("running%");
      paramMeta.addValueMeta(new ValueMetaString(COL_STATUS_DESCRIPTION + "2", 128, -1));
      params.add("initializing%");
    }
    if (selector.isSelectingParents()) {
      where.add(
          "("
              + databaseMeta.quoteField(COL_PARENT_ID)
              + " IS NULL OR "
              + databaseMeta.quoteField(COL_PARENT_ID)
              + " = '')");
    }

    String filterText = selector.filterText();
    if (StringUtils.isNotEmpty(filterText) && !selector.isSelectingByUuid()) {
      String like = "%" + filterText.toLowerCase() + "%";
      where.add(
          "(LOWER("
              + databaseMeta.quoteField(COL_NAME)
              + ") LIKE ? OR LOWER("
              + databaseMeta.quoteField(COL_ID)
              + ") LIKE ?)");
      paramMeta.addValueMeta(new ValueMetaString(COL_NAME, 1024, -1));
      params.add(like);
      paramMeta.addValueMeta(new ValueMetaString(COL_ID, 100, -1));
      params.add(like);
    }
  }

  protected IRowMeta createDataRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(COL_ID, 100, -1));
    rowMeta.addValueMeta(new ValueMetaString(COL_NAME, 1024, -1));
    rowMeta.addValueMeta(new ValueMetaString(COL_EXECUTION_TYPE, 32, -1));
    rowMeta.addValueMeta(new ValueMetaString(COL_PARENT_ID, 100, -1));
    rowMeta.addValueMeta(new ValueMetaDate(COL_REGISTRATION_DATE));
    rowMeta.addValueMeta(new ValueMetaDate(COL_EXECUTION_START_DATE));
    rowMeta.addValueMeta(new ValueMetaDate(COL_EXECUTION_END_DATE));
    rowMeta.addValueMeta(new ValueMetaBoolean(COL_FAILED));
    rowMeta.addValueMeta(new ValueMetaString(COL_STATUS_DESCRIPTION, 128, -1));
    // length 15 → BIGINT on most dialects (default Integer length maps to tinyint on H2)
    rowMeta.addValueMeta(new ValueMetaInteger(COL_DURATION_MS, 15, 0));
    // CLOB for the CacheEntry JSON written on insert
    rowMeta.addValueMeta(new ValueMetaString(COL_JSON, DatabaseMeta.CLOB_LENGTH, -1));
    if (stateJsonColumnAvailable) {
      rowMeta.addValueMeta(new ValueMetaString(COL_STATE_JSON, DatabaseMeta.CLOB_LENGTH, -1));
    }
    return rowMeta;
  }

  private Object[] buildRowData(CacheEntry cacheEntry, String json, String stateJson) {
    Execution execution = cacheEntry.getExecution();
    ExecutionState state = cacheEntry.getExecutionState();

    String name = cacheEntry.getName();
    if (StringUtils.isEmpty(name) && execution != null) {
      name = execution.getName();
    }
    String executionType =
        execution != null && execution.getExecutionType() != null
            ? execution.getExecutionType().name()
            : null;
    String parentId = execution != null ? execution.getParentId() : null;
    Date registrationDate = execution != null ? execution.getRegistrationDate() : null;
    Date startDate = execution != null ? execution.getExecutionStartDate() : null;
    Date endDate = state != null ? state.getExecutionEndDate() : null;
    Boolean failed = state != null ? state.isFailed() : null;
    String status = state != null ? state.getStatusDescription() : null;
    Long durationMs =
        cacheEntry.getSummary() != null ? cacheEntry.getSummary().getDurationMs() : null;

    if (!stateJsonColumnAvailable) {
      return new Object[] {
        cacheEntry.getId(),
        name,
        executionType,
        parentId,
        registrationDate,
        startDate,
        endDate,
        failed,
        status,
        durationMs,
        json
      };
    }
    return new Object[] {
      cacheEntry.getId(),
      name,
      executionType,
      parentId,
      registrationDate,
      startDate,
      endDate,
      failed,
      status,
      durationMs,
      json,
      stateJson
    };
  }

  /**
   * The state document is a later snapshot of the same cache entry without project metadata or
   * pipeline XML. Copy the parts that move onto the inserted document.
   */
  private static void applyStateOverlay(CacheEntry target, CacheEntry state) {
    if (target == null || state == null) {
      return;
    }
    if (state.getExecutionState() != null) {
      target.setExecutionState(state.getExecutionState());
    }
    if (state.getChildExecutionStates() != null) {
      target.setChildExecutionStates(state.getChildExecutionStates());
    }
    if (state.getChildExecutionData() != null) {
      target.setChildExecutionData(state.getChildExecutionData());
    }
    if (state.getSummary() != null) {
      target.setSummary(state.getSummary());
    }
    target.setDirty(false);
  }

  /** Existing tables predate the state column. Add it so a long run can update state in place. */
  private void ensureStateJsonColumn() {
    if (database == null || databaseMeta == null) {
      return;
    }
    try {
      if (!database.checkTableExists(actualSchemaName, actualTableName)) {
        return;
      }
      if (database.checkColumnExists(actualSchemaName, actualTableName, COL_STATE_JSON)) {
        stateJsonColumnAvailable = true;
        return;
      }
      String sql =
          databaseMeta.getAddColumnStatement(
              getQuotedSchemaTable(),
              new ValueMetaString(COL_STATE_JSON, DatabaseMeta.CLOB_LENGTH, -1),
              "",
              false,
              "",
              false);
      database.execStatement(sql);
      stateJsonColumnAvailable = true;
      LogChannel.GENERAL.logBasic(
          "Added column "
              + COL_STATE_JSON
              + " to execution information table "
              + getQuotedSchemaTable());
    } catch (Exception e) {
      stateJsonColumnAvailable = false;
      LogChannel.GENERAL.logError(
          "Unable to add column "
              + COL_STATE_JSON
              + " to "
              + getQuotedSchemaTable()
              + ". Later updates keep the status columns only and leave execution state in the inserted document.",
          e);
    }
  }

  protected String getQuotedSchemaTable() {
    if (database != null) {
      return databaseMeta.getQuotedSchemaTableCombination(
          database, actualSchemaName, actualTableName);
    }
    return databaseMeta.getQuotedSchemaTableCombination(
        variables, actualSchemaName, actualTableName);
  }

  /**
   * Build dialect-specific CREATE TABLE + CREATE INDEX DDL for the configured connection and table.
   */
  public String buildDdl(IVariables vars) throws HopException {
    DatabaseMeta meta = databaseMeta;
    if (meta == null) {
      if (StringUtils.isEmpty(connectionName)) {
        throw new HopException("Please select a database connection first");
      }
      String conn = vars != null ? vars.resolve(connectionName) : connectionName;
      if (metadataProvider == null && HopGui.getInstance() != null) {
        metadataProvider = HopGui.getInstance().getMetadataProvider();
      }
      if (metadataProvider == null) {
        throw new HopException("No metadata provider available to load connection '" + conn + "'");
      }
      meta = metadataProvider.getSerializer(DatabaseMeta.class).load(conn);
      if (meta == null) {
        throw new HopException("Database connection '" + conn + "' could not be found");
      }
    }

    String schema =
        vars != null ? vars.resolve(Const.NVL(schemaName, "")) : Const.NVL(schemaName, "");
    String table =
        vars != null
            ? vars.resolve(Const.NVL(tableName, DEFAULT_TABLE_NAME))
            : Const.NVL(tableName, DEFAULT_TABLE_NAME);
    if (StringUtils.isEmpty(table)) {
      table = DEFAULT_TABLE_NAME;
    }

    Database db =
        new Database(new LoggingObject("CachingDatabaseExecutionInfoLocation"), vars, meta);
    // No connect required for DDL generation
    String schemaTable = meta.getQuotedSchemaTableCombination(vars, schema, table);

    IRowMeta fields = createDataRowMeta();
    StringBuilder ddl = new StringBuilder();
    ddl.append(db.getCreateTableStatement(schemaTable, fields, null, false, COL_ID, true));
    ddl.append(Const.CR);

    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_start", COL_EXECUTION_START_DATE);
    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_name", COL_NAME);
    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_type", COL_EXECUTION_TYPE);
    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_failed", COL_FAILED);
    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_parent", COL_PARENT_ID);
    addIndexDdl(ddl, db, schemaTable, "idx_hop_exec_status", COL_STATUS_DESCRIPTION);

    return ddl.toString();
  }

  private void addIndexDdl(
      StringBuilder ddl, Database db, String schemaTable, String indexName, String... columns) {
    String indexSql =
        db.getCreateIndexStatement(schemaTable, indexName, columns, false, false, false, true);
    if (StringUtils.isNotEmpty(indexSql)) {
      ddl.append(indexSql);
      if (!indexSql.trim().endsWith(";")) {
        ddl.append(";");
      }
      ddl.append(Const.CR);
    }
  }

  /** Show dialect-specific CREATE TABLE / CREATE INDEX DDL (Neo4j-style, does not execute). */
  @GuiWidgetElement(
      id = "showDdlButton",
      order = "035",
      parentId = ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      label = "i18n::CachingDatabaseExecutionInfoLocation.ShowDdl.Label",
      toolTip = "i18n::CachingDatabaseExecutionInfoLocation.ShowDdl.Tooltip")
  public void showDdlButton(Object object) {
    HopGui hopGui = HopGui.getInstance();
    CachingDatabaseExecutionInfoLocation location = (CachingDatabaseExecutionInfoLocation) object;
    try {
      location.metadataProvider = hopGui.getMetadataProvider();
      String ddl = location.buildDdl(hopGui.getVariables());
      EnterTextDialog dialog =
          new EnterTextDialog(
              hopGui.getShell(),
              BaseMessages.getString(PKG, "CachingDatabaseExecutionInfoLocation.DdlDialog.Title"),
              BaseMessages.getString(PKG, "CachingDatabaseExecutionInfoLocation.DdlDialog.Message"),
              ddl);
      dialog.setReadOnly();
      dialog.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error generating DDL for the execution information table",
          e);
    }
  }

  @Override
  public String getPluginId() {
    return PLUGIN_ID;
  }

  @Override
  public void setPluginId(String pluginId) {
    // Don't set anything
  }

  @Override
  public String getPluginName() {
    return "Caching Database location";
  }

  @Override
  public void setPluginName(String pluginName) {
    // Nothing to set
  }
}
