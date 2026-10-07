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

package org.apache.hop.pipeline.transforms.maskfields.store;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;

/**
 * Mapping stored in a database table. The table holds the original value so the same input can be
 * found again. A second table, the map name plus {@code _seq}, holds the next sequence number.
 */
public class DatabaseMaskingStore implements IMaskingStore {

  static final String COLUMN_PATTERN = "pattern_name";
  static final String COLUMN_SOURCE = "source_key";
  static final String COLUMN_MASKED = "masked_value";
  static final String COLUMN_NEXT = "next_value";

  /** Length of the masked value column in a new mapping table. */
  public static final int MASKED_LENGTH = 255;

  /** Attempts of one lookup that loses a race with another process before it gives up. */
  static final int MAX_ATTEMPTS = 5;

  private final ILoggingObject parent;
  private final IVariables variables;
  private final DatabaseMeta databaseMeta;
  private final String schemaName;
  private final String tableName;
  private final Map<String, Map<String, String>> cache = new ConcurrentHashMap<>();

  private Database database;
  private String mapTable;
  private String sequenceTable;
  private String quotedPattern;
  private String quotedSource;
  private String quotedMasked;
  private String quotedNext;

  public DatabaseMaskingStore(
      ILoggingObject parent,
      IVariables variables,
      DatabaseMeta databaseMeta,
      String schemaName,
      String tableName) {
    this.parent = parent;
    this.variables = variables;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public void open() throws HopException {
    database = new Database(parent, variables, databaseMeta);
    database.connect();
    quotedPattern = databaseMeta.quoteField(COLUMN_PATTERN);
    quotedSource = databaseMeta.quoteField(COLUMN_SOURCE);
    quotedMasked = databaseMeta.quoteField(COLUMN_MASKED);
    quotedNext = databaseMeta.quoteField(COLUMN_NEXT);
    mapTable = databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
    sequenceTable =
        databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName + "_seq");
    // Create the tables before the transaction that remembers values starts.
    database.setAutoCommit(true);
    createMapTable();
    createSequenceTable();
    database.setAutoCommit(false);
  }

  @Override
  public String findOrCreate(String patternName, String sourceKey, MaskAllocator allocator)
      throws HopException {
    return findOrCreate(patternName, sourceKey, null, allocator);
  }

  /**
   * Finds or stores the replacement in one transaction. When another process stores the same source
   * key, the next sequence row or the same replacement at the same moment, the constraint violation
   * rolls the attempt back and the next attempt sees what that process committed.
   */
  @Override
  public synchronized String findOrCreate(
      String patternName, String sourceKey, KeySupplier legacyKey, MaskAllocator allocator)
      throws HopException {
    Map<String, String> patternCache =
        cache.computeIfAbsent(patternName, k -> new ConcurrentHashMap<>());
    String cached = patternCache.get(sourceKey);
    if (cached != null) {
      return cached;
    }
    ensureOpen();
    for (int attempt = 1; ; attempt++) {
      try {
        String masked = lookup(patternName, sourceKey);
        String oldKey = masked == null && legacyKey != null ? legacyKey.get() : null;
        if (oldKey != null && !oldKey.equals(sourceKey)) {
          masked = lookup(patternName, oldKey);
          if (masked != null) {
            rekey(patternName, oldKey, sourceKey);
          }
        }
        if (masked == null) {
          masked = allocator.allocate(this);
          insertMap(patternName, sourceKey, masked);
        }
        database.commit();
        patternCache.put(sourceKey, masked);
        return masked;
      } catch (HopException e) {
        rollbackQuietly();
        if (!isConstraintViolation(e)) {
          throw e;
        }
        if (attempt >= MAX_ATTEMPTS) {
          // The database's message names the conflicting key, which can be the original value.
          // Leave the cause out so that value does not reach the log.
          throw new HopException(
              "Unable to store a replacement for pattern '"
                  + patternName
                  + "' after "
                  + MAX_ATTEMPTS
                  + " attempts: other processes kept storing conflicting rows in "
                  + mapTable);
        }
      }
    }
  }

  /**
   * Next sequence value. The update locks the pattern's row until the caller commits, so a second
   * process waits and then continues from the committed value.
   */
  @Override
  public synchronized long allocateSequence(String patternName, long start) throws HopException {
    ensureOpen();
    Result result =
        database.execStatement(
            "UPDATE "
                + sequenceTable
                + " SET "
                + quotedNext
                + " = "
                + quotedNext
                + " + 1 WHERE "
                + quotedPattern
                + " = ?",
            oneString(),
            new Object[] {patternName});
    if (result.getNrLinesUpdated() > 0) {
      Long next = queryLong(selectNextSql(), oneString(), new Object[] {patternName});
      if (next == null) {
        throw new HopException("The sequence row for pattern '" + patternName + "' disappeared");
      }
      return next - 1;
    }
    exec(
        "INSERT INTO "
            + sequenceTable
            + " ("
            + quotedPattern
            + ", "
            + quotedNext
            + ") VALUES (?, ?)",
        stringAndLong(),
        new Object[] {patternName, start + 1});
    return start;
  }

  @Override
  public synchronized int count(String patternName) throws HopException {
    ensureOpen();
    Long count =
        queryLong(
            "SELECT COUNT(*) FROM " + mapTable + " WHERE " + quotedPattern + " = ?",
            oneString(),
            new Object[] {patternName});
    return count == null ? 0 : count.intValue();
  }

  @Override
  public synchronized void close() {
    cache.clear();
    if (database != null) {
      database.disconnect();
      database = null;
    }
  }

  private void createMapTable() throws HopException {
    if (database.checkTableExists(schemaName, tableName)) {
      return;
    }
    // Primary-key columns have to be NOT NULL. The generic CREATE TABLE leaves them nullable, and
    // H2 rejects a primary key on a nullable column. The unique key stops two source values from
    // sharing a replacement, which keeps the masked value short enough to index on every database.
    // Tables created by earlier versions keep their layout.
    database.execStatement(
        "CREATE TABLE "
            + mapTable
            + " ("
            + columnDefinition(stringColumn(COLUMN_PATTERN, 128))
            + ", "
            + columnDefinition(stringColumn(COLUMN_SOURCE, 255))
            + ", "
            + columnDefinition(stringColumn(COLUMN_MASKED, MASKED_LENGTH))
            + ", PRIMARY KEY ("
            + quotedPattern
            + ", "
            + quotedSource
            + "), UNIQUE ("
            + quotedPattern
            + ", "
            + quotedMasked
            + "))");
  }

  private void createSequenceTable() throws HopException {
    if (database.checkTableExists(schemaName, tableName + "_seq")) {
      return;
    }
    ValueMetaInteger next = new ValueMetaInteger(COLUMN_NEXT);
    next.setLength(18);
    database.execStatement(
        "CREATE TABLE "
            + sequenceTable
            + " ("
            + columnDefinition(stringColumn(COLUMN_PATTERN, 128))
            + ", "
            + columnDefinition(next)
            + ", PRIMARY KEY ("
            + quotedPattern
            + "))");
  }

  private String columnDefinition(IValueMeta value) {
    String type =
        databaseMeta
            .getIDatabase()
            .getFieldDefinition(value, null, null, false, false, false)
            .trim();
    return databaseMeta.quoteField(value.getName()) + " " + type + " NOT NULL";
  }

  private String lookup(String patternName, String sourceKey) throws HopException {
    return queryString(
        "SELECT "
            + quotedMasked
            + " FROM "
            + mapTable
            + " WHERE "
            + quotedPattern
            + " = ? AND "
            + quotedSource
            + " = ?",
        twoStrings(),
        new Object[] {patternName, sourceKey});
  }

  private void insertMap(String patternName, String sourceKey, String masked) throws HopException {
    exec(
        "INSERT INTO "
            + mapTable
            + " ("
            + quotedPattern
            + ", "
            + quotedSource
            + ", "
            + quotedMasked
            + ") VALUES (?, ?, ?)",
        threeStrings(),
        new Object[] {patternName, sourceKey, masked});
  }

  private void rekey(String patternName, String oldKey, String newKey) throws HopException {
    exec(
        "UPDATE "
            + mapTable
            + " SET "
            + quotedSource
            + " = ? WHERE "
            + quotedPattern
            + " = ? AND "
            + quotedSource
            + " = ?",
        rekeyParameters(),
        new Object[] {newKey, patternName, oldKey});
  }

  private String selectNextSql() {
    return "SELECT " + quotedNext + " FROM " + sequenceTable + " WHERE " + quotedPattern + " = ?";
  }

  private void exec(String sql, IRowMeta params, Object[] data) throws HopException {
    database.execStatement(sql, params, data);
  }

  private String queryString(String sql, IRowMeta params, Object[] data) throws HopException {
    Object value = firstValue(sql, params, data);
    return value == null ? null : value.toString();
  }

  private Long queryLong(String sql, IRowMeta params, Object[] data) throws HopException {
    Object value = firstValue(sql, params, data);
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    return Long.valueOf(value.toString());
  }

  private Object firstValue(String sql, IRowMeta params, Object[] data) throws HopException {
    RowMetaAndData row = database.getOneRow(sql, params, data);
    if (row == null || row.getData() == null || row.getData().length == 0) {
      return null;
    }
    return row.getData()[0];
  }

  private void ensureOpen() throws HopException {
    if (database == null) {
      throw new HopException("The mapping database is not open");
    }
  }

  private void rollbackQuietly() {
    try {
      database.rollback();
    } catch (HopDatabaseException e) {
      // The original failure is the one the caller sees.
    }
  }

  static boolean isConstraintViolation(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof SQLException sqlException) {
        String state = sqlException.getSQLState();
        if ("23505".equals(state)) {
          return true;
        }
      }
      String message = current.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("unique")
            || lower.contains("primary key")
            || lower.contains("duplicate")) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  private static ValueMetaString stringColumn(String name, int length) {
    ValueMetaString value = new ValueMetaString(name);
    value.setLength(length);
    return value;
  }

  private static RowMeta oneString() {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString(COLUMN_PATTERN));
    return meta;
  }

  private static RowMeta twoStrings() {
    RowMeta meta = oneString();
    meta.addValueMeta(new ValueMetaString(COLUMN_SOURCE));
    return meta;
  }

  private static RowMeta threeStrings() {
    RowMeta meta = twoStrings();
    meta.addValueMeta(new ValueMetaString(COLUMN_MASKED));
    return meta;
  }

  private static RowMeta rekeyParameters() {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString(COLUMN_SOURCE));
    meta.addValueMeta(new ValueMetaString(COLUMN_PATTERN));
    meta.addValueMeta(new ValueMetaString("old_" + COLUMN_SOURCE));
    return meta;
  }

  private static IRowMeta stringAndLong() {
    RowMeta meta = oneString();
    meta.addValueMeta(new ValueMetaInteger(COLUMN_NEXT));
    return meta;
  }
}
