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
package org.apache.hop.pgvector.util;

import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta;

public final class PgVectorDatabase {

  private static final Class<?> PKG =
      org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta.class;

  private PgVectorDatabase() {}

  public static Database connect(
      ILoggingObject loggingObject,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String connectionName)
      throws HopException {
    if (metadataProvider == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorDatabase.Error.MetadataProviderRequired"));
    }
    String resolvedName = variables.resolve(connectionName);
    org.apache.hop.core.database.DatabaseMeta databaseMeta =
        metadataProvider
            .getSerializer(org.apache.hop.core.database.DatabaseMeta.class)
            .load(resolvedName);
    if (databaseMeta == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "PgVectorDatabase.Error.ConnectionNotFound", resolvedName));
    }
    Database database = new Database(loggingObject, variables, databaseMeta);
    database.connect();
    return database;
  }

  public static void ensureSchema(
      Database database,
      PgVectorUpsertMeta meta,
      String schemaName,
      String tableName,
      VectorDistanceMetric indexMetric)
      throws HopDatabaseException {
    database.execStatement(PgVectorSqlBuilder.createExtensionSql());
    List<PgVectorTableColumn> columns = PgVectorSchemaBuilder.tableColumns(meta);
    String qualifiedTable = PgVectorSqlBuilder.qualifiedTable(schemaName, tableName);
    if (meta.isCreateTableIfMissing()) {
      database.execStatement(
          PgVectorSqlBuilder.createTableSql(
              qualifiedTable, columns, meta.getEmbeddingDimensions()));
    }
    ensureMappedColumns(database, meta, qualifiedTable);
    if (meta.isCreateHnswIndex()) {
      database.execStatement(PgVectorSqlBuilder.createHnswIndexSql(qualifiedTable, indexMetric));
    }
  }

  /** Adds mapped TEXT columns when the table already exists from an earlier schema version. */
  static void ensureMappedColumns(Database database, PgVectorUpsertMeta meta, String qualifiedTable)
      throws HopDatabaseException {
    if (meta.getColumnMappings() == null) {
      return;
    }
    for (PgVectorColumnMapping mapping : meta.getColumnMappings()) {
      if (mapping == null || mapping.getColumnName() == null) {
        continue;
      }
      String column = PgVectorSchemaBuilder.normalizeColumnName(mapping.getColumnName());
      if (PgVectorSchemaBuilder.isReservedColumn(column)) {
        continue;
      }
      database.execStatement(
          PgVectorSqlBuilder.addColumnSql(
              qualifiedTable,
              new PgVectorTableColumn(column, PgVectorColumnType.TEXT, false),
              meta.getEmbeddingDimensions()));
    }
  }
}
