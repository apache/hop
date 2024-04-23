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
 *
 */
package org.apache.hop.databases.cassandra.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.spi.CqlRowHandler;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;

public class DriverKeyspace implements Keyspace {

  protected DriverConnection conn;
  private KeyspaceMetadata meta;
  private String name;

  public DriverKeyspace(DriverConnection conn, KeyspaceMetadata keyspace) {
    this.meta = keyspace;
    this.conn = conn;
    this.name = keyspace.getName().asCql(false);
  }

  public void setConnection(DriverConnection conn) throws Exception {
    this.conn = conn;
  }

  public DriverConnection getConnection() {
    return conn;
  }

  @Override
  public void setKeyspace(String keyspaceName) throws Exception {
    this.name = keyspaceName;
  }

  public String getName() {
    return name;
  }

  @Override
  public void setOptions(Map<String, String> options) {
    conn.setAdditionalOptions(options);
  }

  @Override
  public void executeCQL(String cql, String compression, String consistencyLevel, ILogChannel log)
      throws Exception {
    conn.getSession(name).execute(cql);
  }

  @Override
  public void createKeyspace(String keyspaceName, Map<String, Object> options, ILogChannel log)
      throws Exception {
    SchemaBuilder.createKeyspace(keyspaceName);
  }

  @Override
  public List<String> getTableNamesCQL3() throws Exception {
    List<String> names = new ArrayList<>();
    meta.getTables().keySet().forEach(ti -> names.add(ti.asCql(false)));
    return names;
  }

  @Override
  public boolean tableExists(String tableName) throws Exception {
    return meta.getTable(tableName).isPresent();
  }

  @Override
  public ITableMetaData getTableMetaData(String familyName) throws Exception {
    Optional<TableMetadata> optionalTable = meta.getTable(familyName);
    if (optionalTable.isEmpty()) {
      return null;
    }
    TableMetadata tableMeta = optionalTable.get();
    return new TableMetaData(this, tableMeta);
  }

  @Override
  public boolean createTable(
      String tableName,
      IRowMeta rowMeta,
      List<Integer> keyIndexes,
      String createTableWithClause,
      ILogChannel log)
      throws Exception {
    CreateTableStart createTableStart = SchemaBuilder.createTable(tableName).ifNotExists();

    CreateTable createTable = null;
    for (int i = 0; i < rowMeta.size(); i++) {
      if (keyIndexes.contains(i)) {
        IValueMeta key = rowMeta.getValueMeta(i);
        createTable =
            createTableStart.withPartitionKey(
                key.getName(), CassandraUtils.getCassandraDataTypeFromValueMeta(key));
      }
    }

    if (createTable == null) {
      throw new HopException("Please specify one or more keys fields");
    }

    for (int i = 0; i < rowMeta.size(); i++) {
      if (!keyIndexes.contains(i)) {
        IValueMeta valueMeta = rowMeta.getValueMeta(i);
        createTable =
            createTable.withColumn(
                valueMeta.getName(), CassandraUtils.getCassandraDataTypeFromValueMeta(valueMeta));
      }
    }

    CqlSession session = getSession();
    if (!Utils.isEmpty(createTableWithClause)) {
      StringBuilder cql = new StringBuilder(createTable.asCql());
      if (!createTableWithClause.toLowerCase().trim().startsWith("with")) {
        cql.append(" WITH ");
      }
      cql.append(createTableWithClause);
      session.execute(cql.toString());
    } else {
      session.execute(createTable.asCql());
    }
    return true;
  }

  /** Actually an ALTER to add columns, not UPDATE. Purpose of keyIndexes yet to be determined */
  @Override
  public void updateTableCQL3(
      String tableName, IRowMeta rowMeta, List<Integer> keyIndexes, ILogChannel log)
      throws Exception {
    CqlSession session = getSession();
    ITableMetaData table = getTableMetaData(tableName);
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      if (!table.columnExistsInSchema(valueMeta.getName())) {
        DataType dataType = CassandraUtils.getCassandraDataTypeFromValueMeta(valueMeta);
        String cql =
            SchemaBuilder.alterTable(tableName).addColumn(valueMeta.getName(), dataType).asCql();
        session.execute(cql);
      }
    }
  }

  @Override
  public void truncateTable(String tableName, ILogChannel log) throws Exception {
    getSession().execute(QueryBuilder.truncate(tableName).asCql());
  }

  protected CqlSession getSession() {
    return conn.getSession(name);
  }

  @Override
  public CqlRowHandler getCQLRowHandler() {
    return new DriverCqlRowHandler(this, getSession(), getConnection().isExpandCollection());
  }
}
