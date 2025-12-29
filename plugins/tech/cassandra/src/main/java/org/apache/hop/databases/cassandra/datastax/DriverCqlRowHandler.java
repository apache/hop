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
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.databases.cassandra.spi.CqlRowHandler;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.pipeline.transform.ITransform;

public class DriverCqlRowHandler implements CqlRowHandler {

  private final CqlSession session;
  DriverKeyspace keyspace;
  ResultSet result;

  ColumnDefinitions columns;

  private boolean unloggedBatch = true;

  private int ttlSec;

  public DriverCqlRowHandler(
      DriverKeyspace keyspace, CqlSession session, boolean expandCollection) {
    this.keyspace = keyspace;
    this.session = session;
  }

  public DriverCqlRowHandler(DriverKeyspace keyspace) {
    this(keyspace, keyspace.getConnection().getSession(keyspace.getName()), true);
  }

  @Override
  public void setKeyspace(Keyspace keyspace) {
    this.keyspace = (DriverKeyspace) keyspace;
  }

  @Override
  public void newRowQuery(
      ITransform requestingTransform,
      String tableName,
      String cqlQuery,
      String compress,
      String consistencyLevel,
      ILogChannel log)
      throws Exception {
    result = getSession().execute(cqlQuery);
    columns = result.getColumnDefinitions();
  }

  @Override
  public Object[][] getNextOutputRow(IRowMeta outputRowMeta, Map<String, Integer> outputFormatMap)
      throws Exception {
    if (result == null || !result.iterator().hasNext()) {
      result = null;
      columns = null;
      return null;
    }
    Row row = result.one();
    Object[][] outputRowData = new Object[1][];
    Object[] baseOutputRowData =
        RowDataUtil.allocateRowData(Math.max(outputRowMeta.size(), columns.size()));
    for (int i = 0; i < columns.size(); i++) {
      baseOutputRowData[i] = readValue(outputRowMeta.getValueMeta(i), row, i);
    }
    outputRowData[0] = baseOutputRowData;
    return outputRowData;
  }

  public static Object readValue(IValueMeta meta, Row row, int i) {
    if (row.isNull(i)) {
      return null;
    }

    return switch (meta.getType()) {
      case IValueMeta.TYPE_INTEGER -> row.getLong(i);
      case IValueMeta.TYPE_NUMBER -> row.getDouble(i);
      case IValueMeta.TYPE_BIGNUMBER -> row.get(i, GenericType.BIG_DECIMAL);
      case IValueMeta.TYPE_DATE -> row.get(i, GenericType.of(Date.class));
      case IValueMeta.TYPE_TIMESTAMP -> row.get(i, GenericType.of(Timestamp.class));
      default -> row.getObject(i);
    };
  }

  public static Object[] readRow(IRowMeta rowMeta, Row row) {
    Object[] hopRow = RowDataUtil.allocateRowData(rowMeta.size());
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      hopRow[i] = readValue(valueMeta, row, i);
    }
    return hopRow;
  }

  public void batchInsert(
      IRowMeta inputMeta,
      Iterable<Object[]> rows,
      ITableMetaData tableMeta,
      String consistencyLevel,
      boolean insertFieldsNotInMetadata)
      throws Exception {
    String[] columnNames = getColumnNames(inputMeta);

    InsertInto insertInto = QueryBuilder.insertInto(tableMeta.getTableName());
    RegularInsert insert = null;
    Set<Integer> excludedIndexes = new HashSet<>();
    for (int index = 0; index < columnNames.length; index++) {
      String columnName = columnNames[index];

      // See if the column is present in the table
      //
      if (insertFieldsNotInMetadata || tableMeta.columnExistsInSchema(columnName)) {
        if (insert == null) {
          insert = insertInto.value(columnName, QueryBuilder.bindMarker());
        } else {
          insert = insert.value(columnName, QueryBuilder.bindMarker());
        }
      } else {
        excludedIndexes.add(index);
      }
    }

    if (insert == null) {
      throw new HopException("No fields found to insert");
    }

    // Add all bound statements to a batch
    //
    BatchStatementBuilder batchBuilder =
        new BatchStatementBuilder(unloggedBatch ? BatchType.UNLOGGED : BatchType.LOGGED);
    PreparedStatement preparedInsert = getSession().prepare(insert.asCql());

    for (Object[] row : rows) {
      Object[] bindRow = new Object[columnNames.length - excludedIndexes.size()];
      int bindIndex = 0;
      for (int index = 0; index < columnNames.length; index++) {
        if (!excludedIndexes.contains(index)) {
          bindRow[bindIndex++] = row[index];
        }
      }
      BoundStatement statement = preparedInsert.bind(bindRow);
      batchBuilder.addStatement(statement);
    }

    // Set the consistency level (if any)
    //
    if (StringUtils.isNotEmpty(consistencyLevel)) {
      batchBuilder =
          batchBuilder.setConsistencyLevel(DefaultConsistencyLevel.valueOf(consistencyLevel));
    }

    // Execute the batch statement
    //
    BatchStatement batch = batchBuilder.build();

    session.execute(batch);

    /*

      TODO: set write timeout somewhere

    if (batchInsertTimeout > 0) {
      try {
        getSession()
            .executeAsync(batch)
            .getUninterruptibly(batchInsertTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        log.logError(
            BaseMessages.getString(
                DriverCqlRowHandler.class, "DriverCqlRowHandler.Error.TimeoutReached"));
      }
    } else {
      getSession().execute(batch);
    }*/
  }

  private String[] getColumnNames(IRowMeta inputMeta) {
    String[] columns = new String[inputMeta.size()];
    for (int i = 0; i < inputMeta.size(); i++) {
      columns[i] = inputMeta.getValueMeta(i).getName();
    }
    return columns;
  }

  @Override
  public void commitCQLBatch(
      ITransform requestingTransform,
      StringBuilder batch,
      String compress,
      String consistencyLevel,
      ILogChannel log)
      throws Exception {
    throw new NotImplementedException();
  }

  public void setUnloggedBatch(boolean unloggedBatch) {
    this.unloggedBatch = unloggedBatch;
  }

  public boolean isUnloggedBatch() {
    return unloggedBatch;
  }

  private CqlSession getSession() {
    return session;
  }

  public void setTtlSec(int ttl) {
    this.ttlSec = ttl;
  }
}
