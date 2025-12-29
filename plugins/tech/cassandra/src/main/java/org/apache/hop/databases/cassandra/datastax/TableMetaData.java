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

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.Selector;

public class TableMetaData implements ITableMetaData {

  private TableMetadata meta;

  private String name;

  // expand collection values into multiple rows (behaviour of other implementation)
  private boolean expandCollection = true;

  public TableMetaData(DriverKeyspace keyspace, TableMetadata metadata) {
    meta = metadata;
    name = meta.getName().asCql(false);
    setKeyspace(keyspace);
  }

  @Override
  public void setKeyspace(Keyspace keyspace) {
    DriverKeyspace keyspace1 = (DriverKeyspace) keyspace;
    expandCollection = keyspace1.getConnection().isExpandCollection();
  }

  @Override
  public void setTableName(String tableName) {
    this.name = tableName;
  }

  @Override
  public String getTableName() {
    return name;
  }

  @Override
  public String describe() throws Exception {
    return meta.toString(); // TODO verify this
  }

  @Override
  public boolean columnExistsInSchema(String colName) {
    return meta.getColumn(colName).isPresent();
  }

  @Override
  public IValueMeta getValueMetaForKey() {
    List<ColumnMetadata> partKeys = meta.getPartitionKey();
    if (partKeys.size() > 1) {
      return new ValueMetaString("KEY");
    }
    ColumnMetadata key = partKeys.get(0);
    return toValueMeta(key.getName().asCql(false), key.getType());
  }

  @Override
  public List<String> getKeyColumnNames() {
    List<String> names = new ArrayList<>();
    meta.getPartitionKey().forEach(cm -> names.add(cm.getName().asCql(false)));
    return names;
  }

  @Override
  public IValueMeta getValueMetaForColumn(String colName) throws HopException {
    Optional<ColumnMetadata> optionalColumn = meta.getColumn(colName);
    if (optionalColumn.isEmpty()) {
      throw new HopException("Column " + colName + " is not present");
    }
    return getValueMetaForColumn(optionalColumn.get());
  }

  protected IValueMeta getValueMetaForColumn(ColumnMetadata column) {
    if (column != null) {
      String name = column.getName().asCql(false);
      if (name.startsWith("\"")) {
        name = name.substring(1);
      }
      if (name.endsWith("\"")) {
        name = name.substring(0, name.length() - 1);
      }
      return toValueMeta(name, column.getType());
    }
    return new ValueMetaString(name);
  }

  @Override
  public List<IValueMeta> getValueMetasForSchema() {
    List<IValueMeta> values = new ArrayList<>();
    Collection<ColumnMetadata> columns = meta.getColumns().values();
    for (ColumnMetadata column : columns) {
      values.add(getValueMetaForColumn(column));
    }
    return values;
  }

  @Override
  public IValueMeta getValueMeta(Selector selector) throws HopException {
    String name = selector.getColumnName();
    return getValueMetaForColumn(name);
  }

  @Override
  public List<String> getColumnNames() {
    Collection<ColumnMetadata> colMeta = meta.getColumns().values();
    List<String> colNames = new ArrayList<>();
    for (ColumnMetadata c : colMeta) {
      colNames.add(c.getName().asCql(false));
    }

    return colNames;
  }

  @Override
  public DataType getColumnCQLType(String colName) {
    ColumnMetadata columnMetadata = meta.getColumn(colName).get();
    return columnMetadata.getType();
  }

  public static IValueMeta toValueMeta(String name, DataType dataType) {
    //    if (expandCollection
    //        && dataType.isCollection()
    //        && dataType.getName().equals(DataType.Name.MAP)) {
    //      dataType = dataType.getTypeArguments().get(0);
    //    }
    // http://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html

    String typeCql = dataType.asCql(false, false).toUpperCase();
    return switch (typeCql) {
      case "BIGINT", "COUNTER", "INT", "SMALLINT", "TINYINT" -> new ValueMetaInteger(name);
      case "DOUBLE", "FLOAT" -> new ValueMetaNumber(name);
      case "DATE", "TIMESTAMP" -> new ValueMetaDate(name);
      case "DECIMAL", "VARINT" -> new ValueMetaBigNumber(name);
      case "BLOB" -> new ValueMetaBinary(name);
      case "BOOLEAN" -> new ValueMetaBoolean(name);
      default -> new ValueMetaString(name);
    };
  }
}
