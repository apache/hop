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
package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.pipeline.transform.BaseTransformMeta;

/**
 * Class providing an output transform for writing data to a cassandra table. Can create the
 * specified table (if it doesn't already exist) and can update table meta data.
 */
@Transform(
    id = "CassandraOutput",
    image = "cassandraout.svg",
    name = "Cassandra output",
    description = "Writes to a Cassandra table",
    documentationUrl = "/pipeline/transforms/cassandra-output.html",
    keywords = "i18n::CassandraOutputMeta.keyword",
    categoryDescription = "Cassandra")
public class CassandraOutputMeta extends BaseTransformMeta<CassandraOutput, CassandraOutputData> {

  public static final Class<?> PKG = CassandraOutputMeta.class;

  /** The host to contact */
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION",
      injectionKeyDescription = "CassandraOutput.Injection.CONNECTION")
  protected String connectionName;

  /** The cassandra node to put schema updates through */
  @HopMetadataProperty(
      key = "schema_host",
      injectionKey = "SCHEMA_HOST",
      injectionKeyDescription = "CassandraOutput.Injection.SCHEMA_HOST")
  protected String schemaHost;

  /** The port of the cassandra node for schema updates */
  @HopMetadataProperty(
      key = "schema_port",
      injectionKey = "SCHEMA_PORT",
      injectionKeyDescription = "CassandraOutput.Injection.SCHEMA_PORT")
  protected String schemaPort;

  /** The table to write to */
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLE",
      injectionKeyDescription = "CassandraOutput.Injection.TABLE")
  protected String tableName = "";

  /** The consistency level to use - null or empty string result in the default */
  @HopMetadataProperty(
      key = "consistency",
      injectionKey = "CONSISTENCY_LEVEL",
      injectionKeyDescription = "CassandraOutput.Injection.CONSISTENCY_LEVEL")
  protected String consistency = "";

  /**
   * The batch size - i.e. how many rows to collect before inserting them via a batch CQL statement
   */
  @HopMetadataProperty(
      key = "batch_size",
      injectionKey = "BATCH_SIZE",
      injectionKeyDescription = "CassandraOutput.Injection.BATCH_SIZE")
  protected String batchSize = "100";

  /** True if unlogged (i.e. non atomic) batch writes are to be used. CQL 3 only */
  @HopMetadataProperty(
      key = "unlogged_batch",
      injectionKey = "USE_UNLOGGED_BATCH",
      injectionKeyDescription = "CassandraOutput.Injection.USE_UNLOGGED_BATCH")
  protected boolean useUnloggedBatch = false;

  /** Whether to create the specified table if it doesn't exist */
  @HopMetadataProperty(
      key = "create_table",
      injectionKey = "CREATE_TABLE",
      injectionKeyDescription = "CassandraOutput.Injection.CREATE_TABLE")
  protected boolean createTable = true;

  /** Anything to include in the WITH clause at table creation time? */
  @HopMetadataProperty(
      key = "create_table_with_clause",
      injectionKey = "CREATE_TABLE_WITH_CLAUSE",
      injectionKeyDescription = "CassandraOutput.Injection.CREATE_TABLE_WITH_CLAUSE")
  protected String createTableWithClause;

  /** The field in the incoming data to use as the key for inserts */
  @HopMetadataProperty(
      key = "key_field",
      injectionKey = "KEY_FIELD",
      injectionKeyDescription = "CassandraOutput.Injection.KEY_FIELD")
  protected String keyField = "";

  /**
   * Timeout (milliseconds) to use for CQL batch inserts. If blank, no timeout is used. Otherwise,
   * whent the timeout occurs the transform will try to kill the insert and re-try after splitting
   * the batch according to the batch split factor
   */
  @HopMetadataProperty(
      key = "cql_batch_timeout",
      injectionKey = "BATCH_TIMEOUT",
      injectionKeyDescription = "CassandraOutput.Injection.BATCH_TIMEOUT")
  protected String cqlBatchInsertTimeout = "";

  /**
   * Default batch split size - only comes into play if cql batch timeout has been specified.
   * Specifies the size of the sub-batches to split the batch into if a timeout occurs.
   */
  @HopMetadataProperty(
      key = "cql_sub_batch_size",
      injectionKey = "SUB_BATCH_SIZE",
      injectionKeyDescription = "CassandraOutput.Injection.SUB_BATCH_SIZE")
  protected String cqlSubBatchSize = "10";

  /**
   * Whether or not to insert incoming fields that are not in the cassandra table's meta data. Has
   * no affect if the user has opted to update the meta data for unknown incoming fields
   */
  @HopMetadataProperty(
      key = "insert_fields_not_in_meta",
      injectionKey = "INSERT_FIELDS_NOT_IN_META",
      injectionKeyDescription = "CassandraOutput.Injection.INSERT_FIELDS_NOT_IN_META")
  protected boolean insertFieldsNotInMeta = false;

  /** Whether or not to initially update the table meta data with any unknown incoming fields */
  @HopMetadataProperty(
      key = "update_cassandra_meta",
      injectionKey = "UPDATE_CASSANDRA_META",
      injectionKeyDescription = "CassandraOutput.Injection.UPDATE_CASSANDRA_META")
  protected boolean updateCassandraMeta = false;

  /** Whether to truncate the table before inserting */
  @HopMetadataProperty(
      key = "truncate_table",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "CassandraOutput.Injection.TRUNCATE_TABLE")
  protected boolean truncateTable = false;

  /** Time to live (TTL) for inserts (affects all fields inserted) */
  @HopMetadataProperty(
      key = "ttl",
      injectionKey = "TTL",
      injectionKeyDescription = "CassandraOutput.Injection.TTL")
  protected String ttl = "";

  @HopMetadataProperty(
      key = "ttl_unit",
      injectionKey = "TTL_UNIT",
      injectionKeyDescription = "CassandraOutput.Injection.TTL_UNIT",
      storeWithCode = true)
  protected TtlUnits ttlUnit = TtlUnits.NONE;

  @Override
  public void setDefault() {
    schemaHost = "";
    schemaPort = "";
    tableName = "";
    batchSize = "10";
    insertFieldsNotInMeta = false;
    updateCassandraMeta = false;
    truncateTable = false;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public enum TtlUnits implements IEnumHasCodeAndDescription {
    NONE(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.None")) {
      @Override
      long convertToSeconds(long value) {
        return -1L;
      }
    },
    SECONDS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Seconds")) {
      @Override
      long convertToSeconds(long value) {
        return value;
      }
    },
    MINUTES(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Minutes")) {
      @Override
      long convertToSeconds(long value) {
        return value * 60L;
      }
    },
    HOURS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Hours")) {
      @Override
      long convertToSeconds(long value) {
        return value * 60L * 60L;
      }
    },
    DAYS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Days")) {
      @Override
      long convertToSeconds(long value) {
        return value * 60L * 60L * 24L;
      }
    };

    private final String code;
    private final String description;

    TtlUnits(String description) {
      this.code = name();
      this.description = description;
    }

    public static TtlUnits findWithDescription(String description) {
      for (TtlUnits value : values()) {
        if (value.getDescription().equals(description)) {
          return value;
        }
      }
      return NONE;
    }

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].description;
      }
      return descriptions;
    }

    @Override
    public String getCode() {
      return code;
    }

    public String getDescription() {
      return description;
    }

    abstract long convertToSeconds(long value);
  }

  /**
   * Gets schemaHost
   *
   * @return value of schemaHost
   */
  public String getSchemaHost() {
    return schemaHost;
  }

  /**
   * @param schemaHost The schemaHost to set
   */
  public void setSchemaHost(String schemaHost) {
    this.schemaHost = schemaHost;
  }

  /**
   * Gets schemaPort
   *
   * @return value of schemaPort
   */
  public String getSchemaPort() {
    return schemaPort;
  }

  /**
   * @param schemaPort The schemaPort to set
   */
  public void setSchemaPort(String schemaPort) {
    this.schemaPort = schemaPort;
  }

  /**
   * Gets tableName
   *
   * @return value of tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Gets consistency
   *
   * @return value of consistency
   */
  public String getConsistency() {
    return consistency;
  }

  /**
   * @param consistency The consistency to set
   */
  public void setConsistency(String consistency) {
    this.consistency = consistency;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize The batchSize to set
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets useUnloggedBatch
   *
   * @return value of useUnloggedBatch
   */
  public boolean isUseUnloggedBatch() {
    return useUnloggedBatch;
  }

  /**
   * @param useUnloggedBatch The useUnloggedBatch to set
   */
  public void setUseUnloggedBatch(boolean useUnloggedBatch) {
    this.useUnloggedBatch = useUnloggedBatch;
  }

  /**
   * Gets createTable
   *
   * @return value of createTable
   */
  public boolean isCreateTable() {
    return createTable;
  }

  /**
   * @param createTable The createTable to set
   */
  public void setCreateTable(boolean createTable) {
    this.createTable = createTable;
  }

  /**
   * Gets createTableWithClause
   *
   * @return value of createTableWithClause
   */
  public String getCreateTableWithClause() {
    return createTableWithClause;
  }

  /**
   * @param createTableWithClause The createTableWithClause to set
   */
  public void setCreateTableWithClause(String createTableWithClause) {
    this.createTableWithClause = createTableWithClause;
  }

  /**
   * Gets keyField
   *
   * @return value of keyField
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /**
   * Gets cqlBatchInsertTimeout
   *
   * @return value of cqlBatchInsertTimeout
   */
  public String getCqlBatchInsertTimeout() {
    return cqlBatchInsertTimeout;
  }

  /**
   * @param cqlBatchInsertTimeout The cqlBatchInsertTimeout to set
   */
  public void setCqlBatchInsertTimeout(String cqlBatchInsertTimeout) {
    this.cqlBatchInsertTimeout = cqlBatchInsertTimeout;
  }

  /**
   * Gets cqlSubBatchSize
   *
   * @return value of cqlSubBatchSize
   */
  public String getCqlSubBatchSize() {
    return cqlSubBatchSize;
  }

  /**
   * @param cqlSubBatchSize The cqlSubBatchSize to set
   */
  public void setCqlSubBatchSize(String cqlSubBatchSize) {
    this.cqlSubBatchSize = cqlSubBatchSize;
  }

  /**
   * Gets insertFieldsNotInMeta
   *
   * @return value of insertFieldsNotInMeta
   */
  public boolean isInsertFieldsNotInMeta() {
    return insertFieldsNotInMeta;
  }

  /**
   * @param insertFieldsNotInMeta The insertFieldsNotInMeta to set
   */
  public void setInsertFieldsNotInMeta(boolean insertFieldsNotInMeta) {
    this.insertFieldsNotInMeta = insertFieldsNotInMeta;
  }

  /**
   * Gets updateCassandraMeta
   *
   * @return value of updateCassandraMeta
   */
  public boolean isUpdateCassandraMeta() {
    return updateCassandraMeta;
  }

  /**
   * @param updateCassandraMeta The updateCassandraMeta to set
   */
  public void setUpdateCassandraMeta(boolean updateCassandraMeta) {
    this.updateCassandraMeta = updateCassandraMeta;
  }

  /**
   * Gets truncateTable
   *
   * @return value of truncateTable
   */
  public boolean isTruncateTable() {
    return truncateTable;
  }

  /**
   * @param truncateTable The truncateTable to set
   */
  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  /**
   * Gets ttl
   *
   * @return value of ttl
   */
  public String getTtl() {
    return ttl;
  }

  /**
   * @param ttl The ttl to set
   */
  public void setTtl(String ttl) {
    this.ttl = ttl;
  }

  /**
   * Gets ttlUnit
   *
   * @return value of ttlUnit
   */
  public TtlUnits getTtlUnit() {
    return ttlUnit;
  }

  /**
   * @param ttlUnit The ttlUnit to set
   */
  public void setTtlUnit(TtlUnits ttlUnit) {
    this.ttlUnit = ttlUnit;
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * Sets connectionName
   *
   * @param connectionName value of connectionName
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }
}
