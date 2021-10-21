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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/**
 * Class providing an output transform for writing data to a cassandra table. Can create the
 * specified table (if it doesn't already exist) and can update table meta data.
 */
@Transform(
    id = "CassandraOutput",
    image = "Cassandraout.svg",
    name = "Cassandra output",
    description = "Writes to a Cassandra table",
    documentationUrl = "/pipeline/transforms/cassandra-output.html",
    keywords = "i18n::CassandraOutputMeta.keyword",
    categoryDescription = "Cassandra")
@InjectionSupported(localizationPrefix = "CassandraOutput.Injection.")
public class CassandraOutputMeta extends BaseTransformMeta
    implements ITransformMeta<CassandraOutput, CassandraOutputData> {

  public static final Class<?> PKG = CassandraOutputMeta.class;

  /** The host to contact */
  @Injection(name = "CONNECTION")
  protected String connectionName;

  /** The cassandra node to put schema updates through */
  @Injection(name = "SCHEMA_HOST")
  protected String schemaHost;

  /** The port of the cassandra node for schema updates */
  @Injection(name = "SCHEMA_PORT")
  protected String schemaPort;

  /** The table to write to */
  @Injection(name = "TABLE")
  protected String tableName = "";

  /** The consistency level to use - null or empty string result in the default */
  @Injection(name = "CONSISTENCY_LEVEL")
  protected String consistency = "";

  /**
   * The batch size - i.e. how many rows to collect before inserting them via a batch CQL statement
   */
  @Injection(name = "BATCH_SIZE")
  protected String batchSize = "100";

  /** True if unlogged (i.e. non atomic) batch writes are to be used. CQL 3 only */
  @Injection(name = "USE_UNLOGGED_BATCH")
  protected boolean useUnloggedBatch = false;

  /** Whether to create the specified table if it doesn't exist */
  @Injection(name = "CREATE_TABLE")
  protected boolean createTable = true;

  /** Anything to include in the WITH clause at table creation time? */
  @Injection(name = "CREATE_TABLE_WITH_CLAUSE")
  protected String createTableWithClause;

  /** The field in the incoming data to use as the key for inserts */
  @Injection(name = "KEY_FIELD")
  protected String keyField = "";

  /**
   * Timeout (milliseconds) to use for CQL batch inserts. If blank, no timeout is used. Otherwise,
   * whent the timeout occurs the transform will try to kill the insert and re-try after splitting
   * the batch according to the batch split factor
   */
  @Injection(name = "BATCH_TIMEOUT")
  protected String cqlBatchInsertTimeout = "";

  /**
   * Default batch split size - only comes into play if cql batch timeout has been specified.
   * Specifies the size of the sub-batches to split the batch into if a timeout occurs.
   */
  @Injection(name = "SUB_BATCH_SIZE")
  protected String cqlSubBatchSize = "10";

  /**
   * Whether or not to insert incoming fields that are not in the cassandra table's meta data. Has
   * no affect if the user has opted to update the meta data for unknown incoming fields
   */
  @Injection(name = "INSERT_FIELDS_NOT_IN_META")
  protected boolean insertFieldsNotInMeta = false;

  /** Whether or not to initially update the table meta data with any unknown incoming fields */
  @Injection(name = "UPDATE_CASSANDRA_META")
  protected boolean updateCassandraMeta = false;

  /** Whether to truncate the table before inserting */
  @Injection(name = "TRUNCATE_TABLE")
  protected boolean truncateTable = false;

  /** Time to live (TTL) for inserts (affects all fields inserted) */
  @Injection(name = "TTL")
  protected String ttl = "";

  @Injection(name = "TTL_UNIT")
  protected String ttlUnit = TtlUnits.NONE.toString();

  public enum TtlUnits {
    NONE(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.None")) {
      @Override
      int convertToSeconds(int value) {
        return -1;
      }
    },
    SECONDS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Seconds")) {
      @Override
      int convertToSeconds(int value) {
        return value;
      }
    },
    MINUTES(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Minutes")) {
      @Override
      int convertToSeconds(int value) {
        return value * 60;
      }
    },
    HOURS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Hours")) {
      @Override
      int convertToSeconds(int value) {
        return value * 60 * 60;
      }
    },
    DAYS(BaseMessages.getString(PKG, "CassandraOutput.TTLUnit.Days")) {
      @Override
      int convertToSeconds(int value) {
        return value * 60 * 60 * 24;
      }
    };

    private final String stringVal;

    TtlUnits(String name) {
      stringVal = name;
    }

    @Override
    public String toString() {
      return stringVal;
    }

    abstract int convertToSeconds(int value);
  }

  @Override
  public String getXml() {
    StringBuffer xml = new StringBuffer();

    xml.append(XmlHandler.addTagValue("connection", connectionName));
    xml.append(XmlHandler.addTagValue("schema_host", schemaHost));
    xml.append(XmlHandler.addTagValue("schema_port", schemaPort));
    xml.append(XmlHandler.addTagValue("table", tableName));
    xml.append(XmlHandler.addTagValue("key_field", keyField));
    xml.append(XmlHandler.addTagValue("consistency", consistency));
    xml.append(XmlHandler.addTagValue("batch_size", batchSize));
    xml.append(XmlHandler.addTagValue("cql_batch_timeout", cqlBatchInsertTimeout));
    xml.append(XmlHandler.addTagValue("cql_sub_batch_size", cqlSubBatchSize));
    xml.append(XmlHandler.addTagValue("insert_fields_not_in_meta", insertFieldsNotInMeta));
    xml.append(XmlHandler.addTagValue("update_cassandra_meta", updateCassandraMeta));
    xml.append(XmlHandler.addTagValue("truncate_table", truncateTable));
    xml.append(XmlHandler.addTagValue("unlogged_batch", useUnloggedBatch));

    xml.append(XmlHandler.addTagValue("create_table", createTable));
    xml.append(XmlHandler.addTagValue("create_table_with_clause", createTableWithClause));
    xml.append(XmlHandler.addTagValue("ttl", ttl));
    xml.append(XmlHandler.addTagValue("ttl_unit", ttlUnit));

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    connectionName = XmlHandler.getTagValue(transformNode, "connection");
    schemaHost = XmlHandler.getTagValue(transformNode, "schema_host");
    schemaPort = XmlHandler.getTagValue(transformNode, "schema_port");
    tableName = XmlHandler.getTagValue(transformNode, "table");
    keyField = XmlHandler.getTagValue(transformNode, "key_field");
    consistency = XmlHandler.getTagValue(transformNode, "consistency");
    batchSize = XmlHandler.getTagValue(transformNode, "batch_size");
    cqlBatchInsertTimeout = XmlHandler.getTagValue(transformNode, "cql_batch_timeout");
    cqlSubBatchSize = XmlHandler.getTagValue(transformNode, "cql_sub_batch_size");

    createTable = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "create_table"));

    insertFieldsNotInMeta =
        XmlHandler.getTagValue(transformNode, "insert_fields_not_in_meta").equalsIgnoreCase("Y");
    updateCassandraMeta =
        XmlHandler.getTagValue(transformNode, "update_cassandra_meta").equalsIgnoreCase("Y");

    truncateTable = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "truncate_table"));

    createTableWithClause = XmlHandler.getTagValue(transformNode, "create_table_with_clause");
    useUnloggedBatch =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "unlogged_batch"));
    ttl = XmlHandler.getTagValue(transformNode, "ttl");
    ttlUnit = XmlHandler.getTagValue(transformNode, "ttl_unit");

    if (Utils.isEmpty(ttlUnit)) {
      ttlUnit = TtlUnits.NONE.toString();
    }
  }

  @Override
  public void setDefault() {
    schemaHost = "localhost";
    schemaPort = "9042";
    tableName = "";
    batchSize = "100";
    insertFieldsNotInMeta = false;
    updateCassandraMeta = false;
    truncateTable = false;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      CassandraOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new CassandraOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public CassandraOutputData getTransformData() {
    return new CassandraOutputData();
  }

  /**
   * Gets schemaHost
   *
   * @return value of schemaHost
   */
  public String getSchemaHost() {
    return schemaHost;
  }

  /** @param schemaHost The schemaHost to set */
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

  /** @param schemaPort The schemaPort to set */
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

  /** @param tableName The tableName to set */
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

  /** @param consistency The consistency to set */
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

  /** @param batchSize The batchSize to set */
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

  /** @param useUnloggedBatch The useUnloggedBatch to set */
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

  /** @param createTable The createTable to set */
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

  /** @param createTableWithClause The createTableWithClause to set */
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

  /** @param keyField The keyField to set */
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

  /** @param cqlBatchInsertTimeout The cqlBatchInsertTimeout to set */
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

  /** @param cqlSubBatchSize The cqlSubBatchSize to set */
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

  /** @param insertFieldsNotInMeta The insertFieldsNotInMeta to set */
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

  /** @param updateCassandraMeta The updateCassandraMeta to set */
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

  /** @param truncateTable The truncateTable to set */
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

  /** @param ttl The ttl to set */
  public void setTtl(String ttl) {
    this.ttl = ttl;
  }

  /**
   * Gets ttlUnit
   *
   * @return value of ttlUnit
   */
  public String getTtlUnit() {
    return ttlUnit;
  }

  /** @param ttlUnit The ttlUnit to set */
  public void setTtlUnit(String ttlUnit) {
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

  /** @param connectionName The connectionName to set */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }
}
