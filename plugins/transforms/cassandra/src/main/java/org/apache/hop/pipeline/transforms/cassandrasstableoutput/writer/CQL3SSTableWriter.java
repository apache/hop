/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.cassandrasstableoutput.writer;

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Map;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.databases.cassandra.util.CassandraUtils;

class CQL3SSTableWriter extends AbstractSSTableWriter {
  private CQLSSTableWriter writer;
  private IRowMeta rowMeta;

  @Override
  public void init() throws Exception {
    // Allow table to be reloaded
    purgeSchemaInstance();
    writer = getCQLSSTableWriter();
  }

  void purgeSchemaInstance() {
    // Since the unload function only cares about the keyspace and table name,
    // the partition key and class don't matter (however, creating the CFMetaData
    // will fail unless something is passed in
    CFMetaData cfm =
        CFMetaData.Builder.create(getKeyspace(), getTable())
            .withPartitioner(CassandraUtils.getPartitionerClassInstance(getPartitionerClass()))
            .addPartitionKey(getPartitionKey(), UTF8Type.instance)
            .build();
    Schema.instance.unload(cfm);
  }

  CQLSSTableWriter getCQLSSTableWriter() {
    return CQLSSTableWriter.builder()
        .inDirectory(getDirectory())
        .forTable(buildCreateTableCQLStatement())
        .using(buildInsertCQLStatement())
        .withBufferSizeInMB(getBufferSize())
        .build();
  }

  @Override
  public void processRow(Map<String, Object> record) throws Exception {
    writer.addRow(record);
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  public void setRowMeta(IRowMeta rowMeta) {
    this.rowMeta = rowMeta;
  }

  String buildCreateTableCQLStatement() {
    StringBuilder tableColumnsSpecification = new StringBuilder();
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      tableColumnsSpecification
          .append(CassandraUtils.cql3MixedCaseQuote(valueMeta.getName()))
          .append(" ")
          .append(CassandraUtils.getCQLTypeForValueMeta(valueMeta))
          .append(",");
    }

    tableColumnsSpecification
        .append("PRIMARY KEY (\"")
        .append(getPrimaryKey().replaceAll(",", "\",\""))
        .append("\" )");

    return String.format(
        "CREATE TABLE %s.%s (%s);", getKeyspace(), getTable(), tableColumnsSpecification);
  }

  String buildInsertCQLStatement() {
    Joiner columnsJoiner = Joiner.on("\",\"").skipNulls();
    Joiner valuesJoiner = Joiner.on(",").skipNulls();
    String[] columnNames = rowMeta.getFieldNames();
    String[] valuePlaceholders = new String[columnNames.length];
    Arrays.fill(valuePlaceholders, "?");
    return String.format(
        "INSERT INTO %s.%s (\"%s\") VALUES (%s);",
        getKeyspace(),
        getTable(),
        columnsJoiner.join(columnNames),
        valuesJoiner.join(valuePlaceholders));
  }
}
