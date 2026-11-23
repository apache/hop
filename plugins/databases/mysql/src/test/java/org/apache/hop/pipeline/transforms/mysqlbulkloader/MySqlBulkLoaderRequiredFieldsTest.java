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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The fields of the target table, as "Edit mapping" reads them through getRequiredFields(), with
 * and without a schema. The connection is H2: the method only uses the generic Database calls.
 */
class MySqlBulkLoaderRequiredFieldsTest {

  private static final String CONNECTION = "bulk-target";

  private DatabaseMeta databaseMeta;
  private Variables variables;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    String name = "mem:bulk" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", name, "", "sa", "");
    variables = new Variables();
    try (Database db = new Database(new LoggingObject("setup"), variables, databaseMeta)) {
      db.connect();
      db.execStatements(
          "CREATE SCHEMA SALES;"
              + "CREATE TABLE SALES.ORDERS (ID INTEGER, AMOUNT DECIMAL(10,2), NOTE VARCHAR(20));"
              + "CREATE TABLE CUSTOMERS (ID INTEGER, NAME VARCHAR(20));");
    }
  }

  private MySqlBulkLoaderMeta meta(String schema, String table) throws Exception {
    MySqlBulkLoaderMeta meta = new MySqlBulkLoaderMeta();
    meta.setConnection(CONNECTION);
    meta.setSchemaName(schema);
    meta.setTableName(table);

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    pipelineMeta.addTransform(new TransformMeta("bulk", meta));
    return meta;
  }

  @Test
  void theFieldsOfATableInASchemaAreFound() throws Exception {
    IRowMeta fields = meta("SALES", "ORDERS").getRequiredFields(variables);

    assertArrayEquals(new String[] {"ID", "AMOUNT", "NOTE"}, fields.getFieldNames());
  }

  @Test
  void theSchemaCanComeFromAVariable() throws Exception {
    variables.setVariable("TARGET_SCHEMA", "SALES");

    IRowMeta fields = meta("${TARGET_SCHEMA}", "ORDERS").getRequiredFields(variables);

    assertArrayEquals(new String[] {"ID", "AMOUNT", "NOTE"}, fields.getFieldNames());
  }

  @Test
  void theFieldsOfATableWithoutASchemaAreFound() throws Exception {
    IRowMeta fields = meta(null, "CUSTOMERS").getRequiredFields(variables);

    assertArrayEquals(new String[] {"ID", "NAME"}, fields.getFieldNames());
  }

  @Test
  void aTableThatDoesNotExistIsReported() throws Exception {
    MySqlBulkLoaderMeta meta = meta("SALES", "MISSING");

    assertThrows(HopException.class, () -> meta.getRequiredFields(variables));
  }
}
