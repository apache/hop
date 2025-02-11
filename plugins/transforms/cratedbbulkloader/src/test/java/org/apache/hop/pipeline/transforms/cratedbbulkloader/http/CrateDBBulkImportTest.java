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
 */

package org.apache.hop.pipeline.transforms.cratedbbulkloader.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.CrateDBBulkLoader;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.CrateDBBulkLoaderData;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.CrateDBBulkLoaderField;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.CrateDBBulkLoaderMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.testcontainers.cratedb.CrateDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class CrateDBBulkImportTest {

  @Container public static CrateDBContainer crateDBContainer = new CrateDBContainer("crate");

  private static Connection connection;

  @BeforeAll
  static void init() throws Exception {
    String skipTestContainers = System.getProperty("SkipTestContainers");
    if (skipTestContainers == null) {
      HopClientEnvironment.init(
          List.of(
              DatabasePluginType.getInstance(),
              TwoWayPasswordEncoderPluginType.getInstance(),
              ValueMetaPluginType.getInstance()));
      HopEnvironment.init();
      HopLogStore.init(true, true);
      connection = crateDBContainer.createConnection("");
      String createTableQuery =
          """
                            CREATE TABLE doc.person
                            (
                              id INTEGER PRIMARY KEY,
                              name TEXT,
                              score DOUBLE PRECISION)
                            ;
                            """;
      executeUpdate("DROP TABLE IF EXISTS doc.person");
      executeUpdate("DROP USER IF EXISTS bob");
      executeUpdate(createTableQuery);
      executeUpdate("REFRESH TABLE doc.person");
      executeUpdate("CREATE USER bob WITH PASSWORD 'password'");
      executeUpdate("GRANT DML ON SCHEMA doc TO bob");
    }
  }

  @AfterAll
  static void shutdown() throws Exception {
    String skipTestContainers = System.getProperty("SkipTestContainers");
    if (skipTestContainers == null) {
      executeUpdate("DROP TABLE doc.person;");
    }
  }

  private static int executeUpdate(String query) throws Exception {
    try (Statement statement = connection.createStatement()) {
      return statement.executeUpdate(query);
    }
  }

  private static ResultSet executeQuery(String query) throws Exception {
    Statement statement = connection.createStatement();
    return statement.executeQuery(query);
  }

  private static Stream<Arguments> getBatchSize() {
    return Stream.of(
        Arguments.of(1),
        Arguments.of(2),
        Arguments.of(3),
        Arguments.of(4),
        Arguments.of(5),
        Arguments.of(6),
        Arguments.of(7),
        Arguments.of(8),
        Arguments.of(9),
        Arguments.of(10),
        Arguments.of(20),
        Arguments.of(30),
        Arguments.of(40),
        Arguments.of(50));
  }

  @BeforeEach
  void setup() throws Exception {
    String skipTestContainers = System.getProperty("SkipTestContainers");
    if (skipTestContainers == null) {
      executeUpdate("DELETE FROM doc.person");
    }
  }

  @ParameterizedTest
  @MethodSource("getBatchSize")
  @DisabledIfSystemProperty(named = "SkipTestContainers", matches = "true")
  void given_batch_size__when_http_insert__should_persist_all_items(Integer batchSize)
      throws Exception {
    Mockito.mock(Pipeline.class);
    CrateDBBulkLoaderMeta meta = new CrateDBBulkLoaderMeta();
    meta.setBatchSize(String.valueOf(batchSize));
    meta.setConnection("test");
    meta.setHttpLogin("bob");
    meta.setHttpPassword("password");
    meta.setHttpEndpoint(
        String.format(
            "http://%s:%d/_sql", crateDBContainer.getHost(), crateDBContainer.getMappedPort(4200)));
    meta.setTablename("person");
    meta.setSchemaName("doc");
    meta.setStreamToS3Csv(false);
    meta.setUseHttpEndpoint(true);
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new CrateDBBulkLoaderField("id", "id"),
            new CrateDBBulkLoaderField("name", "name"),
            new CrateDBBulkLoaderField("score", "score")));

    // meta.set

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("test");
    DatabaseMeta meta2 =
        new DatabaseMeta(
            "test",
            "CrateDB",
            "0",
            "localhost",
            "crate",
            String.valueOf(crateDBContainer.getMappedPort(5432)),
            "crate",
            "password");
    CrateDBBulkLoaderData data = new CrateDBBulkLoaderData();
    data.setDatabaseMeta(meta2);

    PipelineMeta pipelineMeta = Mockito.mock(PipelineMeta.class);
    when(pipelineMeta.getName()).thenReturn("pipeline");
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    when(pipelineMeta.findDatabase(anyString(), any())).thenReturn(meta2);

    Pipeline pipeline = new LocalPipelineEngine();
    pipeline.setRunning(true);
    CrateDBBulkLoader transform =
        new CrateDBBulkLoader(transformMeta, meta, data, 1, pipelineMeta, pipeline);

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaNumber("score", 5, 2));

    BlockingRowSet inputRowSet = new BlockingRowSet(10);
    inputRowSet.putRow(inputRowMeta, new Object[] {1, "DeLo", 9.25});
    inputRowSet.putRow(inputRowMeta, new Object[] {2, "Sergio", 8.0});
    inputRowSet.putRow(inputRowMeta, new Object[] {3, "Fake", 7.5});
    inputRowSet.putRow(inputRowMeta, new Object[] {4, "Fake123", 4.5});
    inputRowSet.putRow(inputRowMeta, new Object[] {5, "Hugo", 1.5});
    inputRowSet.setDone();

    transform.addRowSetToInputRowSets(inputRowSet);

    transform.init();
    transform.setRunning(true);
    transform.setStopped(false);

    // We must process all rows plus one, so the last one can be detected as the end of the stream
    while (transform.processRow()) {
      // Do nothing
    }

    executeUpdate("REFRESH TABLE doc.person");
    final ResultSet resultSet = executeQuery("SELECT count(*) as c FROM doc.person");
    resultSet.next();
    final int resultSize = resultSet.getInt("c");

    assertEquals(5, resultSize);
  }
}
