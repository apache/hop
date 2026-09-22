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

package org.apache.hop.testing.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.generic.GenericDatabaseMeta;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookupMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestDatabaseReplacement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PipelineMetaModifierTest {

  private static final String SQL_SERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  private static final String H2_DRIVER = "org.h2.Driver";

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void replacesDatabaseLookupConnectionOnTheTestCopy() throws HopException {
    IVariables variables = new Variables();
    variables.setVariable("DB", "source-db");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    DatabaseMeta source = genericConnection("source-db", "source.example", "source_db");
    DatabaseMeta test = h2Connection("test-h2", "mem:lookup_data");
    save(provider, source, test, genericConnection("other-db", "other.example", "other_db"));

    PipelineMeta pipeline =
        pipeline(
            provider,
            lookup("lookup", "source-db"),
            lookup("other", "other-db"),
            lookup("variable-lookup", "${DB}"));
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest
        .getDatabaseReplacements()
        .add(new PipelineUnitTestDatabaseReplacement("source-db", "test-h2"));

    PipelineMeta copy =
        new PipelineMetaModifier(variables, pipeline, unitTest)
            .getTestPipeline(LogChannel.GENERAL, variables, provider);

    assertEquals("test-h2", connection(copy, "lookup"));
    DatabaseMeta resolved = copy.findDatabase(connection(copy, "lookup"), variables);
    assertEquals("H2", resolved.getPluginId());
    assertEquals("mem:lookup_data", resolved.getDatabaseName());
    assertEquals(H2_DRIVER, resolved.getDriverClass(variables));

    assertEquals("other-db", connection(copy, "other"));
    assertEquals("test-h2", connection(copy, "variable-lookup"));
    assertEquals("source-db", connection(pipeline, "lookup"));

    DatabaseMeta sourceAfter = provider.getSerializer(DatabaseMeta.class).load("source-db");
    assertEquals("source-db", sourceAfter.getName());
    assertEquals("GENERIC", sourceAfter.getPluginId());
    assertEquals(SQL_SERVER_DRIVER, sourceAfter.getDriverClass(variables));
    assertEquals("source.example", sourceAfter.getHostname());
  }

  @Test
  void replacementsDoNotChain() throws HopException {
    IVariables variables = new Variables();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    save(
        provider,
        genericConnection("source-db", "source.example", "source_db"),
        h2Connection("test-h2", "mem:lookup_data"),
        h2Connection("other-h2", "mem:other"));

    PipelineMeta pipeline = pipeline(provider, lookup("lookup", "source-db"));
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest
        .getDatabaseReplacements()
        .add(new PipelineUnitTestDatabaseReplacement("source-db", "test-h2"));
    unitTest
        .getDatabaseReplacements()
        .add(new PipelineUnitTestDatabaseReplacement("test-h2", "other-h2"));

    PipelineMeta copy =
        new PipelineMetaModifier(variables, pipeline, unitTest)
            .getTestPipeline(LogChannel.GENERAL, variables, provider);

    assertEquals("test-h2", connection(copy, "lookup"));
  }

  @Test
  void missingReplacementDoesNotChangeTheSourceConnection() throws HopException {
    IVariables variables = new Variables();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    DatabaseMeta source = genericConnection("source-db", "source.example", "source_db");
    save(provider, source);
    PipelineMeta pipeline = pipeline(provider, lookup("lookup", "source-db"));
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest
        .getDatabaseReplacements()
        .add(new PipelineUnitTestDatabaseReplacement("source-db", "test-h2"));

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                new PipelineMetaModifier(variables, pipeline, unitTest)
                    .getTestPipeline(LogChannel.GENERAL, variables, provider));

    assertTrue(exception.getMessage().contains("test-h2"));
    assertEquals("source-db", source.getName());
    assertEquals("GENERIC", source.getPluginId());
    assertEquals(SQL_SERVER_DRIVER, source.getDriverClass(variables));
    assertEquals("source-db", connection(pipeline, "lookup"));
  }

  private static PipelineMeta pipeline(MemoryMetadataProvider provider, TransformMeta... transforms)
      throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("lookup-test");
    pipelineMeta.setMetadataProvider(provider);
    for (TransformMeta transform : transforms) {
      pipelineMeta.addTransform(transform);
    }
    return pipelineMeta;
  }

  private static TransformMeta lookup(String name, String connection) {
    DatabaseLookupMeta lookupMeta = new DatabaseLookupMeta();
    lookupMeta.setConnection(connection);
    return new TransformMeta("DBLookup", name, lookupMeta);
  }

  private static String connection(PipelineMeta pipelineMeta, String transformName) {
    return ((DatabaseLookupMeta) pipelineMeta.findTransform(transformName).getTransform())
        .getConnection();
  }

  private static void save(MemoryMetadataProvider provider, DatabaseMeta... connections)
      throws HopException {
    IHopMetadataSerializer<DatabaseMeta> serializer = provider.getSerializer(DatabaseMeta.class);
    for (DatabaseMeta connection : connections) {
      serializer.save(connection);
    }
  }

  private static DatabaseMeta genericConnection(String name, String host, String database) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(name, "GENERIC", "Native", host, database, "1433", "sa", "secret");
    databaseMeta
        .getAttributes()
        .put(GenericDatabaseMeta.ATTRIBUTE_CUSTOM_DRIVER_CLASS, SQL_SERVER_DRIVER);
    return databaseMeta;
  }

  private static DatabaseMeta h2Connection(String name, String database) {
    return new DatabaseMeta(name, "H2", "Native", "localhost", database, "", "sa", "");
  }
}
