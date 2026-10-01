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
 *
 */

package org.apache.hop.arrow.datastream.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.hop.arrow.flight.ArrowFlightSecurity;
import org.apache.hop.arrow.flight.ArrowFlightServer;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Round trips rows through an authenticated Flight server using the data stream itself, so the
 * credentials on the Hop client side are exercised on all three calls it makes: getFlightInfo,
 * doPut and doGet.
 */
class ArrowFlightDataStreamAuthenticationTest {

  private static final String STREAM_NAME = "secured-stream";
  private static final String SCHEMA_NAME = "secured-schema";
  private static final String USERNAME = "hop";
  private static final String PASSWORD = "s3cr3t";

  private final IVariables variables = new Variables();
  private final ILogChannel log = mock(ILogChannel.class);

  private MemoryMetadataProvider metadataProvider;
  private ArrowFlightServer server;
  private ArrowFlightDataStream writer;
  private ArrowFlightDataStream reader;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (writer != null) {
      writer.close();
    }
    if (reader != null) {
      reader.close();
    }
    if (server != null) {
      server.shutdown();
      server.getFlightServer().awaitTermination();
    }
  }

  /**
   * Starts a secured server and registers the metadata the producer and the client both read. The
   * port is only known after the server is up, so it's set on the data stream afterwards: both
   * sides read it lazily.
   */
  private ArrowFlightDataStream startServerAndRegisterMetadata(String password) throws Exception {
    metadataProvider = new MemoryMetadataProvider();

    SchemaDefinition schemaDefinition = new SchemaDefinition();
    schemaDefinition.setName(SCHEMA_NAME);
    schemaDefinition.setFieldDefinitions(
        List.of(
            new SchemaFieldDefinition("id", "Integer"),
            new SchemaFieldDefinition("name", "String")));
    metadataProvider.getSerializer(SchemaDefinition.class).save(schemaDefinition);

    ArrowFlightDataStream dataStream = new ArrowFlightDataStream();
    dataStream.setSchemaDefinitionName(SCHEMA_NAME);
    dataStream.setHostname("localhost");
    dataStream.setUsername(USERNAME);
    dataStream.setPassword(password);

    DataStreamMeta dataStreamMeta = new DataStreamMeta();
    dataStreamMeta.setName(STREAM_NAME);
    dataStreamMeta.setDataStream(dataStream);
    metadataProvider.getSerializer(DataStreamMeta.class).save(dataStreamMeta);

    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setUsername(USERNAME);
    security.setPassword(PASSWORD);

    server = new ArrowFlightServer("localhost", 0, security, variables, metadataProvider, log);
    server.start();
    dataStream.setPort(Integer.toString(server.getFlightServer().getPort()));

    return dataStream;
  }

  private DataStreamMeta loadStreamMeta() throws HopException {
    return metadataProvider.getSerializer(DataStreamMeta.class).load(STREAM_NAME);
  }

  @Test
  void rowsRoundTripThroughAnAuthenticatedServer() throws Exception {
    startServerAndRegisterMetadata(PASSWORD);
    DataStreamMeta dataStreamMeta = loadStreamMeta();
    IRowMeta rowMeta =
        metadataProvider.getSerializer(SchemaDefinition.class).load(SCHEMA_NAME).getRowMeta();

    // Write a handful of rows. This authenticates and then does a doPut.
    //
    writer = (ArrowFlightDataStream) dataStreamMeta.getDataStream();
    writer.initialize(variables, metadataProvider, true, dataStreamMeta);
    writer.setRowMeta(rowMeta);
    for (int i = 0; i < 5; i++) {
      writer.writeRow(new Object[] {(long) i, "row-" + i});
    }
    writer.setOutputDone();

    // Read them back. This authenticates again and then does a getFlightInfo and a doGet.
    //
    DataStreamMeta readMeta = loadStreamMeta();
    reader = (ArrowFlightDataStream) readMeta.getDataStream();
    reader.setPort(Integer.toString(server.getFlightServer().getPort()));
    reader.initialize(variables, metadataProvider, false, readMeta);

    IRowMeta readRowMeta = reader.getRowMeta();
    assertEquals(2, readRowMeta.size());

    for (int i = 0; i < 5; i++) {
      Object[] row = reader.readRow();
      assertEquals((long) i, row[0]);
      assertEquals("row-" + i, row[1]);
    }
    assertNull(reader.readRow());
  }

  @Test
  void theWrongPasswordIsRefusedByTheServer() throws Exception {
    startServerAndRegisterMetadata("not-the-password");
    DataStreamMeta dataStreamMeta = loadStreamMeta();
    IRowMeta rowMeta =
        metadataProvider.getSerializer(SchemaDefinition.class).load(SCHEMA_NAME).getRowMeta();

    writer = (ArrowFlightDataStream) dataStreamMeta.getDataStream();
    writer.initialize(variables, metadataProvider, true, dataStreamMeta);

    assertThrows(HopException.class, () -> writer.setRowMeta(rowMeta));
  }
}
