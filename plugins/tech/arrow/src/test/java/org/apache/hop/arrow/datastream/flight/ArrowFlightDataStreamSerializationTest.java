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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.datastream.plugin.DataStreamPluginType;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The TLS and credential settings are only useful if they survive a save and a load, and the
 * password must not sit in the metadata in plain text.
 */
class ArrowFlightDataStreamSerializationTest {

  private static final String STREAM_NAME = "secured-stream";

  @TempDir private Path metadataFolder;

  private JsonMetadataProvider metadataProvider;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    // The data stream plugin isn't picked up by annotation scanning in a plain unit test.
    PluginRegistry.getInstance()
        .registerPluginClass(
            ArrowFlightDataStream.class.getName(),
            DataStreamPluginType.class,
            DataStreamPlugin.class);
  }

  @BeforeEach
  void setUp() {
    metadataProvider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            metadataFolder.toString(),
            Variables.getADefaultVariableSpace());
  }

  private DataStreamMeta saveStream(ArrowFlightDataStream dataStream) throws Exception {
    DataStreamMeta meta = new DataStreamMeta();
    meta.setName(STREAM_NAME);
    meta.setDataStream(dataStream);
    IHopMetadataSerializer<DataStreamMeta> serializer =
        metadataProvider.getSerializer(DataStreamMeta.class);
    serializer.save(meta);
    return serializer.load(STREAM_NAME);
  }

  private String savedJson() throws Exception {
    try (Stream<Path> files = Files.walk(metadataFolder)) {
      Path file =
          files
              .filter(p -> p.toString().endsWith(".json"))
              .max(Comparator.comparing(Path::toString))
              .orElseThrow();
      return Files.readString(file, StandardCharsets.UTF_8);
    }
  }

  @Test
  void everySecuritySettingSurvivesASaveAndLoad() throws Exception {
    ArrowFlightDataStream dataStream = new ArrowFlightDataStream();
    dataStream.setHostname("flight.example.com");
    dataStream.setPort("44444");
    dataStream.setTls(true);
    dataStream.setVerifyServer(false);
    dataStream.setTrustedCertificatesFile("/etc/hop/ca.crt");
    dataStream.setClientCertificateFile("/etc/hop/client.crt");
    dataStream.setClientKeyFile("/etc/hop/client.key");
    dataStream.setUsername("hop");
    dataStream.setPassword("s3cr3t");

    ArrowFlightDataStream loaded = (ArrowFlightDataStream) saveStream(dataStream).getDataStream();

    assertEquals("flight.example.com", loaded.getHostname());
    assertEquals("44444", loaded.getPort());
    assertTrue(loaded.isTls());
    assertFalse(loaded.isVerifyServer());
    assertEquals("/etc/hop/ca.crt", loaded.getTrustedCertificatesFile());
    assertEquals("/etc/hop/client.crt", loaded.getClientCertificateFile());
    assertEquals("/etc/hop/client.key", loaded.getClientKeyFile());
    assertEquals("hop", loaded.getUsername());
    assertEquals("s3cr3t", loaded.getPassword());
  }

  @Test
  void thePasswordIsNotStoredInPlainText() throws Exception {
    ArrowFlightDataStream dataStream = new ArrowFlightDataStream();
    dataStream.setUsername("hop");
    dataStream.setPassword("s3cr3t");
    saveStream(dataStream);

    String json = savedJson();
    assertFalse(json.contains("s3cr3t"), "The password was written to the metadata in plain text");
    assertTrue(json.contains(Encr.encryptPasswordIfNotUsingVariables("s3cr3t")));
  }

  @Test
  void aVariableIsStoredAsIsSoItStaysResolvable() throws Exception {
    ArrowFlightDataStream dataStream = new ArrowFlightDataStream();
    dataStream.setUsername("hop");
    dataStream.setPassword("${FLIGHT_PASSWORD}");
    saveStream(dataStream);

    assertTrue(savedJson().contains("${FLIGHT_PASSWORD}"));
  }

  /**
   * A stream saved before this change has none of the new keys. It has to keep working, which means
   * plain gRPC, no credentials, and server verification left on rather than silently off.
   */
  @Test
  void aStreamSavedBeforeTheSecurityOptionsExistedStillLoads() throws Exception {
    Path folder = metadataFolder.resolve("data-stream");
    Files.createDirectories(folder);
    Files.writeString(
        folder.resolve(STREAM_NAME + ".json"),
        """
        {
          "dataStream": {
            "ArrowFlightStream": {
              "batchSize": "10000",
              "bufferSize": "10000000",
              "hostname": "localhost",
              "port": "33333",
              "schemaDefinition": "some-schema"
            }
          }
        }
        """);

    DataStreamMeta meta = metadataProvider.getSerializer(DataStreamMeta.class).load(STREAM_NAME);
    ArrowFlightDataStream loaded = (ArrowFlightDataStream) meta.getDataStream();

    assertFalse(loaded.isTls(), "An existing stream must not suddenly expect TLS");
    assertTrue(loaded.isVerifyServer(), "Server verification must default to on, not off");
    assertEquals("localhost", loaded.getHostname());
    assertEquals("33333", loaded.getPort());
  }
}
