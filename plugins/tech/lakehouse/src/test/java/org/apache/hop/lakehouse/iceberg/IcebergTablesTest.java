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

package org.apache.hop.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergTablesTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void identifierWithoutCatalogName() throws Exception {
    assertEquals(
        TableIdentifier.of(Namespace.of("db"), "orders"),
        IcebergTables.toIdentifier("db.orders", "lake"));
  }

  @Test
  void identifierWithCatalogNameUsedOnSpark() throws Exception {
    assertEquals(
        TableIdentifier.of(Namespace.of("db"), "orders"),
        IcebergTables.toIdentifier("lake.db.orders", "lake"));
  }

  @Test
  void identifierWithNestedNamespace() throws Exception {
    assertEquals(
        TableIdentifier.of(Namespace.of("a", "b"), "orders"),
        IcebergTables.toIdentifier("a.b.orders", "lake"));
  }

  @Test
  void identifierNeedsNamespace() {
    assertThrows(HopException.class, () -> IcebergTables.toIdentifier("orders", "lake"));
    assertThrows(HopException.class, () -> IcebergTables.toIdentifier(" ", "lake"));
  }

  @Test
  void hadoopTableLocation() {
    assertEquals(
        "s3://bucket/wh/a/b/orders",
        IcebergTables.hadoopTableLocation(
            "s3://bucket/wh/", TableIdentifier.of(Namespace.of("a", "b"), "orders")));
  }

  @Test
  void extraPropertiesAcceptPlainAndSparkStyleKeys() {
    Map<String, String> properties =
        IcebergTables.extraProperties(
            """
            # comment
            client.region = eu-west-1
            spark.sql.catalog.lake.s3.endpoint=http://minio:9000

            not a property
            """);
    assertEquals(
        Map.of("client.region", "eu-west-1", "s3.endpoint", "http://minio:9000"), properties);
  }

  @Test
  void restPropertiesUseHopVfsUnlessOverridden() throws Exception {
    LakeCatalog catalog = new LakeCatalog();
    catalog.setName("lake");
    catalog.setCatalogType(LakeCatalog.TYPE_REST);
    catalog.setUri("${CATALOG_URI}");
    catalog.setCredential("secret");
    Variables variables = new Variables();
    variables.setVariable("CATALOG_URI", "http://polaris:8181/api/catalog");

    Map<String, String> properties = IcebergTables.restProperties(catalog, variables);
    assertEquals("http://polaris:8181/api/catalog", properties.get("uri"));
    assertEquals(HopVfsFileIO.class.getName(), properties.get("io-impl"));
    assertEquals("secret", properties.get("token"));

    catalog.setConfExtra("io-impl=org.apache.iceberg.aws.s3.S3FileIO");
    assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO",
        IcebergTables.restProperties(catalog, variables).get("io-impl"));
  }

  @Test
  void restCatalogNeedsUri() {
    LakeCatalog catalog = new LakeCatalog();
    catalog.setName("lake");
    catalog.setCatalogType(LakeCatalog.TYPE_REST);
    assertThrows(HopException.class, () -> IcebergTables.restProperties(catalog, new Variables()));
  }

  @Test
  void unsupportedCatalogTypeIsReported() {
    LakeCatalog catalog = new LakeCatalog();
    catalog.setName("lake");
    catalog.setCatalogType(LakeCatalog.TYPE_GLUE);
    HopException e =
        assertThrows(
            HopException.class,
            () -> IcebergTables.loadFromCatalog(catalog, "db.orders", new Variables()));
    assertEquals(true, e.getMessage().contains("only supported on the Spark engine"));
  }

  @Test
  void metadataVersions() {
    assertEquals(3, IcebergTables.metadataVersion("v3.metadata.json"));
    assertEquals(3, IcebergTables.metadataVersion("v3.gz.metadata.json"));
    assertEquals(
        12,
        IcebergTables.metadataVersion("00012-6b0c1f7e-2a71-4f3c-9d6c-0b1d3e6c7f21.metadata.json"));
    assertEquals(-1, IcebergTables.metadataVersion("version-hint.text"));
    assertEquals(-1, IcebergTables.metadataVersion("snap-123-1-abc.avro"));
  }

  @Test
  void versionHintWins() throws Exception {
    Path metadata = Files.createDirectories(tempDir.resolve("t/metadata"));
    Files.writeString(metadata.resolve("v1.metadata.json"), "{}");
    Files.writeString(metadata.resolve("v2.metadata.json"), "{}");
    Files.writeString(metadata.resolve("version-hint.text"), "1\n");

    String root = tempDir.resolve("t").toUri().toString();
    assertEquals(
        metadataFolder(root) + "/v1.metadata.json", IcebergTables.currentMetadataFile(root));
  }

  @Test
  void newestMetadataFileWithoutHint() throws Exception {
    Path metadata = Files.createDirectories(tempDir.resolve("t/metadata"));
    Files.writeString(metadata.resolve("00001-aaaa-bbbb.metadata.json"), "{}");
    Files.writeString(metadata.resolve("00003-cccc-dddd.metadata.json"), "{}");
    Files.writeString(metadata.resolve("00002-eeee-ffff.metadata.json"), "{}");
    Files.writeString(metadata.resolve("snap-1.avro"), "");

    String root = tempDir.resolve("t").toUri().toString();
    assertEquals(
        metadataFolder(root) + "/00003-cccc-dddd.metadata.json",
        IcebergTables.currentMetadataFile(root));
  }

  @Test
  void noTableAtPath() throws Exception {
    Files.createDirectories(tempDir.resolve("empty/metadata"));
    assertThrows(
        HopException.class,
        () -> IcebergTables.currentMetadataFile(tempDir.resolve("empty").toUri().toString()));
    assertThrows(
        HopException.class,
        () -> IcebergTables.currentMetadataFile(tempDir.resolve("missing").toUri().toString()));
  }

  private static String metadataFolder(String root) {
    return (root.endsWith("/") ? root.substring(0, root.length() - 1) : root) + "/metadata";
  }
}
