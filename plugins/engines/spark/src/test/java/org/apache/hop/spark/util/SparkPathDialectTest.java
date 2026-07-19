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

package org.apache.hop.spark.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.Test;

class SparkPathDialectTest {

  @Test
  void extractSchemeFromCommonUris() {
    assertEquals("s3", SparkPathDialect.extractScheme("s3://bucket/key"));
    assertEquals("s3a", SparkPathDialect.extractScheme("s3a://bucket/key"));
    assertEquals("hdfs", SparkPathDialect.extractScheme("hdfs://nn:8020/path"));
    assertEquals("file", SparkPathDialect.extractScheme("file:///tmp/x"));
    assertEquals("file", SparkPathDialect.extractScheme("file:/tmp/x"));
    assertEquals("azure", SparkPathDialect.extractScheme("AZURE://account/container"));
    assertNull(SparkPathDialect.extractScheme("/local/path"));
    assertNull(SparkPathDialect.extractScheme("relative/path"));
    assertNull(SparkPathDialect.extractScheme(null));
    assertNull(SparkPathDialect.extractScheme(""));
  }

  @Test
  void hopVfsSchemesDetected() {
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("s3://bucket/a"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("azure://x"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("azfs://x"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("googledrive://x"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("dropbox://x"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("webdav4://host/path"));
    assertTrue(SparkPathDialect.isKnownHopVfsScheme("webdav4s://host/path"));
  }

  @Test
  void sparkSchemesNotFlaggedAsHopVfs() {
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("s3a://bucket/a"));
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("hdfs://nn/path"));
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("file:///tmp/x"));
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("abfs://container@account/path"));
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("gs://bucket/obj"));
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("${PROJECT_HOME}/out"));
    // Named MinIO-style schemes cannot be listed exhaustively
    assertFalse(SparkPathDialect.isKnownHopVfsScheme("minio:///demo/file"));
  }

  @Test
  void s3HintMentionsS3a() {
    String hint = SparkPathDialect.hopVfsSchemeHint("s3://bucket/key");
    assertNotNull(hint);
    assertTrue(hint.contains("s3a://"));
    assertTrue(hint.toLowerCase().contains("hop vfs"));
  }

  @Test
  void withPathHintAppendsOnlyForHopSchemes() {
    String base = "Error reading 's3://b/k' as csv";
    String with = SparkPathDialect.withPathHint(base, "s3://b/k");
    assertTrue(with.startsWith(base));
    assertTrue(with.contains("Hint:"));
    assertTrue(with.contains("s3a://"));

    assertEquals(
        "Error reading 's3a://b/k' as csv",
        SparkPathDialect.withPathHint("Error reading 's3a://b/k' as csv", "s3a://b/k"));
  }

  @Test
  void parseSchemeMapIgnoresCommentsAndNormalizesTokens() {
    Map<String, String> map =
        SparkPathDialect.parseSchemeMap(
            """
            # hop to spark
            s3=s3a
            s3://=s3a://
            minio = s3a
            azure=abfs
            broken
            =s3a
            s3a=
            """);
    assertEquals("s3a", map.get("s3")); // last s3:// line wins over s3=
    assertEquals("s3a", map.get("minio"));
    assertEquals("abfs", map.get("azure"));
    assertFalse(map.containsKey("s3a"));
  }

  @Test
  void toSparkUriRewritesMappedSchemesOnly() {
    String map = "s3=s3a\nminio=s3a\n";
    assertEquals("s3a://bucket/key", SparkPathDialect.toSparkUri("s3://bucket/key", map));
    assertEquals("s3a://bucket/key", SparkPathDialect.toSparkUri("S3://bucket/key", map));
    assertEquals("s3a:///demo/x", SparkPathDialect.toSparkUri("minio:///demo/x", map));
    assertEquals("s3a://bucket/key", SparkPathDialect.toSparkUri("s3a://bucket/key", map));
    assertEquals("/local/path", SparkPathDialect.toSparkUri("/local/path", map));
    assertEquals("hdfs://nn/path", SparkPathDialect.toSparkUri("hdfs://nn/path", map));
    assertNull(SparkPathDialect.toSparkUri(null, map));
    assertEquals("", SparkPathDialect.toSparkUri("", map));
  }

  @Test
  void toSparkUriWithRunConfiguration() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setPathSchemeMap("s3=s3a");
    assertEquals("s3a://b/k", SparkPathDialect.toSparkUri("s3://b/k", config));
    assertEquals("s3://b/k", SparkPathDialect.toSparkUri("s3://b/k", (String) null));
  }
}
