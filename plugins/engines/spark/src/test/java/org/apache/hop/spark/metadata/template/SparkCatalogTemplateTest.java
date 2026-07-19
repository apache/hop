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

package org.apache.hop.spark.metadata.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.spark.metadata.SparkCatalog;
import org.apache.hop.spark.table.SparkLakeFormats;
import org.junit.jupiter.api.Test;

class SparkCatalogTemplateTest {

  @Test
  void icebergHadoopLocalSetsWarehouseAndType() {
    SparkCatalog cat = new SparkCatalog();
    SparkCatalogTemplate.ICEBERG_HADOOP_LOCAL.applyTo(cat);
    assertEquals("lake", cat.getCatalogName());
    assertEquals(SparkCatalog.TYPE_HADOOP, cat.getCatalogType());
    assertEquals(SparkLakeFormats.ICEBERG_CATALOG, cat.getImplementation());
    assertEquals("file:///tmp/hop-warehouse", cat.getWarehouse());
    assertEquals("", cat.getUri());
    assertEquals("", cat.getCredential());
  }

  @Test
  void icebergRestSetsUri() {
    SparkCatalog cat = new SparkCatalog();
    SparkCatalogTemplate.ICEBERG_REST.applyTo(cat);
    assertEquals(SparkCatalog.TYPE_REST, cat.getCatalogType());
    assertEquals("https://catalog.example.com/v1", cat.getUri());
    assertTrue(cat.getWarehouse() == null || cat.getWarehouse().isEmpty());
  }

  @Test
  void icebergRestAuthLeavesCredentialEmptyAndDocumentsToken() {
    SparkCatalog cat = new SparkCatalog();
    cat.setCredential("should-be-cleared");
    SparkCatalogTemplate.ICEBERG_REST_AUTH.applyTo(cat);
    assertEquals(SparkCatalog.TYPE_REST, cat.getCatalogType());
    assertEquals("", cat.getCredential());
    assertTrue(cat.getConfExtra().contains("Credential"));
  }

  @Test
  void objectStoreSetsS3aAndIoImpl() {
    SparkCatalog cat = new SparkCatalog();
    SparkCatalogTemplate.ICEBERG_HADOOP_OBJECT_STORE.applyTo(cat);
    assertEquals("s3a://bucket/warehouse", cat.getWarehouse());
    assertTrue(cat.getConfExtra().contains("io-impl="));
  }

  @Test
  void hiveAndGlueAreAdvancedTypes() {
    SparkCatalog hive = new SparkCatalog();
    SparkCatalogTemplate.HIVE_METASTORE.applyTo(hive);
    assertEquals(SparkCatalog.TYPE_HIVE, hive.getCatalogType());
    assertEquals("hive", hive.getCatalogName());
    assertTrue(hive.getConfExtra().contains("thrift://"));
    assertTrue(hive.getConfExtra().startsWith("# docs:"));

    SparkCatalog glue = new SparkCatalog();
    SparkCatalogTemplate.AWS_GLUE.applyTo(glue);
    assertEquals(SparkCatalog.TYPE_GLUE, glue.getCatalogType());
    assertEquals("glue", glue.getCatalogName());
    assertTrue(glue.getConfExtra().contains("# docs:"));
  }

  @Test
  void nessieUnityAndDeltaAdvancedTemplates() {
    SparkCatalog nessie = new SparkCatalog();
    SparkCatalogTemplate.NESSIE.applyTo(nessie);
    assertEquals(SparkCatalog.TYPE_CUSTOM, nessie.getCatalogType());
    assertEquals("nessie", nessie.getCatalogName());
    assertTrue(nessie.getConfExtra().contains("NessieCatalog"));
    assertTrue(nessie.getConfExtra().contains(SparkCatalogTemplate.Docs.NESSIE_SPARK));

    SparkCatalog unity = new SparkCatalog();
    SparkCatalogTemplate.DATABRICKS_UNITY.applyTo(unity);
    assertEquals("unity", unity.getCatalogName());
    assertTrue(unity.getConfExtra().contains(SparkCatalogTemplate.Docs.DATABRICKS_UNITY));

    SparkCatalog delta = new SparkCatalog();
    SparkCatalogTemplate.DELTA_NAMED_CATALOG.applyTo(delta);
    assertEquals(SparkLakeFormats.DELTA_CATALOG, delta.getImplementation());
    assertTrue(delta.getConfExtra().contains(SparkCatalogTemplate.Docs.DELTA));
  }

  @Test
  void everyTemplateIncludesDocsCommentInConfExtra() {
    for (SparkCatalogTemplate t : SparkCatalogTemplate.values()) {
      SparkCatalog cat = new SparkCatalog();
      t.applyTo(cat);
      assertTrue(
          cat.getConfExtra() != null && cat.getConfExtra().contains("# docs:"),
          () -> t.name() + " should include # docs: link in conf extra");
    }
  }

  @Test
  void confWithDocsFormatsHeaderAndBody() {
    assertEquals(
        "# docs: https://example.com",
        SparkCatalogTemplate.confWithDocs("https://example.com", ""));
    assertEquals(
        "# docs: https://example.com\nuri=thrift://x",
        SparkCatalogTemplate.confWithDocs("https://example.com", "uri=thrift://x"));
  }

  @Test
  void fromDisplayNameRoundTrip() {
    for (SparkCatalogTemplate t : SparkCatalogTemplate.values()) {
      assertEquals(t, SparkCatalogTemplate.fromDisplayName(t.getDisplayName()));
    }
    assertEquals(SparkCatalogTemplate.values().length, SparkCatalogTemplate.displayNames().length);
  }

  @Test
  void looksCustomizedDetectsEdits() {
    SparkCatalog fresh = new SparkCatalog();
    assertFalse(SparkCatalogTemplate.looksCustomized(fresh));

    SparkCatalog withName = new SparkCatalog();
    withName.setCatalogName("lake");
    assertTrue(SparkCatalogTemplate.looksCustomized(withName));

    SparkCatalog withWarehouse = new SparkCatalog();
    withWarehouse.setWarehouse("file:///tmp/wh");
    assertTrue(SparkCatalogTemplate.looksCustomized(withWarehouse));
  }

  @Test
  void everyTemplateAppliesWithoutNpe() {
    for (SparkCatalogTemplate t : SparkCatalogTemplate.values()) {
      SparkCatalog cat = new SparkCatalog();
      t.applyTo(cat);
      assertNotNull(cat.getCatalogType());
      assertNotNull(cat.getImplementation());
      assertNotNull(t.getDescription());
      assertEquals("", cat.getCredential());
    }
  }
}
