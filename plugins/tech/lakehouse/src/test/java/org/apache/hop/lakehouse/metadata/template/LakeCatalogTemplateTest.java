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

package org.apache.hop.lakehouse.metadata.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.junit.jupiter.api.Test;

class LakeCatalogTemplateTest {

  @Test
  void icebergHadoopLocalSetsWarehouseAndType() {
    LakeCatalog cat = new LakeCatalog();
    LakeCatalogTemplate.ICEBERG_HADOOP_LOCAL.applyTo(cat);
    assertEquals("lake", cat.getCatalogName());
    assertEquals(LakeCatalog.TYPE_HADOOP, cat.getCatalogType());
    assertEquals(LakeFormats.ICEBERG_CATALOG, cat.getImplementation());
    assertEquals("file:///tmp/hop-warehouse", cat.getWarehouse());
    assertEquals("", cat.getUri());
    assertEquals("", cat.getCredential());
  }

  @Test
  void icebergRestSetsUri() {
    LakeCatalog cat = new LakeCatalog();
    LakeCatalogTemplate.ICEBERG_REST.applyTo(cat);
    assertEquals(LakeCatalog.TYPE_REST, cat.getCatalogType());
    assertEquals("https://catalog.example.com/v1", cat.getUri());
    assertTrue(cat.getWarehouse() == null || cat.getWarehouse().isEmpty());
  }

  @Test
  void icebergRestAuthLeavesCredentialEmptyAndDocumentsToken() {
    LakeCatalog cat = new LakeCatalog();
    cat.setCredential("should-be-cleared");
    LakeCatalogTemplate.ICEBERG_REST_AUTH.applyTo(cat);
    assertEquals(LakeCatalog.TYPE_REST, cat.getCatalogType());
    assertEquals("", cat.getCredential());
    assertTrue(cat.getConfExtra().contains("Credential"));
  }

  @Test
  void objectStoreSetsS3aAndIoImpl() {
    LakeCatalog cat = new LakeCatalog();
    LakeCatalogTemplate.ICEBERG_HADOOP_OBJECT_STORE.applyTo(cat);
    assertEquals("s3a://bucket/warehouse", cat.getWarehouse());
    assertTrue(cat.getConfExtra().contains("io-impl="));
  }

  @Test
  void hiveAndGlueAreAdvancedTypes() {
    LakeCatalog hive = new LakeCatalog();
    LakeCatalogTemplate.HIVE_METASTORE.applyTo(hive);
    assertEquals(LakeCatalog.TYPE_HIVE, hive.getCatalogType());
    assertEquals("hive", hive.getCatalogName());
    assertTrue(hive.getConfExtra().contains("thrift://"));
    assertTrue(hive.getConfExtra().startsWith("# docs:"));

    LakeCatalog glue = new LakeCatalog();
    LakeCatalogTemplate.AWS_GLUE.applyTo(glue);
    assertEquals(LakeCatalog.TYPE_GLUE, glue.getCatalogType());
    assertEquals("glue", glue.getCatalogName());
    assertTrue(glue.getConfExtra().contains("# docs:"));
  }

  @Test
  void nessieUnityAndDeltaAdvancedTemplates() {
    LakeCatalog nessie = new LakeCatalog();
    LakeCatalogTemplate.NESSIE.applyTo(nessie);
    assertEquals(LakeCatalog.TYPE_CUSTOM, nessie.getCatalogType());
    assertEquals("nessie", nessie.getCatalogName());
    assertTrue(nessie.getConfExtra().contains("NessieCatalog"));
    assertTrue(nessie.getConfExtra().contains(LakeCatalogTemplate.Docs.NESSIE_SPARK));

    LakeCatalog unity = new LakeCatalog();
    LakeCatalogTemplate.DATABRICKS_UNITY.applyTo(unity);
    assertEquals("unity", unity.getCatalogName());
    assertTrue(unity.getConfExtra().contains(LakeCatalogTemplate.Docs.DATABRICKS_UNITY));

    LakeCatalog delta = new LakeCatalog();
    LakeCatalogTemplate.DELTA_NAMED_CATALOG.applyTo(delta);
    assertEquals(LakeFormats.DELTA_CATALOG, delta.getImplementation());
    assertTrue(delta.getConfExtra().contains(LakeCatalogTemplate.Docs.DELTA));
  }

  @Test
  void everyTemplateIncludesDocsCommentInConfExtra() {
    for (LakeCatalogTemplate t : LakeCatalogTemplate.values()) {
      LakeCatalog cat = new LakeCatalog();
      t.applyTo(cat);
      assertTrue(
          cat.getConfExtra() != null && cat.getConfExtra().contains("# docs:"),
          () -> t.name() + " should include # docs: link in conf extra");
    }
  }

  @Test
  void confWithDocsFormatsHeaderAndBody() {
    assertEquals(
        "# docs: https://example.com", LakeCatalogTemplate.confWithDocs("https://example.com", ""));
    assertEquals(
        "# docs: https://example.com\nuri=thrift://x",
        LakeCatalogTemplate.confWithDocs("https://example.com", "uri=thrift://x"));
  }

  @Test
  void fromDisplayNameRoundTrip() {
    for (LakeCatalogTemplate t : LakeCatalogTemplate.values()) {
      assertEquals(t, LakeCatalogTemplate.fromDisplayName(t.getDisplayName()));
    }
    assertEquals(LakeCatalogTemplate.values().length, LakeCatalogTemplate.displayNames().length);
  }

  @Test
  void looksCustomizedDetectsEdits() {
    LakeCatalog fresh = new LakeCatalog();
    assertFalse(LakeCatalogTemplate.looksCustomized(fresh));

    LakeCatalog withName = new LakeCatalog();
    withName.setCatalogName("lake");
    assertTrue(LakeCatalogTemplate.looksCustomized(withName));

    LakeCatalog withWarehouse = new LakeCatalog();
    withWarehouse.setWarehouse("file:///tmp/wh");
    assertTrue(LakeCatalogTemplate.looksCustomized(withWarehouse));
  }

  @Test
  void everyTemplateAppliesWithoutNpe() {
    for (LakeCatalogTemplate t : LakeCatalogTemplate.values()) {
      LakeCatalog cat = new LakeCatalog();
      t.applyTo(cat);
      assertNotNull(cat.getCatalogType());
      assertNotNull(cat.getImplementation());
      assertNotNull(t.getDescription());
      assertEquals("", cat.getCredential());
    }
  }
}
