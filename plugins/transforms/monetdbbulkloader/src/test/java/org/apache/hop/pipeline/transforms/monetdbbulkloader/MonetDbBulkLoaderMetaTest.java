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

package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class MonetDbBulkLoaderMetaTest {

  @BeforeEach
  void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    DatabaseMeta dbMeta = new DatabaseMeta();
    dbMeta.setName("unit-test-db");
    metadataProvider.getSerializer(DatabaseMeta.class).save(dbMeta);

    Document document =
        XmlHandler.loadXmlFile(
            MonetDbBulkLoaderMeta.class.getResourceAsStream("/monetdbbulkloader-transform.xml"));
    Node node = XmlHandler.getSubNode(document, TransformMeta.XML_TAG);

    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    meta.loadXml(node, metadataProvider);

    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + meta.getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    Document copyDocument = XmlHandler.loadXmlString(xml);
    Node copyNode = XmlHandler.getSubNode(copyDocument, TransformMeta.XML_TAG);
    MonetDbBulkLoaderMeta copy = new MonetDbBulkLoaderMeta();
    copy.loadXml(copyNode, metadataProvider);

    assertEquals(meta.getXml(), copy.getXml());

    assertEquals("unit-test-db", meta.getDbConnectionName());
    assertEquals("myschema", meta.getSchemaName());
    assertEquals("mytable", meta.getTableName());
    assertEquals("100000", meta.getBufferSize());
    assertEquals("/tmp/monetdb.log", meta.getLogFile());
    assertTrue(meta.isTruncate());
    assertTrue(meta.isFullyQuoteSQL());
    assertEquals("|", meta.getFieldSeparator());
    assertEquals("\"", meta.getFieldEnclosure());
    assertEquals("null", meta.getNullRepresentation());
    assertEquals("UTF-8", meta.getEncoding());

    // Assert field mappings list loaded correctly from XML
    assertNotNull(meta.getFields());
    assertEquals(2, meta.getFields().size(), "Expected 2 mapping elements");

    MonetDbBulkLoaderMeta.MonetDbField first = meta.getFields().get(0);
    assertEquals("col1", first.getFieldTable(), "first mapping stream_name -> fieldTable");
    assertEquals("field1", first.getFieldStream(), "first mapping field_name -> fieldStream");
    assertTrue(first.isFieldFormatOk(), "first mapping field_format_ok Y -> true");

    MonetDbBulkLoaderMeta.MonetDbField second = meta.getFields().get(1);
    assertEquals("col2", second.getFieldTable(), "second mapping stream_name -> fieldTable");
    assertEquals("field2", second.getFieldStream(), "second mapping field_name -> fieldStream");
    assertFalse(second.isFieldFormatOk(), "second mapping field_format_ok N -> false");

    // Round-trip: copy should have same field mappings
    assertNotNull(copy.getFields());
    assertEquals(meta.getFields().size(), copy.getFields().size());
    for (int i = 0; i < meta.getFields().size(); i++) {
      MonetDbBulkLoaderMeta.MonetDbField orig = meta.getFields().get(i);
      MonetDbBulkLoaderMeta.MonetDbField copied = copy.getFields().get(i);
      assertEquals(orig.getFieldTable(), copied.getFieldTable(), "field " + i + " table");
      assertEquals(orig.getFieldStream(), copied.getFieldStream(), "field " + i + " stream");
      assertEquals(orig.isFieldFormatOk(), copied.isFieldFormatOk(), "field " + i + " formatOk");
    }
  }

  @Test
  void testSetDefault() {
    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    meta.setDefault();

    assertEquals("", meta.getSchemaName());
    assertNotNull(meta.getTableName());
    assertEquals("100000", meta.getBufferSize());
    assertEquals("", meta.getLogFile());
    assertTrue(meta.isFullyQuoteSQL());
    assertEquals("|", meta.getFieldSeparator());
    assertEquals("\"", meta.getFieldEnclosure());
    assertEquals("", meta.getNullRepresentation());
    assertEquals("UTF-8", meta.getEncoding());
  }

  @Test
  void testClone() {
    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    meta.setSchemaName("s");
    meta.setTableName("t");

    MonetDbBulkLoaderMeta clone = (MonetDbBulkLoaderMeta) meta.clone();

    assertNotNull(clone);
    assertEquals(meta.getSchemaName(), clone.getSchemaName());
    assertEquals(meta.getTableName(), clone.getTableName());
  }

  @Test
  void testGetFieldsDoesNotModifyInputRowMeta() throws Exception {
    MonetDbBulkLoaderMeta meta = new MonetDbBulkLoaderMeta();
    IRowMeta inputRowMeta = new RowMeta();
    int originalSize = inputRowMeta.size();

    meta.getFields(
        inputRowMeta, "origin", null, null, new Variables(), (IHopMetadataProvider) null);

    assertEquals(originalSize, inputRowMeta.size());
  }
}
