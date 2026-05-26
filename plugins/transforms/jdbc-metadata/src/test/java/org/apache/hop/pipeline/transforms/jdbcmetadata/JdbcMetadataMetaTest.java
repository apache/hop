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
package org.apache.hop.pipeline.transforms.jdbcmetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Behaviour tests for {@link JdbcMetadataMeta}. The XML round-trip in particular catches the common
 * class of regression where a {@code @HopMetadataProperty} field is removed, renamed, or its
 * key/groupKey changed - any of which would silently drop data from existing pipelines.
 */
class JdbcMetadataMetaTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void setDefaultPopulatesGetCatalogsAndOneOutputField() {
    JdbcMetadataMeta meta = new JdbcMetadataMeta();
    meta.setDefault();

    assertEquals("getCatalogs", meta.getMethodName());
    assertFalse(meta.isArgumentSourceFields());
    assertNotNull(meta.getOutputFields());
    assertEquals(1, meta.getOutputFields().size());
    OutputField field = meta.getOutputFields().get(0);
    assertEquals("TABLE_CAT", field.getName());
    assertEquals("TABLE_CAT", field.getRename());
  }

  /**
   * Configure every {@code @HopMetadataProperty} field, serialize to XML, deserialize into a fresh
   * instance and assert every value survived. If a property loses its annotation or its
   * key/groupKey is renamed, this test fails.
   */
  @Test
  void xmlRoundTripPreservesAllProperties() throws Exception {
    JdbcMetadataMeta original = new JdbcMetadataMeta();
    original.setConnection("my-jdbc-connection");
    original.setAlwaysPassInputRow(true);
    original.setMethodName("getTables");
    original.setArgumentSourceFields(true);
    original.setRemoveArgumentFields(true);
    original.setArguments(new ArrayList<>(Arrays.asList("catalogArg", "schemaArg", "tableArg")));

    List<OutputField> outputFields = new ArrayList<>();
    outputFields.add(new OutputField("TABLE_NAME", "tableName"));
    outputFields.add(new OutputField("REMARKS", "comment"));
    original.setOutputFields(outputFields);

    JdbcMetadataMeta copy = serializeAndDeserialize(original);

    assertEquals(original.getConnection(), copy.getConnection());
    assertEquals(original.isAlwaysPassInputRow(), copy.isAlwaysPassInputRow());
    assertEquals(original.getMethodName(), copy.getMethodName());
    assertEquals(original.isArgumentSourceFields(), copy.isArgumentSourceFields());
    assertEquals(original.isRemoveArgumentFields(), copy.isRemoveArgumentFields());
    assertEquals(original.getArguments(), copy.getArguments());

    assertNotNull(copy.getOutputFields());
    assertEquals(2, copy.getOutputFields().size());
    assertEquals("TABLE_NAME", copy.getOutputFields().get(0).getName());
    assertEquals("tableName", copy.getOutputFields().get(0).getRename());
    assertEquals("REMARKS", copy.getOutputFields().get(1).getName());
    assertEquals("comment", copy.getOutputFields().get(1).getRename());
  }

  /** A default-only Meta must round-trip without losing or fabricating data. */
  @Test
  void xmlRoundTripWithDefaultsIsIdempotent() throws Exception {
    JdbcMetadataMeta original = new JdbcMetadataMeta();
    original.setDefault();

    JdbcMetadataMeta copy = serializeAndDeserialize(original);

    assertEquals(original.getMethodName(), copy.getMethodName());
    assertEquals(original.isArgumentSourceFields(), copy.isArgumentSourceFields());
    assertNotNull(copy.getOutputFields());
    assertEquals(original.getOutputFields().size(), copy.getOutputFields().size());
    assertEquals(
        original.getOutputFields().get(0).getName(), copy.getOutputFields().get(0).getName());
  }

  @Test
  void argumentsListRoundTripsEmpty() throws Exception {
    JdbcMetadataMeta original = new JdbcMetadataMeta();
    original.setMethodName("getCatalogs");
    original.setArguments(new ArrayList<>());

    JdbcMetadataMeta copy = serializeAndDeserialize(original);

    assertNotNull(copy.getArguments());
    assertTrue(copy.getArguments().isEmpty());
  }

  private static JdbcMetadataMeta serializeAndDeserialize(JdbcMetadataMeta source)
      throws Exception {
    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(source)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    JdbcMetadataMeta copy = new JdbcMetadataMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        JdbcMetadataMeta.class,
        copy,
        new MemoryMetadataProvider());
    return copy;
  }
}
