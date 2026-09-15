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

package org.apache.hop.pipeline.transforms.odata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ODataInputMeta} */
class ODataInputMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void constructorDefaultsAuthTypeAndEmptyFields() {
    ODataInputMeta meta = new ODataInputMeta();
    assertEquals("NONE", meta.getAuthType());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void setDefaultClearsConnectionAndQuery() {
    ODataInputMeta meta = new ODataInputMeta();
    meta.setUrl("https://example.test/odata");
    meta.setEntitySet("Products");
    meta.setAuthType("BASIC");
    meta.getFields().add(new ODataField("Id", "Id", IValueMeta.TYPE_INTEGER, ""));

    meta.setDefault();

    assertEquals("", meta.getUrl());
    assertEquals("", meta.getEntitySet());
    assertEquals("NONE", meta.getAuthType());
    assertEquals("", meta.getUsername());
    assertEquals("", meta.getPassword());
    assertEquals("", meta.getToken());
    assertEquals("", meta.getQuerySelect());
    assertEquals("", meta.getQueryFilter());
    assertEquals("", meta.getQueryOrder());
    assertEquals("", meta.getQueryTop());
    assertEquals("", meta.getQuerySkip());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void getFieldsAddsConfiguredValueMeta() throws Exception {
    ODataInputMeta meta = new ODataInputMeta();
    meta.getFields().add(new ODataField("ProductID", "ProductID", IValueMeta.TYPE_INTEGER, "#"));
    meta.getFields().add(new ODataField("${NAME}", "ProductName", IValueMeta.TYPE_STRING, ""));

    Variables variables = new Variables();
    variables.setVariable("NAME", "ProductName");
    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "OData Input", null, null, variables, new MemoryMetadataProvider());

    assertEquals(2, rowMeta.size());
    assertEquals("ProductID", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(0).getType());
    assertEquals("OData Input", rowMeta.getValueMeta(0).getOrigin());
    assertEquals("ProductName", rowMeta.getValueMeta(1).getName());
  }

  @Test
  void checkReportsMissingUrlAndEntitySet() {
    ODataInputMeta meta = new ODataInputMeta();
    TransformMeta transformMeta = new TransformMeta("OData Input", meta);
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        new PipelineMeta(),
        transformMeta,
        null,
        new String[0],
        new String[0],
        null,
        new Variables(),
        new MemoryMetadataProvider());

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    assertTrue(remarks.get(0).getText().contains("Service URL"));
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
    assertTrue(remarks.get(1).getText().contains("Entity Set"));
  }

  @Test
  void checkTreatsWhitespaceAsMissing() {
    ODataInputMeta meta = new ODataInputMeta();
    meta.setUrl("   ");
    meta.setEntitySet("\t");
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta("OData Input", meta),
        null,
        new String[0],
        new String[0],
        null,
        new Variables(),
        new MemoryMetadataProvider());

    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
  }

  @Test
  void checkAcceptsConfiguredUrlAndEntitySet() {
    ODataInputMeta meta = new ODataInputMeta();
    meta.setUrl("https://example.test/odata");
    meta.setEntitySet("Products");
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta("OData Input", meta),
        null,
        new String[0],
        new String[0],
        null,
        new Variables(),
        new MemoryMetadataProvider());

    assertEquals(2, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  @Test
  void xmlRoundTripPreservesConnectionQueryAndFields() throws Exception {
    ODataInputMeta meta = loadFromClasspath("/odata-input.xml");
    validateLoadedMeta(meta);

    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    ODataInputMeta copy = loadFromXml(xmlCopy);
    validateLoadedMeta(copy);
  }

  private static void validateLoadedMeta(ODataInputMeta meta) {
    assertEquals("https://example.test/odata", meta.getUrl());
    assertEquals("Products", meta.getEntitySet());
    assertEquals("BASIC", meta.getAuthType());
    assertEquals("odata-user", meta.getUsername());
    assertEquals("odata-secret", meta.getPassword());
    assertEquals("bearer-token", meta.getToken());
    assertEquals("ProductID,ProductName", meta.getQuerySelect());
    assertEquals("Discontinued eq false", meta.getQueryFilter());
    assertEquals("ProductName asc", meta.getQueryOrder());
    assertEquals("3", meta.getQueryTop());
    assertEquals("1", meta.getQuerySkip());
    assertEquals(2, meta.getFields().size());
    assertEquals(
        new ODataField("ProductID", "ProductID", IValueMeta.TYPE_INTEGER, "#"),
        meta.getFields().get(0));
    assertEquals("ProductName", meta.getFields().get(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, meta.getFields().get(1).getType());
  }

  private static ODataInputMeta loadFromClasspath(String resource) throws Exception {
    Path path =
        Paths.get(Objects.requireNonNull(ODataInputMetaTest.class.getResource(resource)).toURI());
    return loadFromXml(Files.readString(path));
  }

  private static ODataInputMeta loadFromXml(String xml) throws HopXmlException {
    ODataInputMeta meta = new ODataInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        ODataInputMeta.class,
        meta,
        new MemoryMetadataProvider());
    return meta;
  }
}
