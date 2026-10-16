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
package org.apache.hop.pipeline.transforms.xml.addxml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * Add XML with a blank Encoding must produce the same XML as up to 2.17, where a saved {@code
 * <encoding/>} loaded as null and the serializer kept its default (UTF-8) encoding. Since 2.18 the
 * tag loads as an empty string.
 */
class AddXmlBlankEncodingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<AddXmlMeta, AddXmlData> tmh;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    tmh = new TransformMockHelper<>("AddXml", AddXmlMeta.class, AddXmlData.class);
    doReturn(tmh.iLogChannel).when(tmh.logChannelFactory).create(any(), any(ILoggingObject.class));
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void blankEncodingProducesDefaultUtf8Declaration() throws Exception {
    String xml = runAddXml("<encoding/>");

    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Row><name>café</name></Row>", xml);
    assertTrue(XmlHandler.loadXmlString(xml) != null);
  }

  @Test
  void specifiedEncodingIsWrittenInDeclaration() throws Exception {
    String xml = runAddXml("<encoding>ISO-8859-1</encoding>");

    assertEquals(
        "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><Row><name>café</name></Row>", xml);
  }

  private String runAddXml(String encodingTag) throws Exception {
    AddXmlMeta meta = loadMeta(encodingTag);
    AddXml addXml =
        new AddXml(tmh.transformMeta, meta, new AddXmlData(), 0, tmh.pipelineMeta, tmh.pipeline);
    assertTrue(addXml.init(), "init failed");

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    IRowSet rowSet = tmh.getMockInputRowSet(new Object[][] {{"café"}});
    when(rowSet.getRowMeta()).thenReturn(inputRowMeta);
    addXml.setInputRowSets(new ArrayList<>(Collections.singletonList(rowSet)));

    List<Object[]> written = new ArrayList<>();
    addXml.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            written.add(row);
          }
        });

    assertTrue(addXml.processRow());
    assertEquals(0, addXml.getErrors());
    assertEquals(1, written.size());
    return (String) written.get(0)[1];
  }

  private static AddXmlMeta loadMeta(String encodingTag) throws Exception {
    String xml =
        "<transform>"
            + encodingTag
            + "<valueName>xml</valueName>"
            + "<xml_repeat_element>Row</xml_repeat_element>"
            + "<file><omitXMLheader>N</omitXMLheader><omitNullValues>N</omitNullValues></file>"
            + "<fields><field>"
            + "<name>name</name><element>name</element><type>String</type>"
            + "<format/><currency/><decimal/><group/><nullif/>"
            + "<length>-1</length><precision>-1</precision>"
            + "<attribute>N</attribute><attributeParentName/>"
            + "</field></fields>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    AddXmlMeta meta = new AddXmlMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
