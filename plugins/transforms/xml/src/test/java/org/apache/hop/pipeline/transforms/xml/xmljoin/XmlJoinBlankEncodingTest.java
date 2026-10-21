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
package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
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
 * XML Join with a blank Encoding must produce the same XML as up to 2.17, where a saved {@code
 * <encoding/>} loaded as null and the serializer kept its default (UTF-8) encoding. Since 2.18 the
 * tag loads as an empty string.
 */
class XmlJoinBlankEncodingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<XmlJoinMeta, XmlJoinData> tmh;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    tmh = new TransformMockHelper<>("XmlJoin", XmlJoinMeta.class, XmlJoinData.class);
    doReturn(tmh.iLogChannel).when(tmh.logChannelFactory).create(any(), any(ILoggingObject.class));
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void blankEncodingProducesDefaultUtf8Declaration() throws Exception {
    String xml = runXmlJoin("<encoding/>");

    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><child>café</child></root>", xml);
    assertTrue(XmlHandler.loadXmlString(xml) != null);
  }

  @Test
  void specifiedEncodingIsWrittenInDeclaration() throws Exception {
    String xml = runXmlJoin("<encoding>ISO-8859-1</encoding>");

    assertEquals(
        "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><root><child>café</child></root>", xml);
  }

  private String runXmlJoin(String encodingTag) throws Exception {
    XmlJoinMeta meta = loadMeta(encodingTag);
    XmlJoin xmlJoin =
        spy(
            new XmlJoin(
                tmh.transformMeta, meta, new XmlJoinData(), 0, tmh.pipelineMeta, tmh.pipeline));

    IRowSet targetRowSet = rowSet("targetField", "<root/>");
    IRowSet sourceRowSet = rowSet("sourceField", "<child>café</child>");
    List<IRowSet> inputRowSets = new ArrayList<>();
    inputRowSets.add(sourceRowSet);
    inputRowSets.add(targetRowSet);
    xmlJoin.setInputRowSets(inputRowSets);
    doReturn(targetRowSet).when(xmlJoin).findInputRowSet("target");
    doReturn(sourceRowSet).when(xmlJoin).findInputRowSet("source");

    assertTrue(xmlJoin.init(), "init failed");

    List<Object[]> written = new ArrayList<>();
    xmlJoin.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            written.add(row);
          }
        });

    assertTrue(xmlJoin.processRow());
    assertFalse(xmlJoin.processRow());
    assertEquals(1, written.size());
    return (String) written.get(0)[1];
  }

  private IRowSet rowSet(String fieldName, String value) {
    IRowSet rowSet = tmh.getMockInputRowSet(new Object[][] {{value}});
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(fieldName));
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    return rowSet;
  }

  private static XmlJoinMeta loadMeta(String encodingTag) throws Exception {
    String xml =
        "<transform>"
            + "<complexJoin>N</complexJoin>"
            + "<targetXmlTransform>target</targetXmlTransform>"
            + "<targetXmlField>targetField</targetXmlField>"
            + "<sourceXmlTransform>source</sourceXmlTransform>"
            + "<sourceXmlField>sourceField</sourceXmlField>"
            + "<valueXmlField>result</valueXmlField>"
            + "<targetXPath>/root</targetXPath>"
            + "<joinCompareField/>"
            + encodingTag
            + "<omitXmlHeader>N</omitXmlHeader>"
            + "<omitNullValues>N</omitNullValues>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    XmlJoinMeta meta = new XmlJoinMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
