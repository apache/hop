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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * XML join of several source rows into one target document. The transform runs in the test thread,
 * so the parser factory calls can be counted: one for the target, one for all source rows.
 */
class XmlJoinSeveralRowsTest {

  private TransformMockHelper<XmlJoinMeta, XmlJoinData> tmh;

  @BeforeEach
  void setUp() {
    tmh = new TransformMockHelper<>("XmlJoin", XmlJoinMeta.class, XmlJoinData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void simpleJoinOfSeveralSourceRows() throws Exception {
    XmlJoinMeta meta = tmh.iTransformMeta;
    when(meta.getTargetXPath()).thenReturn("//root");

    try (MockedStatic<XmlParserFactoryProducer> producer =
        mockStatic(XmlParserFactoryProducer.class, CALLS_REAL_METHODS)) {
      String xml =
          join(
              "<root/>",
              sourceRowMeta("sourceField"),
              List.of(
                  new Object[] {"<a>1</a>"}, new Object[] {"<a>2</a>"}, new Object[] {"<a>3</a>"}));

      assertTrue(xml.endsWith("<root><a>1</a><a>2</a><a>3</a></root>"), xml);
      // One parser factory for the target document, one for all source rows.
      producer.verify(XmlParserFactoryProducer::createSecureDocBuilderFactory, times(2));
    }
  }

  @Test
  void complexJoinPlacesEachSourceRowByItsCompareValue() throws Exception {
    XmlJoinMeta meta = tmh.iTransformMeta;
    when(meta.getTargetXPath()).thenReturn("//item[@id='?']");
    when(meta.isComplexJoin()).thenReturn(true);
    when(meta.getJoinCompareField()).thenReturn("id");

    String xml =
        join(
            "<root><item id=\"1\"/><item id=\"2\"/></root>",
            sourceRowMeta("sourceField", "id"),
            List.of(
                new Object[] {"<v>x</v>", "2"},
                new Object[] {"<v>y</v>", "1"},
                new Object[] {"<v>z</v>", "2"}));

    assertTrue(
        xml.endsWith(
            "<root><item id=\"1\"><v>y</v></item><item id=\"2\"><v>x</v><v>z</v></item></root>"),
        xml);
  }

  private static IRowMeta sourceRowMeta(String... fields) {
    IRowMeta rowMeta = new RowMeta();
    for (String field : fields) {
      rowMeta.addValueMeta(new ValueMetaString(field));
    }
    return rowMeta;
  }

  /** Runs the join of one target row and the source rows; returns the XML of the output row. */
  private String join(String targetXml, IRowMeta sourceRowMeta, List<Object[]> sourceRows)
      throws Exception {
    XmlJoinMeta meta = tmh.iTransformMeta;
    when(meta.getSourceXmlTransform()).thenReturn("source");
    when(meta.getTargetXmlTransform()).thenReturn("target");
    when(meta.getSourceXmlField()).thenReturn("sourceField");
    when(meta.getTargetXmlField()).thenReturn("targetField");
    when(meta.getValueXmlField()).thenReturn("resultField");

    ITransformIOMeta transformIOMeta = mock(ITransformIOMeta.class);
    when(meta.getTransformIOMeta()).thenReturn(transformIOMeta);
    IStream targetStream = mock(IStream.class);
    IStream sourceStream = mock(IStream.class);
    when(targetStream.getTransformName()).thenReturn("target");
    when(sourceStream.getTransformName()).thenReturn("source");
    when(transformIOMeta.getInfoStreams()).thenReturn(List.of(targetStream, sourceStream));

    XmlJoin transform =
        spy(
            new XmlJoin(
                tmh.transformMeta, meta, new XmlJoinData(), 0, tmh.pipelineMeta, tmh.pipeline));

    IRowSet targetRowSet = tmh.getMockInputRowSet(new Object[][] {{targetXml}});
    IRowMeta targetRowMeta = new RowMeta();
    targetRowMeta.addValueMeta(new ValueMetaString("targetField"));
    when(targetRowSet.getRowMeta()).thenReturn(targetRowMeta);
    IRowSet sourceRowSet = tmh.getMockInputRowSet(sourceRows);
    when(sourceRowSet.getRowMeta()).thenReturn(sourceRowMeta);

    List<IRowSet> inputRowSets = new ArrayList<>();
    inputRowSets.add(sourceRowSet);
    inputRowSets.add(targetRowSet);
    transform.setInputRowSets(inputRowSets);
    doReturn(sourceRowSet).when(transform).findInputRowSet("source");
    doReturn(targetRowSet).when(transform).findInputRowSet("target");

    transform.init();
    List<String> output = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            output.add((String) row[0]);
          }
        });

    while (transform.processRow()) {
      // one call per source row
    }
    assertEquals(0, transform.getErrors());
    assertEquals(1, output.size());
    return output.get(0);
  }
}
