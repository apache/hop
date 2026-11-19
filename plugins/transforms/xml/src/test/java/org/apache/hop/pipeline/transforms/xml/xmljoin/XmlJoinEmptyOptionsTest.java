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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The dialog leaves an option that was never set at null, as other dialogs built from annotations
 * do. The transform reports such an option, null or an empty string, by name instead of failing on
 * the null. The result field left empty gets an empty name, as an empty string always did.
 */
class XmlJoinEmptyOptionsTest {
  private static final Class<?> PKG = XmlJoinMeta.class;

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

  static Stream<Arguments> emptyOptions() {
    List<Arguments> arguments = new ArrayList<>();
    for (String empty : new String[] {null, ""}) {
      arguments.add(
          Arguments.of(
              "target XML field " + empty,
              (Consumer<XmlJoinMeta>) m -> when(m.getTargetXmlField()).thenReturn(empty),
              "XmlJoin.CheckResult.TargetXMLFieldNotSpecified"));
      arguments.add(
          Arguments.of(
              "XPath statement " + empty,
              (Consumer<XmlJoinMeta>) m -> when(m.getTargetXPath()).thenReturn(empty),
              "XmlJoin.CheckResult.TargetXPathNotSpecified"));
      arguments.add(
          Arguments.of(
              "source XML field " + empty,
              (Consumer<XmlJoinMeta>) m -> when(m.getSourceXmlField()).thenReturn(empty),
              "XmlJoin.CheckResult.SourceXMLFieldNotSpecified"));
      arguments.add(
          Arguments.of(
              "key field " + empty,
              (Consumer<XmlJoinMeta>)
                  m -> {
                    when(m.isComplexJoin()).thenReturn(true);
                    when(m.getTargetXPath()).thenReturn("//item[@id='?']");
                    when(m.getJoinCompareField()).thenReturn(empty);
                  },
              "XmlJoin.Exception.KeyFieldNotSpecified"));
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("emptyOptions")
  void anEmptyOptionIsReportedByName(
      String description, Consumer<XmlJoinMeta> emptyOption, String messageKey) throws Exception {
    XmlJoinMeta meta = tmh.iTransformMeta;
    setUpJoin(meta);
    emptyOption.accept(meta);
    XmlJoin transform = transform(meta);

    HopException e =
        assertThrows(
            HopException.class,
            () -> {
              while (transform.processRow()) {
                // until the option is needed
              }
            },
            description);

    String expected = BaseMessages.getString(PKG, messageKey);
    assertFalse(expected.startsWith("!"), "no message " + messageKey);
    assertTrue(e.getMessage().contains(expected), description + ": " + e.getMessage());
  }

  @Test
  void anEmptyResultFieldHasAnEmptyName() throws Exception {
    XmlJoinMeta meta = new XmlJoinMeta();
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("doc"));

    meta.getFields(row, "join", new IRowMeta[] {row.clone(), null}, null, new Variables(), null);

    assertEquals(2, row.size());
    assertEquals("", row.getValueMeta(1).getName());
  }

  /** A simple join with every option set, except what a test leaves empty. */
  private static void setUpJoin(XmlJoinMeta meta) {
    when(meta.getSourceXmlTransform()).thenReturn("source");
    when(meta.getTargetXmlTransform()).thenReturn("target");
    when(meta.getSourceXmlField()).thenReturn("sourceField");
    when(meta.getTargetXmlField()).thenReturn("targetField");
    when(meta.getValueXmlField()).thenReturn("resultField");
    when(meta.getTargetXPath()).thenReturn("//root");
    when(meta.getJoinCompareField()).thenReturn("id");
  }

  private XmlJoin transform(XmlJoinMeta meta) throws Exception {
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

    IRowSet targetRowSet =
        tmh.getMockInputRowSet(new Object[][] {{"<root><item id=\"1\"/></root>"}});
    IRowMeta targetRowMeta = new RowMeta();
    targetRowMeta.addValueMeta(new ValueMetaString("targetField"));
    when(targetRowSet.getRowMeta()).thenReturn(targetRowMeta);
    IRowSet sourceRowSet = tmh.getMockInputRowSet(new Object[][] {{"<a/>", "1"}});
    IRowMeta sourceRowMeta = new RowMeta();
    sourceRowMeta.addValueMeta(new ValueMetaString("sourceField"));
    sourceRowMeta.addValueMeta(new ValueMetaString("id"));
    when(sourceRowSet.getRowMeta()).thenReturn(sourceRowMeta);

    List<IRowSet> inputRowSets = new ArrayList<>();
    inputRowSets.add(sourceRowSet);
    inputRowSets.add(targetRowSet);
    transform.setInputRowSets(inputRowSets);
    doReturn(sourceRowSet).when(transform).findInputRowSet("source");
    doReturn(targetRowSet).when(transform).findInputRowSet("target");
    transform.init();
    return transform;
  }
}
