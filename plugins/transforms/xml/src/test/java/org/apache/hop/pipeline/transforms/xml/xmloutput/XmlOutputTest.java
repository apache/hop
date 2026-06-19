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
package org.apache.hop.pipeline.transforms.xml.xmloutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

class XmlOutputTest {
  private XmlOutput xmlOutput;
  private XmlOutputData xmlOutputData;
  private final Pipeline pipeline = mock(Pipeline.class);
  private static final String[] ILLEGAL_CHARACTERS_IN_XML_ATTRIBUTES = {"<", ">", "&", "'", "\""};

  private static Object[] rowWithData;
  private static Object[] rowWithNullData;

  @BeforeAll
  static void setUpBeforeClass() {

    rowWithData = initRowWithData();
    rowWithNullData = new Object[15];
  }

  @BeforeEach
  void setup() throws Exception {

    TransformMockHelper<XmlOutputMeta, XmlOutputData> transformMockHelper =
        new TransformMockHelper<>("XML_OUTPUT_TEST", XmlOutputMeta.class, XmlOutputData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    TransformMeta mockMeta = mock(TransformMeta.class);
    when(transformMockHelper.pipelineMeta.findTransform(ArgumentMatchers.anyString()))
        .thenReturn(mockMeta);
    when(pipeline.getLogLevel()).thenReturn(LogLevel.DEBUG);

    // Create and set Meta with some realistic data
    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.setOutputFields(initOutputFields(rowWithData.length, ContentType.Attribute));
    // Set as true to prevent unnecessary for this test checks at initialization
    xmlOutputMeta.getFileDetails().setDoNotOpenNewFileInit(false);

    xmlOutputData = new XmlOutputData();
    xmlOutputData.formatRowMeta = initRowMeta(rowWithData.length);
    xmlOutputData.fieldnrs = initFieldNumbers(rowWithData.length);
    xmlOutputData.OpenedNewFile = true;

    TransformMeta transformMeta =
        new TransformMeta("TransformMetaId", "TransformMetaName", xmlOutputMeta);
    xmlOutput =
        spy(
            new XmlOutput(
                transformMeta,
                xmlOutputMeta,
                xmlOutputData,
                0,
                transformMockHelper.pipelineMeta,
                transformMockHelper.pipeline));
  }

  @Test
  void testSpecialSymbolsInAttributeValuesAreEscaped() throws HopException, XMLStreamException {
    xmlOutput.init();

    xmlOutputData.writer = mock(XMLStreamWriter.class);
    xmlOutput.writeRowAttributes(rowWithData);
    xmlOutput.dispose();
    verify(xmlOutputData.writer, times(rowWithData.length)).writeAttribute(any(), any());
    verify(xmlOutput, atLeastOnce()).closeOutputStream(any());
  }

  @Test
  void testNullInAttributeValuesAreEscaped() throws HopException, XMLStreamException {

    testNullValuesInAttribute();
  }

  /** Testing to verify that getIfPresent defaults the XMLField ContentType value */
  @Test
  void testDefaultXmlFieldContentType() {
    List<XmlField> xmlFields = initOutputFields(4, null);
    xmlFields.get(0).setContentType(ContentType.getIfPresent("Element"));
    xmlFields.get(1).setContentType(ContentType.getIfPresent("Attribute"));
    xmlFields.get(2).setContentType(ContentType.getIfPresent(""));
    xmlFields.get(3).setContentType(ContentType.getIfPresent("WrongValue"));
    assertEquals(ContentType.Element, xmlFields.get(0).getContentType());
    assertEquals(ContentType.Attribute, xmlFields.get(1).getContentType());
    assertEquals(ContentType.Element, xmlFields.get(2).getContentType());
    assertEquals(ContentType.Element, xmlFields.get(3).getContentType());
  }

  private void testNullValuesInAttribute() throws HopException, XMLStreamException {
    xmlOutput.init();
    xmlOutputData.writer = mock(XMLStreamWriter.class);
    xmlOutput.writeRowAttributes(rowWithNullData);
    xmlOutput.dispose();
    verify(xmlOutputData.writer, times(0)).writeAttribute(any(), any());
    verify(xmlOutput, atLeastOnce()).closeOutputStream(any());
  }

  private static Object[] initRowWithData() {
    String[] inputData = ILLEGAL_CHARACTERS_IN_XML_ATTRIBUTES;
    Object[] data = new Object[inputData.length * 3];
    for (int i = 0; i < inputData.length; i++) {
      data[3 * i] = inputData[i] + "TEST";
      data[3 * i + 1] = "TEST" + inputData[i] + "TEST";
      data[3 * i + 2] = "TEST" + inputData[i];
    }
    return data;
  }

  private IRowMeta initRowMeta(int count) {
    IRowMeta rm = new RowMeta();
    for (int i = 0; i < count; i++) {
      rm.addValueMeta(new ValueMetaString("string"));
    }
    return rm;
  }

  private List<XmlField> initOutputFields(int amount, ContentType attribute) {

    List<XmlField> fields = new ArrayList<>();
    for (int j = 0; j < amount; j++) {
      fields.add(
          new XmlField(attribute, "Fieldname" + (j + 1), "ElementName" + (j + 1), 2, -1, -1));
    }
    return fields;
  }

  private int[] initFieldNumbers(int i) {
    int[] fieldNumbers = new int[i];
    for (int j = 0; j < fieldNumbers.length; j++) {
      fieldNumbers[j] = j;
    }
    return fieldNumbers;
  }
}
