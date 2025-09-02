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
package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XmlInputStreamTest {
  private static final String INCORRECT_XML_DATA_VALUE_MESSAGE = "Incorrect xml data value - ";
  private static final String INCORRECT_XML_DATA_NAME_MESSAGE = "Incorrect xml data name - ";
  private static final String INCORRECT_XML_PATH_MESSAGE = "Incorrect xml path - ";
  private static final String INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE =
      "Incorrect xml data type description - ";

  private static final String ATTRIBUTE_2 = "ATTRIBUTE_2";

  private static final String ATTRIBUTE_1 = "ATTRIBUTE_1";

  private static final int START_ROW_IN_XML_TO_VERIFY = 9;

  private static TransformMockHelper<XmlInputStreamMeta, XmlInputStreamData> transformMockHelper;

  private XmlInputStreamMeta xmlInputStreamMeta;

  private XmlInputStreamData xmlInputStreamData;

  private TestRowListener rl;

  private int typeDescriptionPos = 0;
  private int pathPos = 1;
  private int dataNamePos = 2;
  private int dataValue = 3;

  @BeforeEach
  void setUp() throws HopException {
    transformMockHelper =
        new TransformMockHelper<>(
            "XmlInputStreamTest", XmlInputStreamMeta.class, XmlInputStreamData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    xmlInputStreamMeta = new XmlInputStreamMeta();
    xmlInputStreamMeta.setDefault();

    xmlInputStreamData = new XmlInputStreamData();
    rl = new TestRowListener();

    // Turn off several options.
    // So there are fields: xml_data_type_description - xml_path - xml_data_name - xml_data_value
    xmlInputStreamMeta.setIncludeXmlParentPathField(false);
    xmlInputStreamMeta.setIncludeXmlParentElementIDField(false);
    xmlInputStreamMeta.setIncludeXmlElementIDField(false);
    xmlInputStreamMeta.setIncludeXmlElementLevelField(false);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void testParseXmlWithPrefixes_WhenSetEnableNamespaceAsTrue() throws HopException, IOException {
    xmlInputStreamMeta.setFilename(createTestFile(getXMLString(getGroupWithPrefix())));
    xmlInputStreamMeta.setEnableNamespaces(true);

    doTest();

    // Assertions
    // check StartElement for the ProductGroup element
    // when namespaces are enabled, we have additional NAMESPACE events - 3 for our test xml;
    int expectedRowNum = START_ROW_IN_XML_TO_VERIFY + 3;
    assertEquals(
        "START_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);

    // attributes
    HashSet<String> expectedAttributeNames = new HashSet<>();
    HashSet<String> expectedAttributeValues = new HashSet<>();
    expectedAttributeNames.add("Fruits:attribute");
    expectedAttributeNames.add("Fish:attribute");
    expectedAttributeValues.add(ATTRIBUTE_1);
    expectedAttributeValues.add(ATTRIBUTE_2);
    // ATTRIBUTE_1
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertTrue(
        expectedAttributeNames.contains(rl.getWritten().get(expectedRowNum)[dataNamePos]),
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);
    // ATTRIBUTE_2
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertTrue(
        expectedAttributeNames.contains(rl.getWritten().get(expectedRowNum)[dataNamePos]),
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);

    // check EndElement for the ProductGroup element
    expectedRowNum = expectedRowNum + 2;
    assertEquals(
        "END_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "Fruits:ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);
  }

  @Test
  void testParseXmlWithPrefixes_WhenSetEnableNamespaceAsFalse() throws HopException, IOException {
    xmlInputStreamMeta.setFilename(createTestFile(getXMLString(getGroupWithPrefix())));
    xmlInputStreamMeta.setEnableNamespaces(false);

    doTest();

    // Assertions
    // check StartElement for the ProductGroup element
    int expectedRowNum = START_ROW_IN_XML_TO_VERIFY;
    assertEquals(
        "START_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);

    // attributes
    HashSet<String> expectedAttributeValues = new HashSet<>();
    expectedAttributeValues.add(ATTRIBUTE_1);
    expectedAttributeValues.add(ATTRIBUTE_2);
    // ATTRIBUTE_1
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "attribute",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);
    // ATTRIBUTE_2
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "attribute",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);

    // check EndElement for the ProductGroup element
    expectedRowNum = expectedRowNum + 2;
    assertEquals(
        "END_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);
  }

  @Test
  void testParseXmlWithoutPrefixes_WhenSetEnableNamespaceAsTrue() throws HopException, IOException {
    xmlInputStreamMeta.setFilename(createTestFile(getXMLString(getGroupWithoutPrefix())));
    xmlInputStreamMeta.setEnableNamespaces(true);

    doTest();

    // Assertions
    // check StartElement for the ProductGroup element
    // when namespaces are enabled, we have additional NAMESPACE events - 3 for our test xml;
    int expectedRowNum = START_ROW_IN_XML_TO_VERIFY + 3;
    assertEquals(
        "START_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);

    // attributes
    HashSet<String> expectedAttributeNames = new HashSet<>();
    HashSet<String> expectedAttributeValues = new HashSet<>();
    expectedAttributeNames.add("attribute1");
    expectedAttributeNames.add("attribute2");
    expectedAttributeValues.add(ATTRIBUTE_1);
    expectedAttributeValues.add(ATTRIBUTE_2);
    // ATTRIBUTE_1
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertTrue(
        expectedAttributeNames.contains(rl.getWritten().get(expectedRowNum)[dataNamePos]),
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);
    // ATTRIBUTE_2
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertTrue(
        expectedAttributeNames.contains(rl.getWritten().get(expectedRowNum)[dataNamePos]),
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertTrue(
        expectedAttributeValues.contains(rl.getWritten().get(expectedRowNum)[dataValue]),
        INCORRECT_XML_DATA_VALUE_MESSAGE);

    // check EndElement for the ProductGroup element
    expectedRowNum = expectedRowNum + 2;
    assertEquals(
        "END_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/Products/Product/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos],
        INCORRECT_XML_DATA_NAME_MESSAGE);
  }

  @Test
  void multiLineDataReadAsMultipleElements() throws HopException, IOException {
    xmlInputStreamMeta.setFilename(createTestFile(getMultiLineDataXml()));
    xmlInputStreamMeta.setEnableNamespaces(true);

    doTest();

    assertEquals("CHARACTERS", rl.getWritten().get(2)[0]);
    assertEquals("some data", rl.getWritten().get(2)[3]);
    assertEquals("CHARACTERS", rl.getWritten().get(3)[0]);
    assertEquals("other data", rl.getWritten().get(3)[3]);
  }

  private void doTest() throws HopException {
    XmlInputStream xmlInputStream =
        new XmlInputStream(
            transformMockHelper.transformMeta,
            xmlInputStreamMeta,
            xmlInputStreamData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    IRowMeta inputRowMeta = new RowMeta();
    xmlInputStream.setInputRowMeta(inputRowMeta);
    xmlInputStream.init();
    xmlInputStream.addRowListener(rl);
    boolean haveRowsToRead;
    do {
      haveRowsToRead = !xmlInputStream.processRow();

    } while (!haveRowsToRead);
  }

  @Test
  void testFromPreviousTransform() throws Exception {
    xmlInputStreamMeta.sourceFromInput = true;
    xmlInputStreamMeta.sourceFieldName = "inf";
    xmlInputStreamData.outputRowMeta = new RowMeta();

    RowMeta rm = new RowMeta();
    String xml = "<ProductGroup attribute1=\"v1\"/>";
    ValueMetaString ms = new ValueMetaString("inf");
    IRowSet rs = new SingleRowRowSet();
    rs.putRow(rm, new Object[] {xml});
    rs.setDone();

    XmlInputStream xmlInputStream =
        new XmlInputStream(
            transformMockHelper.transformMeta,
            xmlInputStreamMeta,
            xmlInputStreamData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    xmlInputStream.setInputRowMeta(rm);
    xmlInputStream.getInputRowMeta().addValueMeta(ms);
    xmlInputStream.addRowSetToInputRowSets(rs);
    xmlInputStream.setOutputRowSets(new ArrayList<>());

    xmlInputStream.init();
    xmlInputStream.addRowListener(rl);
    boolean haveRowsToRead;
    do {
      haveRowsToRead = !xmlInputStream.processRow();
    } while (!haveRowsToRead);

    int expectedRowNum = 1;

    assertEquals(
        "<ProductGroup attribute1=\"v1\"/>",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);

    assertEquals(
        "START_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos + 1],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos + 1],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos + 1],
        INCORRECT_XML_DATA_NAME_MESSAGE);

    // attributes
    // ATTRIBUTE_1
    expectedRowNum++;
    assertEquals(
        "ATTRIBUTE",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos + 1],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos + 1],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "attribute1",
        rl.getWritten().get(expectedRowNum)[dataNamePos + 1],
        INCORRECT_XML_DATA_NAME_MESSAGE);
    assertEquals(
        "v1", rl.getWritten().get(expectedRowNum)[dataValue + 1], INCORRECT_XML_DATA_VALUE_MESSAGE);

    // check EndElement for the ProductGroup element
    expectedRowNum++;
    assertEquals(
        "END_ELEMENT",
        rl.getWritten().get(expectedRowNum)[typeDescriptionPos + 1],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
    assertEquals(
        "/ProductGroup",
        rl.getWritten().get(expectedRowNum)[pathPos + 1],
        INCORRECT_XML_PATH_MESSAGE);
    assertEquals(
        "ProductGroup",
        rl.getWritten().get(expectedRowNum)[dataNamePos + 1],
        INCORRECT_XML_DATA_NAME_MESSAGE);
  }

  @Test
  void testFileNameEnteredManuallyWithIncomingHops() throws Exception {
    testCorrectFileSelected(getFile("default.xml"), 0);
  }

  @Test
  void testNotValidFilePathAndFileField() throws Exception {
    assertThrows(
        HopException.class,
        () -> {
          testCorrectFileSelected("notPathNorValidFieldName", 0);
        });
  }

  @Test
  void testEmptyFileField() throws Exception {
    assertThrows(
        HopException.class,
        () -> {
          testCorrectFileSelected(StringUtils.EMPTY, 0);
        });
  }

  private void testCorrectFileSelected(String filenameParam, int xmlTagsStartPosition)
      throws HopException {
    xmlInputStreamMeta.sourceFromInput = false;
    xmlInputStreamMeta.setFilename(filenameParam);
    xmlInputStreamData.outputRowMeta = new RowMeta();

    RowMeta rm = new RowMeta();
    String pathValue = getFile("default.xml");
    ValueMetaString ms = new ValueMetaString("filename");
    IRowSet rs = new SingleRowRowSet();
    rs.putRow(rm, new Object[] {pathValue});
    rs.setDone();

    XmlInputStream xmlInputStream =
        new XmlInputStream(
            transformMockHelper.transformMeta,
            xmlInputStreamMeta,
            xmlInputStreamData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
    xmlInputStream.setInputRowMeta(rm);
    xmlInputStream.getInputRowMeta().addValueMeta(ms);
    xmlInputStream.addRowSetToInputRowSets(rs);
    xmlInputStream.setOutputRowSets(new ArrayList<>());

    xmlInputStream.init();
    xmlInputStream.addRowListener(rl);
    boolean haveRowsToRead;
    do {
      haveRowsToRead = !xmlInputStream.processRow();
    } while (!haveRowsToRead);
    assertEquals(
        "START_ELEMENT",
        rl.getWritten().get(1)[xmlTagsStartPosition],
        INCORRECT_XML_DATA_TYPE_DESCRIPTION_MESSAGE);
  }

  private String createTestFile(String xmlContent) throws IOException {
    File tempFile = File.createTempFile("Test", ".xml");
    tempFile.deleteOnExit();
    Writer osw = new PrintWriter(tempFile, "UTF8");
    System.out.println(xmlContent);
    osw.write(xmlContent);
    osw.close();

    return tempFile.getAbsolutePath();
  }

  private String getXMLString(String group) {
    return "<Products xmlns:Fruits=\"http://dummy.example/fruits\" xmlns:Fish=\"http://dummy.example/fish\" xmlns=\"http://dummy.example/default\">"
        + getProduct(group)
        + "</Products>";
  }

  private String getProduct(String group) {
    return "<Product><Id>1</Id><Name>TEST_NAME</Name>" + group + "</Product>";
  }

  private String getGroupWithPrefix() {
    return "<Fruits:ProductGroup Fruits:attribute=\""
        + ATTRIBUTE_1
        + "\" Fish:attribute=\""
        + ATTRIBUTE_2
        + "\">G</Fruits:ProductGroup>";
  }

  private String getGroupWithoutPrefix() {
    return "<ProductGroup attribute1=\""
        + ATTRIBUTE_1
        + "\" attribute2=\""
        + ATTRIBUTE_2
        + "\">G</ProductGroup>";
  }

  private String getMultiLineDataXml() {
    return "<tag>" + "some data\n" + "<![CDATA[other data]]></tag>";
  }

  private class TestRowListener extends RowAdapter {
    private List<Object[]> written = new ArrayList<>();

    public List<Object[]> getWritten() {
      return written;
    }

    @Override
    public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
      written.add(row);
    }
  }

  private String getFile(String filename) {
    String inPrefix = '/' + this.getClass().getPackage().getName().replace('.', '/') + "/files/";
    URL res = this.getClass().getResource(inPrefix + filename);
    return res.getFile();
  }
}
