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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * XSD validator over several rows: the schema factory is created and each XSD compiled once per
 * copy, and every row still gets the same result and message as before. The transform runs in the
 * test thread, so the factory calls can be counted.
 */
class XsdValidatorSchemaCacheTest {

  private static final String XSD_ORDER =
      "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'><xs:element name='order'>"
          + "<xs:complexType><xs:sequence><xs:element name='id' type='xs:int'/></xs:sequence>"
          + "</xs:complexType></xs:element></xs:schema>";
  private static final String XSD_ITEM = XSD_ORDER.replace("'order'", "'item'");
  private static final String VALID_ORDER = "<order><id>1</id></order>";
  private static final String INVALID_ORDER = "<order><id>x</id></order>";
  private static final String VALID_ITEM = "<item><id>2</id></item>";

  @TempDir Path tempDir;

  private TransformMockHelper<XsdValidatorMeta, XsdValidatorData> tmh;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    tmh =
        new TransformMockHelper<>("XSD validator", XsdValidatorMeta.class, XsdValidatorData.class);
    when(tmh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(tmh.iLogChannel);
    when(tmh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    tmh.cleanUp();
  }

  @Test
  void fixedXsdFileCreatesTheFactoryOnce() throws Exception {
    String xsd = write("order.xsd", XSD_ORDER);
    XsdValidatorMeta meta = meta(false);
    meta.setXsdSource(XsdValidatorMeta.SPECIFY_FILENAME);
    meta.setXsdFilename(xsd);

    try (MockedStatic<XmlParserFactoryProducer> producer =
        mockStatic(XmlParserFactoryProducer.class, CALLS_REAL_METHODS)) {
      Run run = new Run(meta, xmlOnly(), rows(VALID_ORDER, INVALID_ORDER, VALID_ORDER));
      run.all();

      assertEquals(List.of(true, false, true), run.results());
      assertNull(run.messages().get(0));
      assertNotNull(run.messages().get(1));
      assertNull(run.messages().get(2));
      producer.verify(
          () -> XmlParserFactoryProducer.createSecureSchemaFactory(anyString()), times(1));
    }
  }

  /**
   * Each XSD from the field is compiled the first time it is used. Overwriting the files after that
   * no longer changes the results: the compiled schemas are kept for the run.
   */
  @Test
  void xsdFromFieldIsCompiledOncePerFile() throws Exception {
    Path orderXsd = tempDir.resolve("order.xsd");
    Path itemXsd = tempDir.resolve("item.xsd");
    Files.writeString(orderXsd, XSD_ORDER, StandardCharsets.UTF_8);
    Files.writeString(itemXsd, XSD_ITEM, StandardCharsets.UTF_8);
    XsdValidatorMeta meta = meta(false);
    meta.setXsdSource(XsdValidatorMeta.SPECIFY_FIELDNAME);
    meta.setXsdDefinedField("xsd");

    Run run =
        new Run(
            meta,
            xmlAndXsd(),
            List.of(
                new Object[] {VALID_ORDER, orderXsd.toString()},
                new Object[] {VALID_ITEM, itemXsd.toString()},
                new Object[] {INVALID_ORDER, orderXsd.toString()},
                new Object[] {VALID_ITEM, itemXsd.toString()},
                new Object[] {VALID_ORDER, orderXsd.toString()}));
    run.next();
    run.next();
    Files.writeString(orderXsd, "not a schema", StandardCharsets.UTF_8);
    Files.writeString(itemXsd, "not a schema", StandardCharsets.UTF_8);
    run.all();

    assertEquals(List.of(true, true, false, true, true), run.results());
    assertNotNull(run.messages().get(2));
  }

  /** A schema that doesn't compile is not kept: every row that uses it reports the error. */
  @Test
  void xsdThatFailsToCompileReportsTheErrorOnEveryRow() throws Exception {
    String broken = write("broken.xsd", "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'>");
    String order = write("order.xsd", XSD_ORDER);
    XsdValidatorMeta meta = meta(false);
    meta.setXsdSource(XsdValidatorMeta.SPECIFY_FIELDNAME);
    meta.setXsdDefinedField("xsd");

    Run run =
        new Run(
            meta,
            xmlAndXsd(),
            List.of(
                new Object[] {VALID_ORDER, broken},
                new Object[] {VALID_ORDER, order},
                new Object[] {VALID_ORDER, broken}));
    run.all();

    assertEquals(List.of(false, true, false), run.results());
    assertNotNull(run.messages().get(0));
    assertEquals(run.messages().get(0), run.messages().get(2));
  }

  @Test
  void externalEntitiesAreStillRefusedOnEveryRow() throws Exception {
    String xsd = write("order.xsd", XSD_ORDER);
    String value = write("value.txt", "5");
    String withEntity =
        "<?xml version='1.0'?><!DOCTYPE order [<!ENTITY v SYSTEM '"
            + Path.of(value).toUri()
            + "'>]><order><id>&v;</id></order>";

    XsdValidatorMeta secure = meta(false);
    secure.setXsdSource(XsdValidatorMeta.SPECIFY_FILENAME);
    secure.setXsdFilename(xsd);
    Run refused = new Run(secure, xmlOnly(), rows(withEntity, VALID_ORDER, withEntity));
    refused.all();
    assertEquals(List.of(false, true, false), refused.results());
    assertNotNull(refused.messages().get(0));
    assertEquals(refused.messages().get(0), refused.messages().get(2));

    tearDown();
    setUp();
    XsdValidatorMeta allowed = meta(true);
    allowed.setXsdSource(XsdValidatorMeta.SPECIFY_FILENAME);
    allowed.setXsdFilename(xsd);
    Run accepted = new Run(allowed, xmlOnly(), rows(withEntity, withEntity));
    accepted.all();
    assertEquals(List.of(true, true), accepted.results());
  }

  private XsdValidatorMeta meta(boolean allowExternalEntities) {
    XsdValidatorMeta meta = new XsdValidatorMeta();
    meta.setDefault();
    meta.setXmlSourceFile(false);
    meta.setXmlStream("xml");
    meta.setAddValidationMessage(true);
    meta.setAllowExternalEntities(allowExternalEntities);
    return meta;
  }

  private String write(String name, String content) throws Exception {
    Path file = tempDir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file.toString();
  }

  private static IRowMeta xmlOnly() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("xml"));
    return rowMeta;
  }

  private static IRowMeta xmlAndXsd() {
    IRowMeta rowMeta = xmlOnly();
    rowMeta.addValueMeta(new ValueMetaString("xsd"));
    return rowMeta;
  }

  private static List<Object[]> rows(String... xml) {
    List<Object[]> rows = new ArrayList<>();
    for (String value : xml) {
      rows.add(new Object[] {value});
    }
    return rows;
  }

  /** One transform copy, fed row by row; collects the result flag and message of each row. */
  private class Run {
    private final XsdValidator transform;
    private final int resultIndex;
    private final List<Boolean> results = new ArrayList<>();
    private final List<String> messages = new ArrayList<>();
    private boolean done;

    Run(XsdValidatorMeta meta, IRowMeta rowMeta, List<Object[]> rows) {
      transform =
          new XsdValidator(
              tmh.transformMeta, meta, new XsdValidatorData(), 0, tmh.pipelineMeta, tmh.pipeline);
      transform.init();
      IRowSet rowSet = tmh.getMockInputRowSet(rows);
      when(rowSet.getRowMeta()).thenReturn(rowMeta);
      transform.addRowSetToInputRowSets(rowSet);
      resultIndex = rowMeta.size();
      transform.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta outputRowMeta, Object[] row)
                throws HopTransformException {
              results.add((Boolean) row[resultIndex]);
              messages.add((String) row[resultIndex + 1]);
            }
          });
    }

    void next() throws Exception {
      done = !transform.processRow();
    }

    void all() throws Exception {
      while (!done) {
        next();
      }
      assertEquals(0, transform.getErrors());
      assertFalse(results.contains(null));
      assertTrue(results.size() == messages.size());
    }

    List<Boolean> results() {
      return results;
    }

    List<String> messages() {
      return messages;
    }
  }
}
