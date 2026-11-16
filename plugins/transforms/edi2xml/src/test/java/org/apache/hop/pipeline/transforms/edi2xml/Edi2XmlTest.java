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

package org.apache.hop.pipeline.transforms.edi2xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The EDI to XML transform, row by row. */
class Edi2XmlTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<Edi2XmlMeta, Edi2XmlData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("EDI to XML", Edi2XmlMeta.class, Edi2XmlData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.transformMeta.isDoingErrorHandling()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  private Edi2Xml transform(String outputField, BlockingRowSet output, Object[]... rows) {
    Edi2XmlMeta meta = new Edi2XmlMeta();
    meta.setInputField("edi");
    meta.setOutputField(outputField);
    when(mockHelper.transformMeta.getTransform()).thenReturn(meta);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("edi"));
    BlockingRowSet input = new BlockingRowSet(rows.length + 1);
    input.setThreadNameFromToCopy("previous", 0, "EDI to XML", 0);
    input.setRowMeta(rowMeta);
    for (Object[] row : rows) {
      input.putRow(rowMeta, row);
    }
    input.setDone();
    output.setThreadNameFromToCopy("EDI to XML", 0, "next", 0);

    Edi2Xml transform =
        new Edi2Xml(
            mockHelper.transformMeta,
            meta,
            new Edi2XmlData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform.addRowSetToInputRowSets(input);
    transform.addRowSetToOutputRowSets(output);
    return transform;
  }

  @Test
  void everyRowGetsTheXmlOfItsOwnMessageInTheOutputField() throws Exception {
    BlockingRowSet output = new BlockingRowSet(10);
    Edi2Xml transform =
        transform(
            "xml",
            output,
            new Object[] {1L, "UNH+1+ACME & Sons'"},
            new Object[] {2L, "BGM+220+O?'Brien'"},
            new Object[] {3L, null});
    assertTrue(transform.init());

    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    Object[] first = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(1L, first[0]);
    assertEquals("UNH+1+ACME & Sons'", first[1]);
    assertTrue(((String) first[2]).contains("<value>ACME &amp; Sons</value>"), (String) first[2]);

    Object[] second = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(2L, second[0]);
    String secondXml = (String) second[2];
    assertTrue(secondXml.contains("<BGM>"), secondXml);
    assertTrue(secondXml.contains("<value>O&apos;Brien</value>"), secondXml);
    // nothing of the first message is left in the second
    assertFalse(secondXml.contains("UNH"), secondXml);

    // an empty input is an empty document, not an error
    Object[] third = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(3L, third[0]);
    assertFalse(((String) third[2]).contains("<value>"));
    assertNull(output.getRowWait(100, TimeUnit.MILLISECONDS));
  }

  @Test
  void withoutAnOutputFieldTheXmlReplacesTheInput() throws Exception {
    BlockingRowSet output = new BlockingRowSet(10);
    Edi2Xml transform = transform("", output, new Object[] {1L, "UNH+1+X'"});
    assertTrue(transform.init());

    assertTrue(transform.processRow());

    Object[] row = output.getRowWait(1, TimeUnit.SECONDS);
    assertEquals(1L, row[0]);
    assertTrue(((String) row[1]).contains("<value>X</value>"), (String) row[1]);
  }

  @Test
  void aMessageThatIsNotEdifactFailsTheTransform() {
    BlockingRowSet output = new BlockingRowSet(10);
    Edi2Xml transform = transform("xml", output, new Object[] {1L, "this is not edifact"});
    assertTrue(transform.init());

    assertThrows(HopException.class, transform::processRow);
  }
}
