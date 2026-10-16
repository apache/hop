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

package org.apache.hop.pipeline.transforms.textfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * An output field with a blank Format must keep the conversion mask of the incoming field, so a
 * date formatted upstream is written with that format. Up to 2.17 a saved {@code <format/>} loaded
 * as null; since 2.18 it loads as an empty string, which must not wipe the inherited mask.
 */
class TextFileOutputFormatInheritanceTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String INPUT_MASK = "dd-MM-yyyy";

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankFormatKeepsInputDateMask() throws Exception {
    TextFileOutputMeta meta = loadMeta(fieldXml("<format/>"));

    IRowMeta row = inputRowMeta();
    meta.getFields(row, "Text file output", null, null, new Variables(), null);

    IValueMeta date = row.searchValueMeta("date");
    assertEquals(INPUT_MASK, date.getConversionMask());
    assertEquals("17-05-2024", date.getString(may17()));
  }

  @Test
  void specifiedFormatOverridesInputDateMask() throws Exception {
    TextFileOutputMeta meta = loadMeta(fieldXml("<format>yyyy/MM/dd</format>"));

    IRowMeta row = inputRowMeta();
    meta.getFields(row, "Text file output", null, null, new Variables(), null);

    IValueMeta date = row.searchValueMeta("date");
    assertEquals("yyyy/MM/dd", date.getConversionMask());
    assertEquals("2024/05/17", date.getString(may17()));
  }

  private static String fieldXml(String formatTag) {
    return "<transform><fields><field>"
        + "<name>date</name>"
        + "<type>Date</type>"
        + formatTag
        + "<currency/><decimal/><group/><nullif/>"
        + "<trim_type>none</trim_type>"
        + "<roundingType>half_even</roundingType>"
        + "<length>-1</length><precision>-1</precision>"
        + "</field></fields></transform>";
  }

  private static TextFileOutputMeta loadMeta(String xml) throws Exception {
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }

  private static IRowMeta inputRowMeta() {
    IValueMeta date = new ValueMetaDate("date");
    date.setConversionMask(INPUT_MASK);
    IRowMeta row = new RowMeta();
    row.addValueMeta(date);
    return row;
  }

  private static Date may17() {
    return new GregorianCalendar(2024, Calendar.MAY, 17).getTime();
  }
}
