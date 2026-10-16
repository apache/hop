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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A Date constant with a blank Format must be parsed with the default date format. Up to 2.17 a
 * saved {@code <format/>} loaded as null; since 2.18 it loads as an empty string, which must not be
 * applied as an (empty) date pattern.
 */
class ConstantBlankDateFormatTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankFormatParsesWithDefaultDateFormat() throws Exception {
    // The default SimpleDateFormat pattern is locale dependent, so derive the constant from it.
    String value = new SimpleDateFormat().format(may17());
    ConstantMeta meta = loadMeta(fieldXml(value, "<format/>"));

    List<ICheckResult> remarks = new ArrayList<>();
    RowMetaAndData row = Constant.buildRow(meta, new ConstantData(), remarks);

    assertTrue(remarks.isEmpty(), "Unexpected remarks: " + texts(remarks));
    assertEquals(may17(), row.getData()[0]);
  }

  @Test
  void specifiedFormatIsUsedToParseDate() throws Exception {
    ConstantMeta meta = loadMeta(fieldXml("2024/05/17 10:30", "<format>yyyy/MM/dd HH:mm</format>"));

    List<ICheckResult> remarks = new ArrayList<>();
    RowMetaAndData row = Constant.buildRow(meta, new ConstantData(), remarks);

    assertTrue(remarks.isEmpty(), "Unexpected remarks: " + texts(remarks));
    assertEquals(may17(), row.getData()[0]);
  }

  private static String fieldXml(String value, String formatTag) {
    return "<transform><fields><field>"
        + "<name>date</name>"
        + "<type>Date</type>"
        + formatTag
        + "<currency/><decimal/><group/>"
        + "<nullif>"
        + value
        + "</nullif>"
        + "<length>-1</length><precision>-1</precision>"
        + "<set_empty_string>N</set_empty_string>"
        + "</field></fields></transform>";
  }

  private static ConstantMeta loadMeta(String xml) throws Exception {
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    ConstantMeta meta = new ConstantMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }

  private static String texts(List<ICheckResult> remarks) {
    StringBuilder sb = new StringBuilder();
    for (ICheckResult remark : remarks) {
      sb.append(remark.getText()).append("; ");
    }
    return sb.toString();
  }

  private static Date may17() {
    return new GregorianCalendar(2024, Calendar.MAY, 17, 10, 30).getTime();
  }
}
