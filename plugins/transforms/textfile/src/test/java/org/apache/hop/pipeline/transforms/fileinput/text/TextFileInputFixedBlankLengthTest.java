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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A fixed-width Text File Input with a blank Length and Encoding must split lines by characters. Up
 * to 2.17 a saved {@code <length/>} and {@code <encoding/>} loaded as null; since 2.18 they load as
 * empty strings, which must not switch to byte-based splitting with an empty encoding name.
 */
class TextFileInputFixedBlankLengthTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankLengthAndEncodingSplitByCharacters() throws Exception {
    TextFileInputMeta meta = loadMeta("<length/>", "<encoding/>");

    String[] values = convert(meta, "abc12345");

    assertArrayEquals(new String[] {"abc", "12345"}, values);
  }

  @Test
  void bytesLengthWithEncodingSplitsByBytes() throws Exception {
    TextFileInputMeta meta = loadMeta("<length>Bytes</length>", "<encoding>UTF-8</encoding>");

    String[] values = convert(meta, "abc12345");

    assertArrayEquals(new String[] {"abc", "12345"}, values);
  }

  private static String[] convert(TextFileInputMeta meta, String line) throws HopException {
    return TextFileInputUtils.convertLineToStrings(
        new LogChannel("test"), line, meta, ";", "\"", "");
  }

  private static TextFileInputMeta loadMeta(String lengthTag, String encodingTag) throws Exception {
    String xml =
        "<transform>"
            + "<file><type>Fixed</type><compression>None</compression></file>"
            + "<file_type>Fixed</file_type>"
            + "<separator>;</separator><enclosure>\"</enclosure><escapechar/>"
            + "<format>mixed</format>"
            + encodingTag
            + lengthTag
            + "<fields>"
            + field("code", 0, 3)
            + field("amount", 3, 5)
            + "</fields>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }

  private static String field(String name, int position, int length) {
    return "<field>"
        + "<name>"
        + name
        + "</name>"
        + "<type>String</type>"
        + "<format/><currency/><decimal/><group/><nullif/><ifnull/>"
        + "<position>"
        + position
        + "</position>"
        + "<length>"
        + length
        + "</length>"
        + "<precision>-1</precision>"
        + "<trim_type>none</trim_type>"
        + "<repeat>N</repeat>"
        + "</field>";
  }
}
