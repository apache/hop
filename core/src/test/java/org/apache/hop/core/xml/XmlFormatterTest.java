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
package org.apache.hop.core.xml;

import static org.custommonkey.xmlunit.XMLAssert.assertXMLEqual;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link XmlFormatter} */
class XmlFormatterTest {

  @BeforeAll
  static void setupClass() {
    XMLUnit.setIgnoreWhitespace(true);
  }

  @Test
  void test1() throws Exception {
    String inXml, expectedXml;
    try (InputStream in = XmlFormatterTest.class.getResourceAsStream("XMLFormatterIn1.xml")) {
      assertNotNull(in);
      inXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    try (InputStream in = XmlFormatterTest.class.getResourceAsStream("XMLFormatterExpected1.xml")) {
      assertNotNull(in);
      expectedXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }

    String result = XmlFormatter.format(inXml);
    assertXMLEqual(expectedXml, result);
  }

  @Test
  void test2() throws Exception {
    String inXml, expectedXml;
    try (InputStream in = XmlFormatterTest.class.getResourceAsStream("XMLFormatterExpected2.xml")) {
      assertNotNull(in);
      expectedXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }

    try (InputStream in = XmlFormatterTest.class.getResourceAsStream("XMLFormatterIn2.xml")) {
      assertNotNull(in);
      inXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }

    String result = XmlFormatter.format(inXml);
    assertXMLEqual(expectedXml, result);
  }

  @Test
  void test3() throws Exception {
    String inXml, expectedXml;
    try (InputStream in = XmlFormatterTest.class.getResourceAsStream("XMLFormatterIn3cdata.xml")) {
      assertNotNull(in);
      inXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    try (InputStream in =
        XmlFormatterTest.class.getResourceAsStream("XMLFormatterExpected3cdata.xml")) {
      assertNotNull(in);
      expectedXml = IOUtils.toString(in, StandardCharsets.UTF_8);
      assertNotNull(expectedXml);
    }

    String result = XmlFormatter.format(inXml);
    assertXMLEqual(expectedXml, result);
  }

  @Test
  void test4() throws Exception {
    String inXml, expectedXml;
    try (InputStream in =
        XmlFormatterTest.class.getResourceAsStream("XMLFormatterIn4multilinecdata.xml")) {
      assertNotNull(in);
      inXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    try (InputStream in =
        XmlFormatterTest.class.getResourceAsStream("XMLFormatterExpected4multilinecdata.xml")) {
      assertNotNull(in);
      expectedXml = IOUtils.toString(in, StandardCharsets.UTF_8);
    }

    String result = XmlFormatter.format(inXml);
    assertXMLEqual(expectedXml, result);
  }
}
