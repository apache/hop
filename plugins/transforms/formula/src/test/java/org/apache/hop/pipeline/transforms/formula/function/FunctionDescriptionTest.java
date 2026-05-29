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

package org.apache.hop.pipeline.transforms.formula.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/** Unit test for {@link FunctionDescription} */
class FunctionDescriptionTest {

  private static final String FUNCTION_XML =
      """
      <function>
        <name>ABS</name>
        <category>%Category.Mathematical</category>
        <description>Returns the absolute value of a number.</description>
        <syntax>ABS( NUMBER N )</syntax>
        <returns>Number</returns>
        <constraints></constraints>
        <semantics>If N &lt; 0, returns -N.</semantics>
        <examples>
          <example>
            <expression>ABS(2)</expression>
            <result>2</result>
            <level>1</level>
            <comment>Positive values return unchanged.</comment>
          </example>
        </examples>
      </function>
      """;

  @Test
  void constructorSetsAllFields() {
    List<FunctionExample> examples = List.of(new FunctionExample("ABS(-2)", "2", "1", "Negation"));

    FunctionDescription description =
        new FunctionDescription(
            "Math",
            "ABS",
            "Absolute value",
            "ABS(N)",
            "Number",
            "N must be numeric",
            "Returns |N|",
            examples);

    assertEquals("Math", description.getCategory());
    assertEquals("ABS", description.getName());
    assertEquals("Absolute value", description.getDescription());
    assertEquals("ABS(N)", description.getSyntax());
    assertEquals("Number", description.getReturns());
    assertEquals("N must be numeric", description.getConstraints());
    assertEquals("Returns |N|", description.getSemantics());
    assertEquals(1, description.getFunctionExamples().size());
    assertEquals("ABS(-2)", description.getFunctionExamples().getFirst().getExpression());
  }

  @Test
  void parsesFromXmlNode() throws HopXmlException {
    Node node = XmlHandler.loadXmlString(FUNCTION_XML, FunctionDescription.XML_TAG);
    FunctionDescription description = new FunctionDescription(node);

    assertEquals("ABS", description.getName());
    assertEquals("%Category.Mathematical", description.getCategory());
    assertEquals("Returns the absolute value of a number.", description.getDescription());
    assertEquals("ABS( NUMBER N )", description.getSyntax());
    assertEquals("Number", description.getReturns());
    assertEquals("If N < 0, returns -N.", description.getSemantics());
    assertEquals(1, description.getFunctionExamples().size());
    assertEquals("ABS(2)", description.getFunctionExamples().getFirst().getExpression());
  }

  @Test
  void parsesFromXmlNodeWithoutExamples() throws HopXmlException {
    String xml =
        """
        <function>
          <name>NA</name>
          <category>%Category.Information</category>
          <description>Not available.</description>
        </function>
        """;
    FunctionDescription description =
        new FunctionDescription(XmlHandler.loadXmlString(xml, FunctionDescription.XML_TAG));

    assertEquals("NA", description.getName());
    assertNotNull(description.getFunctionExamples());
    assertTrue(description.getFunctionExamples().isEmpty());
  }

  @Test
  void getHtmlReportIncludesAllSections() {
    FunctionDescription description =
        new FunctionDescription(
            "Math",
            "ABS",
            "Absolute value",
            "ABS(N)",
            "Number",
            "None",
            "Standard abs",
            List.of(new FunctionExample("ABS(-1)", "1", "1", "Sample")));

    String html = description.getHtmlReport();

    assertTrue(html.contains("<H2>ABS</H2>"));
    assertTrue(html.contains("<b><u>Description:</u></b>"));
    assertTrue(html.contains("Absolute value"));
    assertTrue(html.contains("<b><u>Syntax:</u></b>"));
    assertTrue(html.contains("ABS(N)"));
    assertTrue(html.contains("<b><u>Returns:</u></b>"));
    assertTrue(html.contains("<b><u>Constraints:</u></b>"));
    assertTrue(html.contains("<b><u>Semantics:</u></b>"));
    assertTrue(html.contains("<b><u>Examples:</u></b>"));
    assertTrue(html.contains("ABS(-1)"));
    assertTrue(html.contains("Sample"));
  }

  @Test
  void getHtmlReportOmitsEmptyOptionalSections() {
    FunctionDescription description =
        new FunctionDescription("Math", "MIN", "Minimum", "", "", "", "", Collections.emptyList());

    String html = description.getHtmlReport();

    assertTrue(html.contains("<H2>MIN</H2>"));
    assertTrue(html.contains("Minimum"));
    assertFalse(html.contains("<b><u>Syntax:</u></b>"));
    assertFalse(html.contains("<b><u>Returns:</u></b>"));
    assertFalse(html.contains("<b><u>Constraints:</u></b>"));
    assertFalse(html.contains("<b><u>Semantics:</u></b>"));
    assertFalse(html.contains("<b><u>Examples:</u></b>"));
  }

  @Test
  void getHtmlReportExampleWithoutComment() {
    FunctionDescription description =
        new FunctionDescription(
            "Math",
            "SUM",
            "Sum",
            "SUM(a,b)",
            "Number",
            "",
            "",
            List.of(new FunctionExample("SUM(1,2)", "3", "1", "")));

    String html = description.getHtmlReport();

    assertTrue(html.contains("SUM(1,2)"));
    assertTrue(html.contains("<td>3</td>"));
  }
}
