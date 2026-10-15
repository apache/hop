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

package org.apache.hop.workflow.actions.xml.xslt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Issue #8936: the XSLT action handed the stylesheet and the XML to the transformer without their
 * location, so relative references in them resolved against the working directory.
 */
class XsltRelativeReferencesTest {

  private static final String XSL_HEADER =
      "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">";

  @TempDir Path dir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @ParameterizedTest
  @ValueSource(strings = {Xslt.FACTORY_JAXP, Xslt.FACTORY_SAXON})
  void stylesheetIncludesAndImportsRelativeToItsFolder(String factory) throws Exception {
    Path stylesheets = Files.createDirectories(dir.resolve("stylesheets"));
    Files.writeString(
        stylesheets.resolve("separator.xsl"),
        XSL_HEADER + "<xsl:template name=\"separator\">;</xsl:template></xsl:stylesheet>");
    Files.writeString(
        stylesheets.resolve("line.xsl"),
        XSL_HEADER
            + "<xsl:template name=\"line\"><xsl:param name=\"text\"/>"
            + "<xsl:value-of select=\"$text\"/><xsl:call-template name=\"separator\"/>"
            + "</xsl:template></xsl:stylesheet>");
    Path xsl =
        Files.writeString(
            stylesheets.resolve("titles.xsl"),
            XSL_HEADER
                + "<xsl:import href=\"separator.xsl\"/>"
                + "<xsl:include href=\"line.xsl\"/>"
                + "<xsl:output method=\"text\"/>"
                + "<xsl:template match=\"/\"><xsl:for-each select=\"catalog/cd\">"
                + "<xsl:call-template name=\"line\">"
                + "<xsl:with-param name=\"text\" select=\"title\"/></xsl:call-template>"
                + "</xsl:for-each></xsl:template></xsl:stylesheet>");
    Path xml =
        Files.writeString(
            dir.resolve("catalog.xml"),
            "<catalog><cd><title>Empire Burlesque</title></cd><cd><title>Eros</title></cd>"
                + "</catalog>");

    assertEquals("Empire Burlesque;Eros;", transform(factory, xml, xsl));
  }

  @ParameterizedTest
  @ValueSource(strings = {Xslt.FACTORY_JAXP, Xslt.FACTORY_SAXON})
  void documentFunctionResolvesRelativeToTheXmlFile(String factory) throws Exception {
    Path stylesheets = Files.createDirectories(dir.resolve("stylesheets"));
    Path xsl =
        Files.writeString(
            stylesheets.resolve("titles.xsl"),
            XSL_HEADER
                + "<xsl:output method=\"text\"/>"
                + "<xsl:template match=\"/\">"
                + "<xsl:for-each select=\"catalog/cd\"><xsl:value-of select=\"title\"/>;"
                + "</xsl:for-each>"
                // A node-set argument resolves against the base URI of the XML that holds it
                + "<xsl:for-each select=\"document(catalog/more/@href)/catalog/cd\">"
                + "<xsl:value-of select=\"title\"/>;</xsl:for-each>"
                + "</xsl:template></xsl:stylesheet>");
    Path data = Files.createDirectories(dir.resolve("data"));
    Files.writeString(data.resolve("more.xml"), "<catalog><cd><title>Eros</title></cd></catalog>");
    Path xml =
        Files.writeString(
            data.resolve("catalog.xml"),
            "<catalog><more href=\"more.xml\"/><cd><title>Empire Burlesque</title></cd>"
                + "</catalog>");

    assertEquals("Empire Burlesque;Eros;", transform(factory, xml, xsl));
  }

  private String transform(String factory, Path xml, Path xsl) throws Exception {
    Path output = dir.resolve("output-" + factory + ".txt");

    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    Xslt action = new Xslt("xslt");
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setXSLTFactory(factory);
    action.setXmlfilename(xml.toString());
    action.setXslfilename(xsl.toString());
    action.setOutputfilename(output.toString());

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "the XSLT action failed");
    return Files.readString(output);
  }
}
