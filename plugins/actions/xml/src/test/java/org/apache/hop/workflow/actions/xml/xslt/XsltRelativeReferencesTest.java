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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
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

  /** A stylesheet on a file system java.net.URL does not know resolves its includes there. */
  @ParameterizedTest
  @ValueSource(strings = {Xslt.FACTORY_JAXP, Xslt.FACTORY_SAXON})
  void includeInsideAZipFile(String factory) throws Exception {
    Path zip =
        zip(
            "stylesheets.zip",
            "titles.xsl",
            XSL_HEADER + "<xsl:include href=\"line.xsl\"/>" + TITLES_TEMPLATE,
            "line.xsl",
            LINE_TEMPLATE);

    assertEquals(
        "Empire Burlesque;", transform(factory, catalog().toString(), zipEntry(zip, "titles.xsl")));
  }

  /**
   * An absolute path in a stylesheet on another file system keeps reading the local file, as before
   * the stylesheet's location was known.
   */
  @ParameterizedTest
  @ValueSource(strings = {Xslt.FACTORY_JAXP, Xslt.FACTORY_SAXON})
  void absolutePathInAZippedStylesheetReadsTheLocalFile(String factory) throws Exception {
    Path line = Files.writeString(dir.resolve("line.xsl"), LINE_TEMPLATE);
    Path zip =
        zip(
            "stylesheets.zip",
            "titles.xsl",
            XSL_HEADER + "<xsl:include href=\"" + line.toAbsolutePath() + "\"/>" + TITLES_TEMPLATE);

    assertEquals(
        "Empire Burlesque;", transform(factory, catalog().toString(), zipEntry(zip, "titles.xsl")));
  }

  /** An include found only in the working directory still resolves there, as it always did. */
  @ParameterizedTest
  @ValueSource(strings = {Xslt.FACTORY_JAXP, Xslt.FACTORY_SAXON})
  void includeInTheWorkingDirectoryStillResolves(String factory) throws Exception {
    Path line =
        Path.of(System.getProperty("user.dir"))
            .resolve("hop-xslt-test-" + System.nanoTime() + ".xsl");
    Files.writeString(line, LINE_TEMPLATE);
    try {
      Path xsl =
          Files.writeString(
              Files.createDirectories(dir.resolve("stylesheets")).resolve("titles.xsl"),
              XSL_HEADER + "<xsl:include href=\"" + line.getFileName() + "\"/>" + TITLES_TEMPLATE);

      assertEquals("Empire Burlesque;", transform(factory, catalog().toString(), xsl.toString()));
    } finally {
      Files.deleteIfExists(line);
    }
  }

  private static final String LINE_TEMPLATE =
      XSL_HEADER
          + "<xsl:template name=\"line\"><xsl:param name=\"text\"/>"
          + "<xsl:value-of select=\"$text\"/>;</xsl:template></xsl:stylesheet>";

  private static final String TITLES_TEMPLATE =
      "<xsl:output method=\"text\"/>"
          + "<xsl:template match=\"/\"><xsl:for-each select=\"catalog/cd\">"
          + "<xsl:call-template name=\"line\">"
          + "<xsl:with-param name=\"text\" select=\"title\"/></xsl:call-template>"
          + "</xsl:for-each></xsl:template></xsl:stylesheet>";

  private Path catalog() throws Exception {
    return Files.writeString(
        dir.resolve("catalog.xml"), "<catalog><cd><title>Empire Burlesque</title></cd></catalog>");
  }

  /** Writes a zip file with the given name/content pairs. */
  private Path zip(String name, String... entries) throws Exception {
    Path zip = dir.resolve(name);
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      for (int i = 0; i < entries.length; i += 2) {
        out.putNextEntry(new ZipEntry(entries[i]));
        out.write(entries[i + 1].getBytes(StandardCharsets.UTF_8));
        out.closeEntry();
      }
    }
    return zip;
  }

  private static String zipEntry(Path zip, String entry) {
    return "zip:" + zip.toUri() + "!/" + entry;
  }

  private String transform(String factory, Path xml, Path xsl) throws Exception {
    return transform(factory, xml.toString(), xsl.toString());
  }

  private String transform(String factory, String xml, String xsl) throws Exception {
    Path output = dir.resolve("output-" + factory + ".txt");

    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    Xslt action = new Xslt("xslt");
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setXSLTFactory(factory);
    action.setXmlfilename(xml);
    action.setXslfilename(xsl);
    action.setOutputfilename(output.toString());

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult(), "the XSLT action failed");
    return Files.readString(output);
  }
}
