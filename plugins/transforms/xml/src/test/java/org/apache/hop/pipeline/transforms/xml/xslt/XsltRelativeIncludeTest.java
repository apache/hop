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

package org.apache.hop.pipeline.transforms.xml.xslt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.xml.PipelineTestFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Issue #8936: a relative {@code xsl:include} or {@code xsl:import} resolved against the working
 * directory instead of the folder of the stylesheet.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class XsltRelativeIncludeTest {

  static final String XML =
      "<catalog><cd><title>Empire Burlesque</title></cd><cd><title>Eros</title></cd></catalog>";

  /** Includes {@code line.xsl} and imports {@code separator.xsl}, both next to it. */
  static final String TITLES_XSL =
      "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
          + "<xsl:import href=\"separator.xsl\"/>"
          + "<xsl:include href=\"line.xsl\"/>"
          + "<xsl:output method=\"text\"/>"
          + "<xsl:template match=\"/\">"
          + "<xsl:for-each select=\"catalog/cd\">"
          + "<xsl:call-template name=\"line\"><xsl:with-param name=\"text\" select=\"title\"/>"
          + "</xsl:call-template>"
          + "</xsl:for-each>"
          + "</xsl:template>"
          + "</xsl:stylesheet>";

  static final String LINE_XSL =
      "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
          + "<xsl:template name=\"line\"><xsl:param name=\"text\"/>"
          + "<xsl:value-of select=\"$text\"/><xsl:call-template name=\"separator\"/>"
          + "</xsl:template>"
          + "</xsl:stylesheet>";

  static final String SEPARATOR_XSL =
      "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
          + "<xsl:template name=\"separator\"><xsl:text>;</xsl:text></xsl:template>"
          + "</xsl:stylesheet>";

  static final String EXPECTED = "Empire Burlesque;Eros;";

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
  }

  /** Writes the stylesheets to a sub folder, so they are never next to the working directory. */
  static Path writeStylesheets(Path dir) throws Exception {
    Path folder = Files.createDirectories(dir.resolve("stylesheets"));
    Files.writeString(folder.resolve("separator.xsl"), SEPARATOR_XSL);
    Files.writeString(folder.resolve("line.xsl"), LINE_XSL);
    return Files.writeString(folder.resolve("titles.xsl"), TITLES_XSL);
  }

  // "JAXP" is the factory the JAXP lookup finds: Saxon in a Hop installation, which registers it in
  // lib/core. The JDK's own XSLTC is covered by the tests that force it below.

  @ParameterizedTest
  @ValueSource(strings = {"JAXP", "SAXON"})
  void stylesheetFileIncludesRelativeToItsFolder(String factory, @TempDir Path dir)
      throws Exception {
    Path titles = writeStylesheets(dir);

    XsltMeta meta = xsltMeta(factory);
    meta.setXslFilename(titles.toString());

    assertEquals(EXPECTED, transform(meta, titles.toString()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"JAXP", "SAXON"})
  void stylesheetFileFromFieldIncludesRelativeToItsFolder(String factory, @TempDir Path dir)
      throws Exception {
    Path titles = writeStylesheets(dir);

    XsltMeta meta = xsltMeta(factory);
    meta.setXslFileFieldUse(true);
    meta.setXslFieldIsAFile(true);
    meta.setXslFileField("xsl");

    assertEquals(EXPECTED, transform(meta, titles.toString()));
  }

  /** A stylesheet on a file system java.net.URL does not know resolves its includes there. */
  @ParameterizedTest
  @ValueSource(strings = {"JAXP", "SAXON"})
  void includeInsideAZipFile(String factory, @TempDir Path dir) throws Exception {
    Path zip = dir.resolve("stylesheets.zip");
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      for (String[] entry :
          new String[][] {
            {"titles.xsl", TITLES_XSL}, {"line.xsl", LINE_XSL}, {"separator.xsl", SEPARATOR_XSL}
          }) {
        out.putNextEntry(new ZipEntry(entry[0]));
        out.write(entry[1].getBytes(StandardCharsets.UTF_8));
        out.closeEntry();
      }
    }
    String titles = "zip:" + zip.toUri() + "!/titles.xsl";

    XsltMeta meta = xsltMeta(factory);
    meta.setXslFilename(titles);

    assertEquals(EXPECTED, transform(meta, titles));
  }

  /** The JDK's XSLTC refuses external stylesheets under Hop's secure factory, as it always did. */
  @Test
  void xsltcStillRefusesIncludes(@TempDir Path dir) throws Exception {
    Path titles = writeStylesheets(dir);
    XsltMeta meta = xsltMeta("JAXP");
    meta.setXslFilename(titles.toString());

    withXsltc(() -> assertThrows(HopException.class, () -> transform(meta, titles.toString())));
  }

  @Test
  void xsltcReadsAStylesheetWithoutIncludes(@TempDir Path dir) throws Exception {
    Path plain =
        Files.writeString(
            dir.resolve("plain.xsl"),
            "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
                + "<xsl:output method=\"text\"/>"
                + "<xsl:template match=\"/\"><xsl:value-of select=\"count(catalog/cd)\"/>"
                + "</xsl:template></xsl:stylesheet>");
    XsltMeta meta = xsltMeta("JAXP");
    meta.setXslFilename(plain.toString());

    withXsltc(() -> assertEquals("2", transform(meta, plain.toString())));
  }

  private static final String TRANSFORMER_FACTORY = "javax.xml.transform.TransformerFactory";

  private interface Check {
    void run() throws Exception;
  }

  private static void withXsltc(Check check) throws Exception {
    String previous = System.getProperty(TRANSFORMER_FACTORY);
    System.setProperty(
        TRANSFORMER_FACTORY, "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
    try {
      check.run();
    } finally {
      if (previous == null) {
        System.clearProperty(TRANSFORMER_FACTORY);
      } else {
        System.setProperty(TRANSFORMER_FACTORY, previous);
      }
    }
  }

  private XsltMeta xsltMeta(String factory) {
    XsltMeta meta = new XsltMeta();
    meta.setFieldName("xml");
    meta.setResultFieldName("result");
    meta.setXslFactory(factory);
    return meta;
  }

  private String transform(XsltMeta meta, String xslFilename) throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("xml"));
    rowMeta.addValueMeta(new ValueMetaString("xsl"));

    PipelineMeta pipelineMeta = PipelineTestFactory.generateTestTransformation(null, meta, "xslt");
    List<RowMetaAndData> result =
        PipelineTestFactory.executeTestTransformation(
            pipelineMeta,
            PipelineTestFactory.INJECTOR_TRANSFORMNAME,
            "xslt",
            PipelineTestFactory.DUMMY_TRANSFORMNAME,
            List.of(new RowMetaAndData(rowMeta, XML, xslFilename)));

    assertEquals(1, result.size());
    return result.get(0).getString("result", null);
  }
}
