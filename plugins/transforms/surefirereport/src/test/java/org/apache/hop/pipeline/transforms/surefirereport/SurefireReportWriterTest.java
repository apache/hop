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

package org.apache.hop.pipeline.transforms.surefirereport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.pipeline.transforms.surefirereport.SurefireTestCase.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

class SurefireReportWriterTest {

  @TempDir Path tempDir;

  @Test
  void writesEmptySuite() throws Exception {
    Path report = tempDir.resolve("surefile_empty.xml");
    SurefireReportWriter.write(report, "empty", List.of());

    String xml = Files.readString(report, StandardCharsets.UTF_8);
    assertTrue(xml.contains("name=\"empty\""));
    assertTrue(xml.contains("tests=\"0\""));
    assertTrue(xml.contains("failures=\"0\""));
    assertTrue(xml.contains("errors=\"0\""));
    assertTrue(xml.contains("skipped=\"0\""));
    assertFalse(xml.contains("<testcase"));
  }

  @Test
  void writesMixedResultsAndEscapes() throws Exception {
    Path report = tempDir.resolve("nested/surefile_mixed.xml");
    List<SurefireTestCase> cases = new ArrayList<>();
    cases.add(
        new SurefireTestCase("pass-test", 1.5, Status.PASS, "hello ]]> world", null, null, null));
    cases.add(
        new SurefireTestCase(
            "fail\"test", 2.0, Status.FAIL, "out", "err", "boom & more", "AssertionError"));
    cases.add(new SurefireTestCase("skip-test", 0.0, Status.SKIP, null, null, "not now", null));
    cases.add(new SurefireTestCase("error-test", 3.0, Status.ERROR, null, null, "oops", "Error"));

    SurefireReportWriter.write(report, "suite & name", cases);

    String xml = Files.readString(report, StandardCharsets.UTF_8);
    assertTrue(xml.contains("name=\"suite &amp; name\""));
    assertTrue(xml.contains("tests=\"4\""));
    assertTrue(xml.contains("failures=\"1\""));
    assertTrue(xml.contains("errors=\"1\""));
    assertTrue(xml.contains("skipped=\"1\""));
    assertTrue(xml.contains("name=\"pass-test\""));
    assertTrue(xml.contains("name=\"fail&quot;test\""));
    assertTrue(xml.contains("<failure type=\"AssertionError\" message=\"boom &amp; more\""));
    assertTrue(xml.contains("<error type=\"Error\" message=\"oops\""));
    assertTrue(xml.contains("<skipped message=\"not now\"/>"));
    // CDATA terminator must be split safely
    assertTrue(xml.contains("]]]]><![CDATA[>"));
    assertTrue(xml.contains("hello "));
    assertTrue(xml.contains(" world"));
  }

  @Test
  void colouredLogStaysParsableXml() throws Exception {
    Path report = tempDir.resolve("surefile_dbt.xml");
    // What dbt (and any other colourising CLI) leaves in a captured log.
    String colouredLog = "\u001b[0m15:52:04  \u001b[32mRunning with dbt=1.8.0\u001b[0m\nDone.\n";
    List<SurefireTestCase> cases = new ArrayList<>();
    cases.add(
        new SurefireTestCase(
            "main-0001-dbt",
            1.0,
            Status.FAIL,
            colouredLog,
            "\u001b[31merror\u001b[0m",
            "boom \u001b[1mhighlighted\u001b[0m",
            "AssertionError"));

    SurefireReportWriter.write(report, "dbt", cases);

    String xml = Files.readString(report, StandardCharsets.UTF_8);
    assertFalse(xml.contains("\u001b"), "no escape character may reach the report");
    // The escape sequences go whole, so their printable tail does not litter the log either.
    assertFalse(xml.contains("[0m"), xml);
    assertTrue(xml.contains("Running with dbt=1.8.0"));
    assertTrue(xml.contains("message=\"boom highlighted\""));

    // The report has to be readable by an XML parser, that is the whole point.
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    Document doc = factory.newDocumentBuilder().parse(report.toFile());
    assertEquals(1, doc.getElementsByTagName("testcase").getLength());
  }

  @Test
  void sanitizeDropsIllegalCharactersAndKeepsTheRest() {
    assertEquals("ab", SurefireReportWriter.sanitize("a\u0000b\u0008"));
    assertEquals("a\tb\nc\rd", SurefireReportWriter.sanitize("a\tb\nc\rd"));
    assertEquals("", SurefireReportWriter.sanitize("\ud83d"));
    assertEquals("\ud83d\ude00", SurefireReportWriter.sanitize("\ud83d\ude00"));
    assertEquals("", SurefireReportWriter.sanitize(null));
  }

  @Test
  void formatTimeUsesIntegerWhenWhole() {
    assertEquals("6", SurefireReportWriter.formatTime(6.0));
    assertEquals("1.500", SurefireReportWriter.formatTime(1.5));
  }

  @Test
  void escapeAttributeHandlesSpecialChars() {
    assertEquals("&amp;&lt;&gt;&quot;&apos;", SurefireReportWriter.escapeAttribute("&<>\"'"));
  }

  @Test
  void normalizeTestNameStripsMainWorkflowFilename() {
    assertEquals(
        "0001-add-sequence",
        SurefireReportOutput.normalizeTestName("/files/transforms/main-0001-add-sequence.hwf"));
    assertEquals("simple", SurefireReportOutput.normalizeTestName("main_simple.hwf"));
    assertEquals("already-clean", SurefireReportOutput.normalizeTestName("already-clean"));
  }
}
