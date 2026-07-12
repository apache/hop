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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import org.apache.hop.pipeline.transforms.surefirereport.SurefireTestCase.Status;

/** Writes Maven Surefire 3.0-compatible XML reports. */
public final class SurefireReportWriter {

  private static final String XSI = "http://www.w3.org/2001/XMLSchema-instance";
  private static final String XSD =
      "https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd";

  private SurefireReportWriter() {}

  public static void write(Path reportFile, String suiteName, List<SurefireTestCase> cases)
      throws IOException {
    int tests = cases.size();
    int failures = 0;
    int errors = 0;
    int skipped = 0;
    double totalTime = 0.0;
    for (SurefireTestCase c : cases) {
      totalTime += c.getDurationSeconds();
      switch (c.getStatus()) {
        case FAIL -> failures++;
        case ERROR -> errors++;
        case SKIP -> skipped++;
        default -> {
          // PASS
        }
      }
    }

    Path parent = reportFile.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    Path tmp = reportFile.resolveSibling(reportFile.getFileName().toString() + ".tmp");
    try (BufferedWriter out =
        new BufferedWriter(
            new OutputStreamWriter(Files.newOutputStream(tmp), StandardCharsets.UTF_8))) {
      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      out.newLine();
      out.write("<testsuite xmlns:xsi=\"");
      out.write(XSI);
      out.write("\" xsi:noNamespaceSchemaLocation=\"");
      out.write(XSD);
      out.write("\" version=\"3.0\" name=\"");
      out.write(escapeAttribute(suiteName));
      out.write("\" time=\"");
      out.write(formatTime(totalTime));
      out.write("\" tests=\"");
      out.write(Integer.toString(tests));
      out.write("\" errors=\"");
      out.write(Integer.toString(errors));
      out.write("\" skipped=\"");
      out.write(Integer.toString(skipped));
      out.write("\" failures=\"");
      out.write(Integer.toString(failures));
      out.write("\">");
      out.newLine();

      for (SurefireTestCase c : cases) {
        writeTestCase(out, c);
      }

      out.write("</testsuite>");
      out.newLine();
    }

    try {
      Files.move(
          tmp, reportFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException atomicFailed) {
      Files.move(tmp, reportFile, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void writeTestCase(BufferedWriter out, SurefireTestCase c) throws IOException {
    out.write("<testcase name=\"");
    out.write(escapeAttribute(c.getName()));
    out.write("\" time=\"");
    out.write(formatTime(c.getDurationSeconds()));
    out.write("\">");
    out.newLine();

    if (c.getStatus() == Status.FAIL || c.getStatus() == Status.ERROR) {
      String element = c.getStatus() == Status.ERROR ? "error" : "failure";
      String type =
          c.getFailureType() != null && !c.getFailureType().isEmpty()
              ? c.getFailureType()
              : c.getName();
      out.write("<");
      out.write(element);
      out.write(" type=\"");
      out.write(escapeAttribute(type));
      out.write("\"");
      if (c.getFailureMessage() != null && !c.getFailureMessage().isEmpty()) {
        out.write(" message=\"");
        out.write(escapeAttribute(c.getFailureMessage()));
        out.write("\"");
      }
      out.write("></");
      out.write(element);
      out.write(">");
      out.newLine();
    } else if (c.getStatus() == Status.SKIP) {
      out.write("<skipped");
      if (c.getFailureMessage() != null && !c.getFailureMessage().isEmpty()) {
        out.write(" message=\"");
        out.write(escapeAttribute(c.getFailureMessage()));
        out.write("\"");
      }
      out.write("/>");
      out.newLine();
    }

    if (c.getSystemOut() != null && !c.getSystemOut().isEmpty()) {
      out.write("<system-out>");
      out.newLine();
      writeCdata(out, c.getSystemOut());
      out.write("</system-out>");
      out.newLine();
    }

    if (c.getSystemErr() != null && !c.getSystemErr().isEmpty()) {
      out.write("<system-err>");
      out.newLine();
      writeCdata(out, c.getSystemErr());
      out.write("</system-err>");
      out.newLine();
    }

    out.write("</testcase>");
    out.newLine();
  }

  static void writeCdata(BufferedWriter out, String text) throws IOException {
    // Split CDATA if the text contains the terminator sequence.
    String remaining = text == null ? "" : text;
    int idx;
    out.write("<![CDATA[");
    while ((idx = remaining.indexOf("]]>")) >= 0) {
      out.write(remaining.substring(0, idx));
      out.write("]]]]><![CDATA[>");
      remaining = remaining.substring(idx + 3);
    }
    out.write(remaining);
    out.write("]]>");
    out.newLine();
  }

  static String escapeAttribute(String value) {
    if (value == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(value.length() + 16);
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '&' -> sb.append("&amp;");
        case '<' -> sb.append("&lt;");
        case '>' -> sb.append("&gt;");
        case '"' -> sb.append("&quot;");
        case '\'' -> sb.append("&apos;");
        default -> {
          if (ch < 0x20 && ch != '\t' && ch != '\n' && ch != '\r') {
            // skip illegal XML 1.0 control characters
          } else {
            sb.append(ch);
          }
        }
      }
    }
    return sb.toString();
  }

  static String formatTime(double seconds) {
    // Surefire commonly uses integer seconds; keep one decimal when needed.
    if (Math.abs(seconds - Math.rint(seconds)) < 0.0005) {
      return Long.toString(Math.round(seconds));
    }
    return String.format(Locale.US, "%.3f", seconds);
  }
}
