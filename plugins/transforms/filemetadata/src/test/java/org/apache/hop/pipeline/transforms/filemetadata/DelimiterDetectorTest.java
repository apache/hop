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

package org.apache.hop.pipeline.transforms.filemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetector;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetectorBuilder;
import org.junit.jupiter.api.Test;

class DelimiterDetectorTest {

  @Test
  void confirmsSimpleCSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                this.getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder().withDelimiterCandidates(',').withInput(f).build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void doesNotConfirmSimpleCSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(';') // that is not the correct delimiter
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNull(result);
    }
  }

  @Test
  void confirmsSimpleCSVwithEnclosure() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-enclosed.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(',')
              .withEnclosureCandidates('"')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void prefersNoEnclosureIfNotSeenSimpleCSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(',')
              .withEnclosureCandidates('"', '\'')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertNull(result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void confirmsSimpleCSVwithOptionalEnclosure() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-optionally-enclosed.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(',')
              .withEnclosureCandidates('"')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsSimpleCSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsExcelExportCSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/excel-export.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withEnclosureCandidates('\'', '"')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(';', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertEquals(28, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(31, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsSimpleCSVWithEnclosure() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-enclosed.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withEnclosureCandidates('\'', '"')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsSimpleCSVWithOptionalEnclosure() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-optionally-enclosed.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withEnclosureCandidates('\'', '"')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsCSVWithHeaders() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-6h.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(6, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsCSVWithFooters() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-6f.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(6, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsCSVWithHeadersAndFooters() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-2h-3f.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsCSVWithHeadersAndFootersAndEnclosure() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/simple-2h-3f-enclosed.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withEnclosureCandidates('"', '\t')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  @Test
  void detectsSimpleTSV() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/tab-separated.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates(' ', ';', '\t', ',')
              .withInput(f)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals('\t', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }
  }

  /**
   * #5609: a quote inside an enclosed field (BOB "ROBERT" SMITH) is data. Once more than 10 such
   * lines were scanned, the enclosure was dropped and every value kept its quotes.
   */
  @Test
  void keepsEnclosureWithEnclosuresInsideFields() throws Exception {

    try (BufferedReader f =
        Files.newBufferedReader(
            Paths.get(
                getClass()
                    .getResource(
                        '/'
                            + this.getClass().getPackage().getName().replace('.', '/')
                            + "/delimited/embedded-enclosure.csv")
                    .toURI()),
            Charsets.UTF_8)) {
      DelimiterDetector detector =
          new DelimiterDetectorBuilder()
              .withDelimiterCandidates('\t', ';', ',')
              .withEnclosureCandidates('"', '\'')
              .withInput(f)
              .withRowLimit(10000)
              .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals('"', (char) result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(21, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(10, result.getDataLineFrequency());
    }
  }

  /** Every such line used to be a streak of its own, a large file dropped the enclosure early. */
  @Test
  void keepsEnclosureWithManyEnclosuresInsideFields() throws Exception {
    StringBuilder csv = new StringBuilder("\"id\",\"name\",\"amount\"\n");
    for (int i = 0; i < 1000; i++) {
      csv.append('"')
          .append(i)
          .append("\",\"BOB \"ROBERT\" SMITH\",\"")
          .append(i)
          .append(".25\"\n");
    }

    DelimiterDetector.DetectionResult result = detect(csv.toString(), ',', '"');

    assertNotNull(result);
    assertEquals('"', (char) result.getEnclosure());
    assertEquals(1001, result.getDataLines());
    assertEquals(0, result.getBadFooters());
    assertEquals(2, result.getDataLineFrequency());
  }

  @Test
  void keepsEnclosureWithEscapedEnclosures() throws Exception {
    String csv =
        "\"id\",\"name\",\"note\"\n"
            + "\"1\",\"BOB \"\"ROBERT\"\" SMITH\",\"a, b\"\n"
            + "\"2\",\"\"\"quoted\"\"\",\"\"\n"
            + "\"3\",\"ends with \"\"\",\"x\"\n";

    DelimiterDetector.DetectionResult result = detect(csv, ',', '"');

    assertNotNull(result);
    assertEquals('"', (char) result.getEnclosure());
    assertTrue(result.isConsistentEnclosure());
    assertEquals(4, result.getDataLines());
    assertEquals(2, result.getDataLineFrequency());
  }

  /** Another delimiter candidate between the enclosures inside a field is data. */
  @Test
  void countsOnlyDelimitersOutsideEnclosedFields() throws Exception {
    String csv = "\"id\",\"name\"\n" + "\"1\",\"a \"b; c\" d\"\n" + "\"2\",\"e\"\n";

    DelimiterDetector.DetectionResult result = detect(csv, ';', ',', '"');

    assertNotNull(result);
    assertEquals(',', (char) result.getDelimiter());
    assertEquals('"', (char) result.getEnclosure());
    assertEquals(3, result.getDataLines());
    assertEquals(1, result.getDataLineFrequency());
  }

  private static DelimiterDetector.DetectionResult detect(String csv, char... candidates)
      throws Exception {
    // the last candidate is the enclosure
    char[] delimiters = Arrays.copyOf(candidates, candidates.length - 1);
    try (BufferedReader reader = new BufferedReader(new StringReader(csv))) {
      return new DelimiterDetectorBuilder()
          .withDelimiterCandidates(delimiters)
          .withEnclosureCandidates(candidates[candidates.length - 1])
          .withInput(reader)
          .build()
          .detectDelimiters();
    }
  }
}
