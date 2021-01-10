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


import com.google.common.base.Charsets;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetector;
import org.apache.hop.pipeline.transforms.filemetadata.util.delimiters.DelimiterDetectorBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;


class DelimiterDetectorTest {

  @Test
  public void confirmsSimpleCSV() throws Exception {

//    filesPath = '/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/files/";

    try(BufferedReader f = Files.newBufferedReader(Paths.get(this.getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
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
  public void doesNotConfirmSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(';') // that is not the correct delimiter
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNull(result);
    }

  }

  @Test
  public void confirmsSimpleCSVwithEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      Assertions.assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void prefersNoEnclosureIfNotSeenSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"','\'')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertNull(result.getEnclosure());
      Assertions.assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void confirmsSimpleCSVwithOptionalEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-optionally-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      Assertions.assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }


  @Test
  public void detectsSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }


  @Test
  public void detectsExcelExportCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/excel-export.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(';', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(28, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(31, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleCSVWithEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleCSVWithOptionalEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-optionally-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeaders() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-6h.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(6, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithFooters() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-6f.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(6, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeadersAndFooters() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-2h-3f.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeadersAndFootersAndEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/simple-2h-3f-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('"','\t')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleTSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource('/' + this.getClass().getPackage().getName().replace( '.', '/' ) + "/delimited/tab-separated.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
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
}
