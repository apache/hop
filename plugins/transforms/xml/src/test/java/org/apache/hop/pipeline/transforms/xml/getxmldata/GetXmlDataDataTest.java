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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.text.SimpleDateFormat;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataDataTest {

  private GetXmlDataData data;

  @BeforeEach
  void setUp() {
    data = new GetXmlDataData();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(data);
    assertNull(data.thisline);
    assertNull(data.nextline);
    assertNull(data.previousRow);
    assertEquals(0, data.nr_repeats);
    assertEquals(0, data.filenr);
    assertNull(data.fr);
    assertNull(data.is);
    assertEquals(-1, data.indexOfXmlField);
    assertEquals(-1, data.nrInputFields);
    assertNull(data.PathValue);
    assertEquals("@_", data.tokenStart);
    assertEquals("-", data.tokenEnd);
    assertEquals(0, data.nodenr);
    assertEquals(0, data.nodesize);
    assertNull(data.an);
    assertNull(data.readrow);
    assertEquals(0, data.totalpreviousfields);
    assertEquals("", data.prunePath);
    assertFalse(data.stopPruning);
    assertFalse(data.errorInRowButContinue);
    assertEquals(0, data.nrReadRow);
  }

  @Test
  void testNumberFormatInitialization() {
    assertNotNull(data.nf);
    assertNotNull(data.df);
    assertNotNull(data.dfs);
  }

  @Test
  void testDateFormatInitialization() {
    assertNotNull(data.daf);
    assertNotNull(data.dafs);
    assertEquals(SimpleDateFormat.class, data.daf.getClass());
  }

  @Test
  void testNamespaceMap() {
    assertNotNull(data.NAMESPACE);
    assertEquals(0, data.NAMESPACE.size());

    data.NAMESPACE.put("ns1", "http://example.com/ns1");
    assertEquals(1, data.NAMESPACE.size());
    assertEquals("http://example.com/ns1", data.NAMESPACE.get("ns1"));
  }

  @Test
  void testNamespacePathList() {
    assertNotNull(data.NSPath);
    assertEquals(0, data.NSPath.size());

    data.NSPath.add("/root/item");
    assertEquals(1, data.NSPath.size());
    assertEquals("/root/item", data.NSPath.get(0));
  }

  @Test
  void testFileInputList() {
    assertNull(data.files);

    // Can be set later during processing
    data.files = null; // Would be FileInputList in real usage
  }

  @Test
  void testRowMetaFields() {
    assertNull(data.inputRowMeta);
    assertNull(data.outputRowMeta);
    assertNull(data.convertRowMeta);
    assertNull(data.outputMeta);
  }

  @Test
  void testFileProperties() {
    assertNull(data.filename);
    assertNull(data.shortFilename);
    assertNull(data.path);
    assertNull(data.extension);
    assertFalse(data.hidden);
    assertNull(data.lastModificationDateTime);
    assertNull(data.uriName);
    assertNull(data.rootUriName);
    assertEquals(0L, data.size);
  }

  @Test
  void testDocumentAndItemTracking() {
    assertNull(data.document);
    assertNull(data.itemElement);
    assertEquals(0, data.itemCount);
    assertEquals(0, data.itemPosition);
    assertEquals(0L, data.rownr);
  }

  @Test
  void testPruningConfiguration() {
    assertEquals("", data.prunePath);
    assertFalse(data.stopPruning);

    data.prunePath = "/root/large-element";
    assertEquals("/root/large-element", data.prunePath);

    data.stopPruning = true;
    assertEquals(true, data.stopPruning);
  }

  @Test
  void testErrorHandling() {
    assertFalse(data.errorInRowButContinue);

    data.errorInRowButContinue = true;
    assertEquals(true, data.errorInRowButContinue);
  }

  @Test
  void testTokenSettings() {
    assertEquals("@_", data.tokenStart);
    assertEquals("-", data.tokenEnd);

    data.tokenStart = "@@";
    assertEquals("@@", data.tokenStart);

    data.tokenEnd = "::";
    assertEquals("::", data.tokenEnd);
  }

  @Test
  void testNodeTracking() {
    assertEquals(0, data.nodenr);
    assertEquals(0, data.nodesize);

    data.nodenr = 5;
    data.nodesize = 10;

    assertEquals(5, data.nodenr);
    assertEquals(10, data.nodesize);
  }

  @Test
  void testRowCounters() {
    assertEquals(0, data.nr_repeats);
    assertEquals(0, data.nrReadRow);

    data.nr_repeats = 3;
    data.nrReadRow = 100;

    assertEquals(3, data.nr_repeats);
    assertEquals(100, data.nrReadRow);
  }
}
