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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for PdOption configuration object. */
class PdOptionTest {

  private PdOption pdOption;

  @BeforeEach
  void setUp() {
    pdOption = new PdOption();
  }

  @Test
  void testDefaultValues() {
    // Verify all default values after construction
    assertFalse(pdOption.isValidating(), "isValidating should default to false");
    assertFalse(pdOption.isUseUrl(), "useUrl should default to false");
    assertFalse(pdOption.isUseSnippet(), "useSnippet should default to false");
    assertNull(pdOption.getEncoding(), "encoding should default to null");
    assertFalse(pdOption.isXmlSourceFile(), "isXmlSourceFile should default to false");
    assertEquals("", pdOption.getLoopXPath(), "loopXPath should default to empty string");
  }

  @Test
  void testSetValidating() {
    pdOption.setValidating(true);
    assertTrue(pdOption.isValidating());

    pdOption.setValidating(false);
    assertFalse(pdOption.isValidating());
  }

  @Test
  void testSetUseUrl() {
    pdOption.setUseUrl(true);
    assertTrue(pdOption.isUseUrl());

    pdOption.setUseUrl(false);
    assertFalse(pdOption.isUseUrl());
  }

  @Test
  void testSetUseSnippet() {
    pdOption.setUseSnippet(true);
    assertTrue(pdOption.isUseSnippet());

    pdOption.setUseSnippet(false);
    assertFalse(pdOption.isUseSnippet());
  }

  @Test
  void testSetEncoding() {
    // Setting encoding should also set isXmlSourceFile to true
    pdOption.setEncoding("UTF-8");
    assertEquals("UTF-8", pdOption.getEncoding());
    assertTrue(
        pdOption.isXmlSourceFile(), "Setting encoding should automatically set isXmlSourceFile");
  }

  @Test
  void testSetEncodingMultipleTimes() {
    // Verify that setting encoding multiple times maintains isXmlSourceFile
    pdOption.setEncoding("UTF-8");
    assertTrue(pdOption.isXmlSourceFile());

    pdOption.setEncoding("ISO-8859-1");
    assertEquals("ISO-8859-1", pdOption.getEncoding());
    assertTrue(pdOption.isXmlSourceFile());

    pdOption.setEncoding(null);
    assertNull(pdOption.getEncoding());
    assertTrue(
        pdOption.isXmlSourceFile(), "isXmlSourceFile should remain true even if encoding is null");
  }

  @Test
  void testSetLoopXPath() {
    pdOption.setLoopXPath("/root/element");
    assertEquals("/root/element", pdOption.getLoopXPath());

    pdOption.setLoopXPath("/another/path");
    assertEquals("/another/path", pdOption.getLoopXPath());

    pdOption.setLoopXPath("");
    assertEquals("", pdOption.getLoopXPath());
  }

  @Test
  void testCompleteConfiguration() {
    // Test setting all properties
    pdOption.setValidating(true);
    pdOption.setUseUrl(true);
    pdOption.setUseSnippet(true);
    pdOption.setEncoding("UTF-16");
    pdOption.setLoopXPath("/root/items/item");

    assertTrue(pdOption.isValidating());
    assertTrue(pdOption.isUseUrl());
    assertTrue(pdOption.isUseSnippet());
    assertEquals("UTF-16", pdOption.getEncoding());
    assertTrue(pdOption.isXmlSourceFile());
    assertEquals("/root/items/item", pdOption.getLoopXPath());
  }

  @Test
  void testEncodingImpliesSourceFile() {
    // Verify the comment: "if the encoding is not null, the source must be a file"
    assertFalse(pdOption.isXmlSourceFile(), "Initially should be false");

    pdOption.setEncoding("UTF-8");
    assertTrue(
        pdOption.isXmlSourceFile(), "Setting encoding should indicate that source is a file");
  }

  @Test
  void testCommonEncodings() {
    // Test with common encoding values
    String[] encodings = {"UTF-8", "UTF-16", "ISO-8859-1", "US-ASCII", "windows-1252"};

    for (String encoding : encodings) {
      PdOption option = new PdOption();
      option.setEncoding(encoding);
      assertEquals(encoding, option.getEncoding());
      assertTrue(option.isXmlSourceFile());
    }
  }

  @Test
  void testXPathVariations() {
    // Test various XPath expressions
    String[] xpaths = {
      "/root",
      "/root/element",
      "/root/element[@type='test']",
      "//element",
      "/root/items/item[1]",
      ""
    };

    for (String xpath : xpaths) {
      PdOption option = new PdOption();
      option.setLoopXPath(xpath);
      assertEquals(xpath, option.getLoopXPath());
    }
  }

  @Test
  void testUrlModeConfiguration() {
    // Test configuration for URL-based XML reading
    pdOption.setUseUrl(true);
    pdOption.setLoopXPath("/data/records/record");

    assertTrue(pdOption.isUseUrl());
    assertFalse(pdOption.isUseSnippet());
    assertFalse(pdOption.isXmlSourceFile());
    assertEquals("/data/records/record", pdOption.getLoopXPath());
  }

  @Test
  void testSnippetModeConfiguration() {
    // Test configuration for XML snippet reading
    pdOption.setUseSnippet(true);
    pdOption.setLoopXPath("/snippet/data");

    assertTrue(pdOption.isUseSnippet());
    assertFalse(pdOption.isUseUrl());
    assertFalse(pdOption.isXmlSourceFile());
    assertEquals("/snippet/data", pdOption.getLoopXPath());
  }

  @Test
  void testFileModeConfiguration() {
    // Test configuration for file-based XML reading with encoding
    pdOption.setEncoding("UTF-8");
    pdOption.setValidating(true);
    pdOption.setLoopXPath("/file/records");

    assertTrue(pdOption.isXmlSourceFile());
    assertTrue(pdOption.isValidating());
    assertFalse(pdOption.isUseUrl());
    assertFalse(pdOption.isUseSnippet());
    assertEquals("UTF-8", pdOption.getEncoding());
    assertEquals("/file/records", pdOption.getLoopXPath());
  }

  @Test
  void testMutuallyExclusiveModes() {
    // Although not enforced by the class, test that different modes can be independently set
    pdOption.setUseUrl(true);
    pdOption.setUseSnippet(true);
    pdOption.setEncoding("UTF-8");

    // All can be true simultaneously (validation logic might be in the calling code)
    assertTrue(pdOption.isUseUrl());
    assertTrue(pdOption.isUseSnippet());
    assertTrue(pdOption.isXmlSourceFile());
  }
}
