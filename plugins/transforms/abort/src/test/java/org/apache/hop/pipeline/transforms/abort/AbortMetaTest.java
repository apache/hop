/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.abort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/** AbortMeta test */
class AbortMetaTest {
  private AbortMeta meta;

  @BeforeEach
  void setUp() {
    meta = new AbortMeta();
  }

  @Test
  void testRoundTrip() throws HopException {
    List<String> attributes =
        Arrays.asList("row_threshold", "message", "always_log_rows", "abort_option");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("row_threshold", "getRowThreshold");
    getterMap.put("message", "getMessage");
    getterMap.put("always_log_rows", "isAlwaysLogRows");
    getterMap.put("abort_option", "getAbortOption");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("row_threshold", "setRowThreshold");
    setterMap.put("message", "setMessage");
    setterMap.put("always_log_rows", "setAlwaysLogRows");
    setterMap.put("abort_option", "setAbortOption");

    Map<String, IFieldLoadSaveValidator<?>> attributeValidators = Collections.emptyMap();

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();
    typeValidators.put(
        AbortMeta.AbortOption.class.getCanonicalName(),
        new EnumLoadSaveValidator<>(AbortMeta.AbortOption.ABORT));

    LoadSaveTester<AbortMeta> loadSaveTester =
        new LoadSaveTester<>(
            AbortMeta.class, attributes, getterMap, setterMap, attributeValidators, typeValidators);
    loadSaveTester.testSerialization();
  }

  @Test
  void testBackwardsCompatibilityAbortWithError() throws HopXmlException {
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);

    // No abort option specified: leave the default: Abort
    String inputXml =
        """
        <transform>
            <name>Abort</name>
            <type>Abort</type>
          </transform>\
        """;
    Node node = XmlHandler.loadXmlString(inputXml).getFirstChild();
    meta.loadXml(node, metadataProvider);
    assertTrue(meta.isAbort());
  }

  @Test
  void testDefaultValuesAfterConstruction() {
    assertEquals(AbortMeta.AbortOption.ABORT, meta.getAbortOption());
  }

  @Test
  void testSetDefaultValues() {
    meta.setDefault();

    assertEquals("0", meta.getRowThreshold());
    assertEquals("", meta.getMessage());
    assertTrue(meta.isAlwaysLogRows());
    assertEquals(AbortMeta.AbortOption.ABORT_WITH_ERROR, meta.getAbortOption());
  }

  @Test
  void testCheckWithNoInputAddsWarning() {
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);

    meta.check(remarks, pipelineMeta, transformMeta, null, new String[0], null, null, null, null);

    assertFalse(remarks.isEmpty());
    CheckResult result = (CheckResult) remarks.get(0);
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, result.getType());
  }

  @Test
  void testCheckWithInputDoesNotAddWarning() {
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    TransformMeta transformMeta = mock(TransformMeta.class);

    meta.check(
        remarks, pipelineMeta, transformMeta, null, new String[] {"input"}, null, null, null, null);

    assertTrue(remarks.isEmpty());
  }

  @Test
  void testAbortOptionHelperMethods() {
    meta.setAbortOption(AbortMeta.AbortOption.ABORT);
    assertTrue(meta.isAbort());
    assertFalse(meta.isAbortWithError());
    assertFalse(meta.isSafeStop());

    meta.setAbortOption(AbortMeta.AbortOption.ABORT_WITH_ERROR);
    assertTrue(meta.isAbortWithError());
    assertFalse(meta.isAbort());
    assertFalse(meta.isSafeStop());

    meta.setAbortOption(AbortMeta.AbortOption.SAFE_STOP);
    assertTrue(meta.isSafeStop());
  }

  @Test
  void testLoadXmlBackwardCompatibility() throws Exception {
    meta.setAbortOption(null);
    Document doc = XmlTestUtil.createDocument();
    Element transformNode = doc.createElement("transform");
    // Simulate legacy XML tag
    transformNode.appendChild(XmlTestUtil.createElement(doc, "abort_with_error", "Y"));

    meta.loadXml(transformNode, mock(IHopMetadataProvider.class));
    assertEquals(AbortMeta.AbortOption.ABORT_WITH_ERROR, meta.getAbortOption());

    // Test with value N
    meta.setAbortOption(null);
    transformNode = doc.createElement("transform");
    transformNode.appendChild(XmlTestUtil.createElement(doc, "abort_with_error", "N"));
    transformNode.appendChild(XmlTestUtil.createElement(doc, "abort_with_success", "Y"));
    meta.loadXml(transformNode, mock(IHopMetadataProvider.class));
    assertEquals(AbortMeta.AbortOption.ABORT, meta.getAbortOption());
  }

  @Test
  void testSupportsMultiCopyExecution() {
    assertFalse(meta.supportsMultiCopyExecution());
  }

  static class XmlTestUtil {

    static Document createDocument() throws Exception {
      return DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    }

    static Element createElement(Document doc, String name, String value) {
      Element element = doc.createElement(name);
      element.setTextContent(value);
      return element;
    }
  }
}
