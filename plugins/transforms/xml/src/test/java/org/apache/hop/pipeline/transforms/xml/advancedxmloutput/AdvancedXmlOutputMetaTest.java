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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdvancedXmlOutputMetaTest {

  @BeforeEach
  void beforeEach() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    AdvancedXmlOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/advanced-xml-output.xml", AdvancedXmlOutputMeta.class);

    assertEquals("UTF-8", meta.getEncoding());
    assertEquals("/tmp/orders", meta.getFileSupport().getFileName());
    assertEquals("xml", meta.getFileSupport().getExtension());
    assertTrue(meta.getFileSupport().isAddToResultFilenames());
    assertTrue(meta.getFileSupport().isDoNotCreateEmptyFile());
    assertTrue(meta.isCreateEmptyElement());
    assertTrue(meta.isBlankLineAfterXmlDeclaration());

    XmlNode root = meta.getRootNode();
    assertNotNull(root);
    assertEquals("orders", root.getName());
    assertEquals(1, root.getChildren().size());

    XmlNode order = root.getChildren().get(0);
    assertEquals("order", order.getName());
    assertTrue(order.isGroupBy());
    assertEquals("orderId", order.getMappedField());
    assertEquals(2, order.getChildren().size());

    XmlNode id = order.getChildren().get(0);
    assertEquals("id", id.getName());
    assertEquals(XmlNode.NodeKind.Attribute, id.getKind());
    assertEquals("orderId", id.getMappedField());

    XmlNode item = order.getChildren().get(1);
    assertEquals("item", item.getName());
    assertTrue(item.isLoop());
    assertEquals(2, item.getChildren().size());

    XmlNode price = item.getChildren().get(1);
    assertEquals("price", price.getName());
    assertEquals("price", price.getMappedField());
    assertEquals("0.00", price.getFormat());
  }

  @Test
  void testCloneCreatesIndependentTree() {
    AdvancedXmlOutputMeta original = new AdvancedXmlOutputMeta();
    original.getRootNode().setName("Original");
    original.getRootNode().getChildren().get(0).setName("OriginalLoop");

    AdvancedXmlOutputMeta copy = (AdvancedXmlOutputMeta) original.clone();

    assertEquals("Original", copy.getRootNode().getName());
    assertEquals("OriginalLoop", copy.getRootNode().getChildren().get(0).getName());

    // Mutate original; copy must remain untouched.
    original.getRootNode().setName("Mutated");
    original.getRootNode().getChildren().get(0).setName("MutatedLoop");

    assertEquals("Original", copy.getRootNode().getName());
    assertEquals("OriginalLoop", copy.getRootNode().getChildren().get(0).getName());
  }

  @Test
  void testDefaultMetaValidates() {
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    java.util.List<String> errors = AdvancedXmlOutputValidator.validate(meta.getRootNode(), null);
    assertEquals(0, errors.size(), "Default tree should validate cleanly: " + errors);
  }

  @Test
  void testValidatorRejectsTreeWithoutLoop() {
    XmlNode root = new XmlNode("root", XmlNode.NodeKind.Element);
    root.addChild(new XmlNode("child", XmlNode.NodeKind.Element));
    java.util.List<String> errors = AdvancedXmlOutputValidator.validate(root, null);
    assertTrue(
        errors.stream().anyMatch(e -> e.toLowerCase().contains("loop")),
        "Expected validation error about missing loop, got: " + errors);
  }

  @Test
  void testValidatorRejectsTreeWithMultipleLoops() {
    XmlNode root = new XmlNode("root", XmlNode.NodeKind.Element);
    XmlNode a = new XmlNode("a", XmlNode.NodeKind.Element);
    a.setLoop(true);
    XmlNode b = new XmlNode("b", XmlNode.NodeKind.Element);
    b.setLoop(true);
    root.addChild(a);
    root.addChild(b);
    java.util.List<String> errors = AdvancedXmlOutputValidator.validate(root, null);
    assertTrue(
        errors.stream().anyMatch(e -> e.toLowerCase().contains("multiple loop")),
        "Expected validation error about multiple loops, got: " + errors);
  }
}
