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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class TransformErrorMetaTest {
  @BeforeEach
  void setUp() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        FakeMeta.class.getName(), TransformPluginType.class, Transform.class);
  }

  @Test
  void testGetErrorRowMeta() {
    IVariables vars = new Variables();
    vars.setVariable("VarNumberErrors", "nbrErrors");
    vars.setVariable("VarErrorDescription", "errorDescription");
    vars.setVariable("VarErrorFields", "errorFields");
    vars.setVariable("VarErrorCodes", "errorCodes");
    TransformErrorMeta testObject =
        new TransformErrorMeta(
            new TransformMeta(),
            new TransformMeta(),
            "${VarNumberErrors}",
            "${VarErrorDescription}",
            "${VarErrorFields}",
            "${VarErrorCodes}");
    // 10, "some data was bad", "factId", "BAD131" );
    IRowMeta result = testObject.getErrorRowMeta(vars);

    assertNotNull(result);
    assertEquals(4, result.size());
    assertEquals(IValueMeta.TYPE_INTEGER, result.getValueMeta(0).getType());
    assertEquals("nbrErrors", result.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta(1).getType());
    assertEquals("errorDescription", result.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta(2).getType());
    assertEquals("errorFields", result.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta(3).getType());
    assertEquals("errorCodes", result.getValueMeta(3).getName());
  }

  @Test
  void testSerialization() throws Exception {
    List<TransformMeta> transforms = new ArrayList<>();
    TransformMeta t1 = new TransformMeta("t1", new FakeMeta());
    transforms.add(t1);
    TransformMeta t2 = new TransformMeta("t1", new FakeMeta());
    transforms.add(t2);

    TransformErrorMeta meta = new TransformErrorMeta();
    meta.setSourceTransform(t1);
    meta.setTargetTransform(t2);
    meta.setEnabled(true);
    meta.setMaxErrors("400");
    meta.setMinPercentRows("100");
    meta.setMaxPercentErrors("25");
    meta.setErrorFieldsValueName("errorFields");
    meta.setErrorCodesValueName("errorCodes");
    meta.setErrorDescriptionsValueName("errorDescriptions");

    String xml = meta.getXml();
    Node node = XmlHandler.loadXmlString(xml, TransformErrorMeta.XML_ERROR_TAG);
    TransformErrorMeta copy = new TransformErrorMeta(node, transforms);

    assertEquals(meta.getSourceTransform(), copy.getSourceTransform());
    assertEquals(meta.getTargetTransform(), copy.getTargetTransform());
    assertTrue(copy.isEnabled());
    assertEquals("400", copy.getMaxErrors());
    assertEquals("100", copy.getMinPercentRows());
    assertEquals("25", copy.getMaxPercentErrors());
    assertEquals("errorFields", copy.getErrorFieldsValueName());
    assertEquals("errorCodes", copy.getErrorCodesValueName());
    assertEquals("errorDescriptions", copy.getErrorDescriptionsValueName());
  }
}
