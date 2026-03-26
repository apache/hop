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
 *
 */

package org.apache.hop.pipeline.transforms.xml.xslt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class XsltMetaTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    XsltMeta meta =
        TransformSerializationTestUtil.testSerialization("/xsl-transformation.xml", XsltMeta.class);

    assertEquals("xsl-filename", meta.getXslFilename());
    assertEquals("field-name", meta.getFieldName());
    assertEquals("result", meta.getResultFieldName());
    assertEquals("xsl-filename-field", meta.getXslFileField());
    assertTrue(meta.isXslFileFieldUse());
    assertTrue(meta.isXslFieldIsAFile());
    assertEquals("JAXP", meta.getXslFactory());
    assertEquals(3, meta.getParameters().size());
    XsltMeta.Parameter p = meta.getParameters().getFirst();
    assertEquals("streamField1", p.getParameterField());
    assertEquals("parameterName1", p.getParameterName());
    p = meta.getParameters().get(1);
    assertEquals("streamField2", p.getParameterField());
    assertEquals("parameterName2", p.getParameterName());
    p = meta.getParameters().getLast();
    assertEquals("streamField3", p.getParameterField());
    assertEquals("parameterName3", p.getParameterName());

    assertEquals(3, meta.getOutputProperties().size());
    XsltMeta.OutputProperty o = meta.getOutputProperties().getFirst();
    assertEquals("method", o.getOutputPropertyName());
    assertEquals("methodValue", o.getOutputPropertyValue());
    o = meta.getOutputProperties().get(1);
    assertEquals("version", o.getOutputPropertyName());
    assertEquals("versionValue", o.getOutputPropertyValue());
    o = meta.getOutputProperties().getLast();
    assertEquals("encoding", o.getOutputPropertyName());
    assertEquals("encodingValue", o.getOutputPropertyValue());
  }
}
