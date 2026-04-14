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

package org.apache.hop.pipeline.transforms.xml.addxml;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AddXmlMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    AddXmlMeta meta =
        TransformSerializationTestUtil.testSerialization("/add-xml.xml", AddXmlMeta.class);

    Assertions.assertEquals(Const.UTF_8, meta.getEncoding());
    Assertions.assertEquals("xmlValueName", meta.getValueName());
    Assertions.assertEquals("Row", meta.getRootNode());
    Assertions.assertTrue(meta.getOmitDetails().isOmittingNullValues());
    Assertions.assertTrue(meta.getOmitDetails().isOmittingXmlHeader());
    validateFields(meta);
  }

  private static void validateFields(AddXmlMeta meta) {
    Assertions.assertEquals(3, meta.getOutputFields().size());
    XmlField f = meta.getOutputFields().getFirst();
    Assertions.assertEquals("f1", f.getFieldName());
    Assertions.assertEquals(IValueMeta.TYPE_STRING, f.getType());
    Assertions.assertEquals("element1", f.getElementName());
    Assertions.assertEquals("parent1", f.getAttributeParentName());
    Assertions.assertFalse(f.isAttribute());
    Assertions.assertEquals(100, f.getLength());
    Assertions.assertEquals(-1, f.getPrecision());

    f = meta.getOutputFields().get(1);
    Assertions.assertEquals("f2", f.getFieldName());
    Assertions.assertEquals(IValueMeta.TYPE_INTEGER, f.getType());
    Assertions.assertEquals("element2", f.getElementName());
    Assertions.assertEquals("parent2", f.getAttributeParentName());
    Assertions.assertEquals("#", f.getFormat());
    Assertions.assertTrue(f.isAttribute());
    Assertions.assertEquals(7, f.getLength());
    Assertions.assertEquals(0, f.getPrecision());

    f = meta.getOutputFields().getLast();
    Assertions.assertEquals("f3", f.getFieldName());
    Assertions.assertEquals(IValueMeta.TYPE_NUMBER, f.getType());
    Assertions.assertEquals("element3", f.getElementName());
    Assertions.assertEquals("parent3", f.getAttributeParentName());
    Assertions.assertEquals("#.#", f.getFormat());
    Assertions.assertTrue(f.isAttribute());
    Assertions.assertEquals(9, f.getLength());
    Assertions.assertEquals(2, f.getPrecision());
  }
}
