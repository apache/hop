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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class XmlOutputMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    XmlOutputMeta meta =
        TransformSerializationTestUtil.testSerialization("/xml-output.xml", XmlOutputMeta.class);

    Assertions.assertEquals(3, meta.getOutputFields().size());
    XmlField f = meta.getOutputFields().getFirst();
    Assertions.assertEquals("f1", f.getFieldName());
    Assertions.assertEquals("element1", f.getElementName());
    Assertions.assertEquals(IValueMeta.TYPE_STRING, f.getType());
    Assertions.assertEquals(100, f.getLength());
    Assertions.assertEquals(-1, f.getPrecision());

    f = meta.getOutputFields().get(1);
    Assertions.assertEquals("f2", f.getFieldName());
    Assertions.assertEquals("element2", f.getElementName());
    Assertions.assertEquals("#", f.getFormat());
    Assertions.assertEquals(IValueMeta.TYPE_INTEGER, f.getType());
    Assertions.assertEquals(7, f.getLength());
    Assertions.assertEquals(0, f.getPrecision());

    f = meta.getOutputFields().getLast();
    Assertions.assertEquals("f3", f.getFieldName());
    Assertions.assertEquals("element3", f.getElementName());
    Assertions.assertEquals("#.#", f.getFormat());
    Assertions.assertEquals(IValueMeta.TYPE_NUMBER, f.getType());
    Assertions.assertEquals(9, f.getLength());
    Assertions.assertEquals(2, f.getPrecision());
  }
}
