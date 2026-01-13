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

package org.apache.hop.pipeline.transforms.sasinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SasInputMetaTest {

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    SasInputMeta meta =
        TransformSerializationTestUtil.testSerialization("/transform.xml", SasInputMeta.class);
    Assertions.assertNotNull(meta);
    Assertions.assertEquals("filename", meta.getAcceptingField());
    Assertions.assertEquals("metaFilename", meta.getMetadataFilename());
    Assertions.assertEquals("1", meta.getLimit());
    Assertions.assertEquals(4, meta.getOutputFields().size());

    SasInputField f1 = meta.getOutputFields().get(0);
    Assertions.assertEquals("a", f1.getName());
    Assertions.assertEquals("newA", f1.getRename());
    Assertions.assertEquals(IValueMeta.TYPE_STRING, f1.getType());
    Assertions.assertNull(f1.getConversionMask());
    Assertions.assertEquals(50, f1.getLength());
    Assertions.assertEquals(-1, f1.getPrecision());
    Assertions.assertEquals(IValueMeta.TRIM_TYPE_NONE, f1.getTrimType());

    SasInputField f2 = meta.getOutputFields().get(1);
    Assertions.assertEquals("b", f2.getName());
    Assertions.assertEquals("newB", f2.getRename());
    Assertions.assertEquals(IValueMeta.TYPE_INTEGER, f2.getType());
    Assertions.assertEquals("#", f2.getConversionMask());
    Assertions.assertEquals(5, f2.getLength());
    Assertions.assertEquals(-1, f2.getPrecision());
    Assertions.assertEquals(IValueMeta.TRIM_TYPE_NONE, f2.getTrimType());

    SasInputField f3 = meta.getOutputFields().get(2);
    Assertions.assertEquals("c", f3.getName());
    Assertions.assertEquals("newC", f3.getRename());
    Assertions.assertEquals("yyyy/MM/dd HH:mm:ss", f3.getConversionMask());
    Assertions.assertEquals(IValueMeta.TYPE_DATE, f3.getType());
    Assertions.assertEquals(-1, f3.getLength());
    Assertions.assertEquals(-1, f3.getPrecision());
    Assertions.assertEquals(IValueMeta.TRIM_TYPE_NONE, f3.getTrimType());

    SasInputField f4 = meta.getOutputFields().get(3);
    Assertions.assertEquals("d", f4.getName());
    Assertions.assertEquals("newD", f4.getRename());
    Assertions.assertEquals("0,000,000.00", f4.getConversionMask());
    Assertions.assertEquals(IValueMeta.TYPE_NUMBER, f4.getType());
    Assertions.assertEquals(9, f4.getLength());
    Assertions.assertEquals(2, f4.getPrecision());
    Assertions.assertEquals(IValueMeta.TRIM_TYPE_BOTH, f4.getTrimType());
  }
}
