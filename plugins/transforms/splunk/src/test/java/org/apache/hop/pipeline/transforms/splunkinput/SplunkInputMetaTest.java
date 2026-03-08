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

package org.apache.hop.pipeline.transforms.splunkinput;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SplunkInputMetaTest {

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    String xml = Files.readString(path);
    SplunkInputMeta meta = new SplunkInputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        SplunkInputMeta.class,
        meta,
        new MemoryMetadataProvider());

    Assertions.assertEquals("SomeConnection", meta.getConnectionName());
    Assertions.assertEquals("SEARCH * | HEAD 100", meta.getQuery());
    Assertions.assertEquals(3, meta.getReturnValues().size());

    ReturnValue v1 = meta.getReturnValues().get(0);
    Assertions.assertEquals("field1", v1.getName());
    Assertions.assertEquals("splunkField1", v1.getSplunkName());
    Assertions.assertEquals("String", v1.getType());
    Assertions.assertEquals(10, v1.getLength());
    Assertions.assertNull(v1.getFormat());

    ReturnValue v2 = meta.getReturnValues().get(1);
    Assertions.assertEquals("field2", v2.getName());
    Assertions.assertEquals("splunkField2", v2.getSplunkName());
    Assertions.assertEquals("Integer", v2.getType());
    Assertions.assertEquals(5, v2.getLength());
    Assertions.assertEquals("#", v2.getFormat());

    ReturnValue v3 = meta.getReturnValues().get(2);
    Assertions.assertEquals("field3", v3.getName());
    Assertions.assertEquals("splunkField3", v3.getSplunkName());
    Assertions.assertEquals("Date", v3.getType());
    Assertions.assertEquals(-1, v3.getLength());
    Assertions.assertEquals("yyyy/MM/dd", v3.getFormat());
  }
}
