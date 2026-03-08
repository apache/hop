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

package org.apache.hop.pipeline.transforms.html2text;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class Html2TextMetaTest {

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(getClass().getResource("/transform.xml").toURI());
    String xml = Files.readString(path);
    Html2TextMeta meta = new Html2TextMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        Html2TextMeta.class,
        meta,
        new MemoryMetadataProvider());

    Assertions.assertEquals("html", meta.getHtmlField());
    Assertions.assertEquals("outputField", meta.getOutputField());
    Assertions.assertEquals(Html2TextMeta.SafelistType.BASIC, meta.getSafelistType());
    Assertions.assertTrue(meta.isCleanOnly());
    Assertions.assertTrue(meta.isNormalisedText());
    Assertions.assertTrue(meta.isParallelism());
  }
}
