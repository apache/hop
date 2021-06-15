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

package org.apache.hop.pipeline.transforms.blockingtransform;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Assert;
import org.junit.Test;

public class BlockingTransformMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    BlockingTransformMeta meta = new BlockingTransformMeta();
    meta.setDirectory("my-folder");
    meta.setCompressFiles(true);
    meta.setCacheSize(123456);
    meta.setPassAllRows(false);
    meta.setPrefix("blocking");

    String xml = meta.getXml();

    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    BlockingTransformMeta meta2 = new BlockingTransformMeta();
    meta2.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);

    Assert.assertEquals(meta.getDirectory(), meta2.getDirectory());
    Assert.assertEquals(meta.isCompressFiles(), meta2.isCompressFiles());
    Assert.assertEquals(meta.getCacheSize(), meta2.getCacheSize());
    Assert.assertEquals(meta.isPassAllRows(), meta2.isPassAllRows());
    Assert.assertEquals(meta.getPrefix(), meta2.getPrefix());
  }
}
