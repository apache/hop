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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Assert;
import org.junit.Test;

public class AppendMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    TransformMeta headTransform = new TransformMeta("headTransform", null);
    TransformMeta tailTransform = new TransformMeta("tailTransform", null);

    AppendMeta meta = new AppendMeta();
    ITransformIOMeta ioMeta = meta.getTransformIOMeta();
    ioMeta.getInfoStreams().get(0).setTransformMeta(headTransform);
    ioMeta.getInfoStreams().get(1).setTransformMeta(tailTransform);

    String xml = meta.getXml();

    Assert.assertNotNull(xml);

    // Re-inflate from XML
    //
    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    AppendMeta meta2 = new AppendMeta();
    meta2.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);

    Assert.assertEquals(meta.getHeadTransformName(), meta2.getHeadTransformName());
    Assert.assertEquals(meta.getTailTransformName(), meta2.getTailTransformName());
    Assert.assertEquals("headTransform", meta2.getHeadTransformName());
    Assert.assertEquals("tailTransform", meta2.getTailTransformName());
  }
}
