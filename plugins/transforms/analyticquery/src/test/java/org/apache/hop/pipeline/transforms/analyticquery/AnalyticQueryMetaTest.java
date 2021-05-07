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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Assert;
import org.junit.Test;

public class AnalyticQueryMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    String xml =
        "<group>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<name>X</name>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "</group>"
            + Const.CR
            + "<fields>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLead1</aggregate>"
            + Const.CR
            + "<type>LEAD</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>1</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLead2</aggregate>"
            + Const.CR
            + "<type>LEAD</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>2</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLead3</aggregate>"
            + Const.CR
            + "<type>LEAD</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>2</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLead4</aggregate>"
            + Const.CR
            + "<type>LEAD</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>4</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLag1</aggregate>"
            + Const.CR
            + "<type>LAG</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>1</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLag2</aggregate>"
            + Const.CR
            + "<type>LAG</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>2</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLag3</aggregate>"
            + Const.CR
            + "<type>LAG</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>3</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<aggregate>yLag4</aggregate>"
            + Const.CR
            + "<type>LAG</type>"
            + Const.CR
            + "<subject>Y</subject>"
            + Const.CR
            + "<valuefield>4</valuefield>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "</fields>"
            + Const.CR;

    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    AnalyticQueryMeta meta = new AnalyticQueryMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG),
        AnalyticQueryMeta.class,
        meta,
        null);
    Assert.assertEquals(1, meta.getGroupFields().size());
    Assert.assertEquals(8, meta.getQueryFields().size());
    String xml2 = XmlMetadataUtil.serializeObjectToXml(meta);
    Assert.assertEquals(xml, xml2);

    AnalyticQueryMeta meta2 = new AnalyticQueryMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG),
        AnalyticQueryMeta.class,
        meta2,
        null);

    // Compare meta and meta2 to see if all serialization survived correctly...
    //
    Assert.assertEquals(1, meta2.getGroupFields().size());
    Assert.assertEquals(8, meta2.getQueryFields().size());
    for (int i = 0; i < meta.getGroupFields().size(); i++) {
      Assert.assertEquals(meta.getGroupFields().get(i), meta2.getGroupFields().get(i));
    }
    for (int i = 0; i < meta.getQueryFields().size(); i++) {
      Assert.assertEquals(meta.getQueryFields().get(i), meta2.getQueryFields().get(i));
    }
  }
}
