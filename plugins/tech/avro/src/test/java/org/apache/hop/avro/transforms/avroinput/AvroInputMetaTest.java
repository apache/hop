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

package org.apache.hop.avro.transforms.avroinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Test;
import org.w3c.dom.Node;

import static org.junit.Assert.assertEquals;

public class AvroInputMetaTest {

  @Test
  public void testGetLoadXml() throws Exception {

    AvroFileInputMeta meta = new AvroFileInputMeta();
    meta.setOutputFieldName("avro");
    meta.setDataFilenameField("filename");

    String xml = meta.getXml();
    assertEquals(
        "<data_filename_field>filename</data_filename_field>"
            + Const.CR
            + "<output_field>avro</output_field>"
            + Const.CR,
        xml);

    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + xml
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    Node transformNode = XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG);

    // Read it back...
    //
    AvroFileInputMeta verify = new AvroFileInputMeta();
    verify.loadXml(transformNode, null);

    assertEquals(meta.getOutputFieldName(), verify.getOutputFieldName());
    assertEquals(meta.getDataFilenameField(), verify.getDataFilenameField());
  }
}
