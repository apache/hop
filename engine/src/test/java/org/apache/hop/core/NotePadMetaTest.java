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

package org.apache.hop.core;

import static org.junit.Assert.assertEquals;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.Test;
import org.w3c.dom.Node;

public class NotePadMetaTest {

  @Test
  public void testSerialization() throws Exception {

    NotePadMeta note = new NotePadMeta();
    note.setNote("This is a test");
    note.setWidth(1000);
    note.setHeight(500);
    note.setLocation(123, 456);
    note.setFontName("Arial");
    note.setFontSize(20);
    note.setFontBold(true);
    note.setFontItalic(true);
    note.setFontColorRed(100);
    note.setFontColorGreen(150);
    note.setFontColorBlue(200);
    note.setBackGroundColorRed(20);
    note.setBackGroundColorGreen(30);
    note.setBackGroundColorBlue(40);
    note.setBorderColorRed(250);
    note.setBorderColorGreen(251);
    note.setBorderColorBlue(252);

    String xml = note.getXml();

    Node notepadNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), NotePadMeta.XML_TAG);
    NotePadMeta copy = XmlMetadataUtil.deSerializeFromXml(notepadNode, NotePadMeta.class, null);

    assertEquals(xml, copy.getXml());
  }
}
