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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.xml.XmlHandler;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class ResultTest {

  @Test
  void getXmlHandlesLightCloneWithoutRows() throws Exception {
    Result result = new Result();
    result.setNrErrors(2);
    result.setLogText("log text");

    String xml = result.lightClone().getXml();
    Node node = XmlHandler.loadXmlString(xml, Result.XML_TAG);
    Result copy = new Result(node);

    assertEquals(2, copy.getNrErrors());
    assertEquals("log text", copy.getLogText());
    assertEquals(0, copy.getRows().size());
    assertEquals(0, copy.getResultFiles().size());
  }
}
