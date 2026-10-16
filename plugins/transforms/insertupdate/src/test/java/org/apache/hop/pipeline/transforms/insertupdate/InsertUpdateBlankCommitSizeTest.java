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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * A blank Commit size must fall back to 0 (no batching). Up to 2.17 a saved {@code <commit/>}
 * loaded as null; since 2.18 it loads as an empty string, which must not be parsed as an integer
 * (NumberFormatException at transform start-up).
 */
class InsertUpdateBlankCommitSizeTest {

  @Test
  void blankCommitSizeFallsBackToZero() throws Exception {
    InsertUpdateMeta meta = loadMeta("<commit/>");

    assertEquals(0, meta.getCommitSizeVar(new Variables()));
  }

  @Test
  void specifiedCommitSizeIsUsed() throws Exception {
    InsertUpdateMeta meta = loadMeta("<commit>100</commit>");

    assertEquals(100, meta.getCommitSizeVar(new Variables()));
  }

  private static InsertUpdateMeta loadMeta(String commitTag) throws Exception {
    String xml = "<transform>" + commitTag + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    InsertUpdateMeta meta = new InsertUpdateMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
