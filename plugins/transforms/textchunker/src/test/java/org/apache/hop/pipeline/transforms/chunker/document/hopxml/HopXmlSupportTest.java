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
package org.apache.hop.pipeline.transforms.chunker.document.hopxml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class HopXmlSupportTest {

  @Test
  void sanitizeEscapesLtInDoubleQuotedAttributeValues() {
    String raw = "<option property=\"filter\" value=\"score<100\"/>";
    String sanitized = HopXmlSupport.sanitizeDoubleQuotedAttributes(raw);
    assertEquals("<option property=\"filter\" value=\"score&lt;100\"/>", sanitized);
  }

  @Test
  void parseDocumentRecoversFromUnescapedLtInAttributes() {
    String xml =
        """
                <pipeline>
                  <info><name>demo</name></info>
                  <transform>
                    <name>T</name>
                    <type>Dummy</type>
                    <advancedConfig>
                      <option property="x" value="a<b"/>
                    </advancedConfig>
                  </transform>
                </pipeline>
                """;

    Document document = HopXmlSupport.parseDocument(xml);

    assertNotNull(document);
    assertEquals("pipeline", document.getDocumentElement().getTagName());
  }
}
