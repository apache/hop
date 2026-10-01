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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.junit.jupiter.api.Test;

class HopWorkflowXmlParserTest {

  private static final String MINIMAL_WORKFLOW =
      """
            <?xml version="1.0" encoding="UTF-8"?>
            <workflow>
              <name>child-wf</name>
              <description>Child workflow sample</description>
              <parameters></parameters>
              <actions>
                <action>
                  <name>Start</name>
                  <type>SPECIAL</type>
                </action>
                <action>
                  <name>Wait</name>
                  <type>DELAY</type>
                  <maximumTimeout>1</maximumTimeout>
                </action>
              </actions>
              <hops>
                <hop><from>Start</from><to>Wait</to><enabled>Y</enabled></hop>
              </hops>
            </workflow>
            """;

  @Test
  void parsesWorkflowIntoSections() {
    DocumentNode root = new HopWorkflowXmlParser().parse(MINIMAL_WORKFLOW);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    assertTrue(sections.size() >= 4);
    assertTrue(sections.stream().anyMatch(s -> s.getBody().contains("child-wf")));
    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Action: Start")));
    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Action: Wait")));
    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Control flow")));
  }
}
