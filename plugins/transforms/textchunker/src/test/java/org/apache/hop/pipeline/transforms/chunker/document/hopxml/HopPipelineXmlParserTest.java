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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.transforms.chunker.document.DocumentNode;
import org.junit.jupiter.api.Test;

class HopPipelineXmlParserTest {

  private static final String MINIMAL_PIPELINE =
      """
            <?xml version="1.0" encoding="UTF-8"?>
            <pipeline>
              <info>
                <name>filter-demo</name>
                <description>Filters rows by score</description>
                <parameters>
                  <parameter>
                    <name>THRESHOLD</name>
                    <default_value>30</default_value>
                    <description>Minimum score</description>
                  </parameter>
                </parameters>
              </info>
              <notepads>
                <notepad><note>Example pipeline for FilterRows</note></notepad>
              </notepads>
              <order>
                <hop><from>Generate</from><to>Filter</to><enabled>Y</enabled></hop>
              </order>
              <transform>
                <name>Filter</name>
                <type>FilterRows</type>
                <compare>
                  <condition>
                    <leftvalue>score</leftvalue>
                    <function>&gt;</function>
                    <value><text>30</text></value>
                  </condition>
                </compare>
              </transform>
            </pipeline>
            """;

  @Test
  void parsesPipelineIntoSections() {
    DocumentNode root = new HopPipelineXmlParser().parse(MINIMAL_PIPELINE);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    assertTrue(sections.size() >= 4);
    assertTrue(
        sections.stream()
            .anyMatch(
                s -> s.breadcrumb().contains("Overview") && s.getBody().contains("filter-demo")));
    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Notes")));
    assertTrue(
        sections.stream()
            .anyMatch(
                s ->
                    s.breadcrumb().contains("Transform: Filter")
                        && s.getBody().contains("FilterRows")));
    assertTrue(sections.stream().anyMatch(s -> s.breadcrumb().contains("Data flow")));
  }

  @Test
  void parsesPipelineWithUnescapedLtInOptionValueAttribute() {
    String xml =
        """
                <pipeline>
                  <info><name>bad-option</name></info>
                  <transform>
                    <name>Kafka Consumer</name>
                    <type>KafkaConsumer</type>
                    <advancedConfig>
                      <option property="filter" value="score<100"/>
                    </advancedConfig>
                  </transform>
                </pipeline>
                """;

    DocumentNode root = new HopPipelineXmlParser().parse(xml);
    List<DocumentNode.DocumentSection> sections = root.flattenSections();

    assertFalse(sections.isEmpty());
    assertTrue(
        sections.stream()
            .anyMatch(s -> s.getBody().contains("bad-option") || s.getBody().contains("score")),
        () ->
            sections.stream().map(s -> s.breadcrumb() + " -> " + s.getBody()).toList().toString());
  }

  @Test
  void fallsBackToPlainRootWhenXmlIsNotRecoverable() {
    String xml = "<pipeline><transform><broken";

    DocumentNode root = new HopPipelineXmlParser().parse(xml);

    assertEquals(xml, root.getBody());
    assertTrue(root.getChildren().isEmpty());
  }
}
