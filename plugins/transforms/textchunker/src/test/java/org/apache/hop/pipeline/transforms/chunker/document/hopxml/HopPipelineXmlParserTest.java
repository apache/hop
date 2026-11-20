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
import java.util.stream.Collectors;
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

  @Test
  void parameterDefaultsWithCredentialNamesAreRedacted() {
    String xml =
        """
        <pipeline>
          <info>
            <name>secrets</name>
            <parameters>
              <parameter>
                <name>DB_PASSWORD</name>
                <default_value>s3cr3t</default_value>
                <description>Database password</description>
              </parameter>
              <parameter>
                <name>API_TOKEN</name>
                <default_value>tok123</default_value>
              </parameter>
              <parameter>
                <name>THRESHOLD</name>
                <default_value>30</default_value>
              </parameter>
            </parameters>
          </info>
        </pipeline>
        """;

    String all = allSections(new HopPipelineXmlParser().parse(xml));

    assertFalse(all.contains("s3cr3t"), all);
    assertFalse(all.contains("tok123"), all);
    // The parameter names and ordinary defaults stay: they are what makes the chunk findable.
    assertTrue(all.contains("DB_PASSWORD"), all);
    assertTrue(all.contains("API_TOKEN"), all);
    assertTrue(all.contains("default=30"), all);
  }

  @Test
  void transformNameValueEntriesWithCredentialNamesAreRedacted() {
    String xml =
        """
        <pipeline>
          <info><name>executor</name></info>
          <transform>
            <name>Run child</name>
            <type>PipelineExecutor</type>
            <parameters>
              <variablemapping>
                <variable>API_TOKEN</variable>
                <field/>
                <input>tok123</input>
              </variablemapping>
              <variablemapping>
                <variable>REGION</variable>
                <field/>
                <input>eu-west-1</input>
              </variablemapping>
            </parameters>
          </transform>
          <transform>
            <name>Generate rows</name>
            <type>RowGenerator</type>
            <fields>
              <field>
                <name>token_count</name>
                <type>Integer</type>
                <nullif>4096</nullif>
              </field>
              <field>
                <name>password_changed_at</name>
                <type>String</type>
                <nullif>2026-01-01</nullif>
              </field>
            </fields>
          </transform>
        </pipeline>
        """;

    String all = allSections(new HopPipelineXmlParser().parse(xml));

    assertFalse(all.contains("tok123"), all);
    assertTrue(all.contains("API_TOKEN"), all);
    assertTrue(all.contains("eu-west-1"), all);
    // Field definitions are not name/value entries: a field named like a credential keeps its
    // settings.
    assertTrue(all.contains("nullif: 4096"), all);
    assertTrue(all.contains("nullif: 2026-01-01"), all);
  }

  @Test
  void variableReferencesAreKept() {
    String xml =
        """
        <pipeline>
          <info>
            <name>references</name>
            <parameters>
              <parameter>
                <name>DB_PASSWORD</name>
                <default_value>${PROD_DB_PASSWORD}</default_value>
              </parameter>
            </parameters>
          </info>
          <transform>
            <name>Run child</name>
            <type>PipelineExecutor</type>
            <parameters>
              <variablemapping>
                <variable>API_TOKEN</variable>
                <field/>
                <input>${VAULT_API_TOKEN}</input>
              </variablemapping>
            </parameters>
          </transform>
        </pipeline>
        """;

    String all = allSections(new HopPipelineXmlParser().parse(xml));

    assertTrue(all.contains("default=${PROD_DB_PASSWORD}"), all);
    assertTrue(all.contains("input: ${VAULT_API_TOKEN}"), all);
  }

  private static String allSections(DocumentNode root) {
    return root.flattenSections().stream()
        .map(section -> section.breadcrumb() + " " + section.getBody())
        .collect(Collectors.joining("\n"));
  }
}
