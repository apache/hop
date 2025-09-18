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
 *
 */
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SSTableOutputMetaTest {

  @Test
  void testGetXmlKeyField() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setKeyField("some_key");
    assertTrue(
        ssTableOutputMeta.getXml().contains("<key_field>some_key</key_field>"),
        "getXml() does not cover setKeyField() ");
  }

  @Test
  void testGetXmlTableName() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setTable("someTableName");
    assertTrue(
        ssTableOutputMeta.getXml().contains("<table>someTableName</table>"),
        "getXml() does not cover setTableName() ");
  }

  @Test
  void testGetBufferSize() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setBufferSize("some_buffer_size");
    assertTrue(
        ssTableOutputMeta.getXml().contains("<buffer_size_mb>some_buffer_size</buffer_size_mb>"),
        "getXml() does not cover setBufferSize() ");
  }

  @Test
  void testGetXmlCassandraKeyspace() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setCassandraKeyspace("someCassandraKeyspace");
    assertTrue(
        ssTableOutputMeta
            .getXml()
            .contains("<cassandra_keyspace>someCassandraKeyspace</cassandra_keyspace>"),
        "getXml() does not cover setCassandraKeyspace() ");
  }

  @Test
  void testGetXmlYamlPath() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setYamlPath("some_YamlPath");
    assertTrue(
        ssTableOutputMeta.getXml().contains("<yaml_path>some_YamlPath</yaml_path>"),
        "getXml() does not cover setYamlPath() ");
  }

  @Test
  void testGetXmlDirectory() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setDirectory("someDirectory");
    assertTrue(
        ssTableOutputMeta.getXml().contains("<output_directory>someDirectory</output_directory>"),
        "getXml() does not cover setDirectory() ");
  }

  @Test
  void testGetXmlDefault() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    String xml = ssTableOutputMeta.getXml();
    assertTrue(
        xml.contains("<buffer_size_mb>16</buffer_size_mb>"), "getXml() does not cover defaults ");
    String defDirectory =
        "<output_directory>" + System.getProperty("java.io.tmpdir") + "</output_directory>";
    //  ( "<output_directory>" + System.getProperty( "java.io.tmpdir" ) + "</output_directory>"
    // ).replace( ":", "&#x3a;" )
    //    .replace( "\\", "&#x5c;" );
    // assertTrue( defDirectory,
    //    xml.contains( defDirectory ) );
    assertTrue(xml.contains(defDirectory), "getXml() does not cover defaults ");
  }
}
