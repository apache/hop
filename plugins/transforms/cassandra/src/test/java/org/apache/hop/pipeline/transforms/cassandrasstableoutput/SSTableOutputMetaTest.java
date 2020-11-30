/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SSTableOutputMetaTest {

  @Test
  public void testgetXmlUseCQL3() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setUseCQL3( true );
    assertTrue( "getXml() does not cover setUseCQL3() ",
      ssTableOutputMeta.getXml().contains( "<use_cql3>Y</use_cql3>" ) );

  }

  @Test
  public void testgetXmlUseCQL2() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setUseCQL3( false );
    assertTrue( "getXml() does not cover setUseCQL3() ",
      ssTableOutputMeta.getXml().contains( "<use_cql3>N</use_cql3>" ) );
  }

  @Test
  public void testgetXmlKeyField() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setKeyField( "some_key" );
    assertTrue( "getXml() does not cover setKeyField() ",
      ssTableOutputMeta.getXml().contains( "<key_field>some_key</key_field>" ) );

  }

  @Test
  public void testgetXmlTableName() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setTableName( "someTableName" );
    assertTrue( "getXml() does not cover setTableName() ",
      ssTableOutputMeta.getXml().contains( "<table>someTableName</table>" ) );
  }

  @Test
  public void testGetBufferSize() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setBufferSize( "some_buffer_size" );
    assertTrue( "getXml() does not cover setBufferSize() ",
      ssTableOutputMeta.getXml().contains( "<buffer_size_mb>some_buffer_size</buffer_size_mb>" ) );

  }

  @Test
  public void testgetXmlCassandraKeyspace() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setCassandraKeyspace( "someCassandraKeyspace" );
    assertTrue( "getXml() does not cover setCassandraKeyspace() ",
      ssTableOutputMeta.getXml().contains( "<cassandra_keyspace>someCassandraKeyspace</cassandra_keyspace>" ) );
  }

  @Test
  public void testgetXmlYamlPath() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setYamlPath( "some_YamlPath" );
    assertTrue( "getXml() does not cover setYamlPath() ",
      ssTableOutputMeta.getXml().contains( "<yaml_path>some_YamlPath</yaml_path>" ) );

  }

  @Test
  public void testgetXmlDirectory() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    ssTableOutputMeta.setDirectory( "someDirectory" );
    assertTrue( "getXml() does not cover setDirectory() ",
      ssTableOutputMeta.getXml().contains( "<output_directory>someDirectory</output_directory>" ) );
  }

  @Test
  public void testgetXmlDefault() throws Exception {
    SSTableOutputMeta ssTableOutputMeta = new SSTableOutputMeta();
    ssTableOutputMeta.setDefault();
    String xml = ssTableOutputMeta.getXml();
    assertTrue( "getXml() does not cover defaults ",
      xml.contains( "<use_cql3>Y</use_cql3>" ) );
    assertTrue( "getXml() does not cover defaults ",
      xml.contains( "<buffer_size_mb>16</buffer_size_mb>" ) );
    String defDirectory = "<output_directory>" + System.getProperty( "java.io.tmpdir" ) + "</output_directory>";
    //  ( "<output_directory>" + System.getProperty( "java.io.tmpdir" ) + "</output_directory>" ).replace( ":", "&#x3a;" )
    //    .replace( "\\", "&#x5c;" );
    //assertTrue( defDirectory,
    //    xml.contains( defDirectory ) );
    assertTrue( "getXml() does not cover defaults ",
      xml.contains( defDirectory ) );
  }
}
