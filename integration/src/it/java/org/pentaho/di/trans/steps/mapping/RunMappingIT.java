/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.trans.steps.mapping;

import junit.framework.TestCase;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.run.TimedTransRunner;

public class RunMappingIT extends TestCase {
  private static String TARGET_CONNECTION_XML =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
      + "<connection>"
      + "  <name>TARGET</name>"
      + "  <server/>"
      + "  <type>GENERIC</type>"
      + "  <access>Native</access>"
      + "  <database>&#47;test</database>"
      + "  <port/>"
      + "  <username/>"
      + "  <password>Encrypted </password>"
      + "  <servername/>"
      + "  <data_tablespace/>"
      + "  <index_tablespace/>"
      + "  <attributes>"
      + "    <attribute><code>CUSTOM_DRIVER_CLASS</code>"
      + "<attribute>org.apache.derby.jdbc.EmbeddedDriver</attribute></attribute>"
      + "    <attribute><code>CUSTOM_URL</code>"
      + "<attribute>jdbc:derby:test&#47;derbyNew;create=true</attribute></attribute>"
      + "  </attributes>" + "</connection>";

  public static DatabaseMeta getTargetDatabase() throws HopXMLException {
    return new DatabaseMeta( TARGET_CONNECTION_XML );
  }

  public void test_MAPPING_INPUT_ONLY() throws Exception {
    HopEnvironment.init();
    TimedTransRunner timedTransRunner =
      new TimedTransRunner(
        "test/org.apache.hop/trans/steps/mapping/filereader/use filereader.ktr", LogLevel.ERROR,
        getTargetDatabase(), 1000 );
    assertTrue( timedTransRunner.runEngine( true ) );

    Result newResult = timedTransRunner.getNewResult();
    assertTrue( newResult.getNrErrors() == 0 );
  }

  public void test_MAPPING_OUTPUT_ONLY() throws Exception {
    HopEnvironment.init();
    TimedTransRunner timedTransRunner =
      new TimedTransRunner(
        "test/org.apache.hop/trans/steps/mapping/filewriter/use filewriter.ktr", LogLevel.ERROR,
        getTargetDatabase(), 1000 );
    assertTrue( timedTransRunner.runEngine( true ) );

    Result newResult = timedTransRunner.getNewResult();
    assertTrue( newResult.getNrErrors() == 0 );
  }

  public void test_MAPPING_MULTI_OUTPUT() throws Exception {
    HopEnvironment.init();
    TimedTransRunner timedTransRunner =
      new TimedTransRunner(
        "test/org.apache.hop/trans/steps/mapping/multi_output/use filereader.ktr", LogLevel.ERROR,
        getTargetDatabase(), 1000 );
    assertTrue( timedTransRunner.runEngine( true ) );

    Result newResult = timedTransRunner.getNewResult();
    assertTrue( newResult.getNrErrors() == 0 );
  }
}
