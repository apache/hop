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

package org.apache.hop.www;

import org.apache.hop.cluster.HttpUtil;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SlaveServerJobStatusTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testStaticFinal() {
    assertEquals( "jobstatus", SlaveServerJobStatus.XML_TAG );
  }

  @Test
  public void testNoDate() throws HopException {
    String jobName = "testNullDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    SlaveServerJobStatus js = new SlaveServerJobStatus( jobName, id, status );
    String resultXML = js.getXML();
    Node newJobStatus = XMLHandler.getSubNode( XMLHandler.loadXMLString( resultXML ), SlaveServerJobStatus.XML_TAG );

    assertEquals( "The XML document should match after rebuilding from XML", resultXML,
      SlaveServerJobStatus.fromXML( resultXML ).getXML() );
    assertEquals( "There should be one \"log_date\" node in the XML", 1,
      XMLHandler.countNodes( newJobStatus, "log_date" ) );
    assertTrue( "The \"log_date\" node should have a null value",
      Utils.isEmpty( XMLHandler.getTagValue( newJobStatus, "log_date" ) ) );
  }

  @Test
  public void testWithDate() throws HopException {
    String jobName = "testWithDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    Date logDate = new Date();
    SlaveServerJobStatus js = new SlaveServerJobStatus( jobName, id, status );
    js.setLogDate( logDate );
    String resultXML = js.getXML();
    Node newJobStatus = XMLHandler.getSubNode( XMLHandler.loadXMLString( resultXML ), SlaveServerJobStatus.XML_TAG );

    assertEquals( "The XML document should match after rebuilding from XML", resultXML,
      SlaveServerJobStatus.fromXML( resultXML ).getXML() );
    assertEquals( "There should be one \"log_date\" node in the XML", 1,
      XMLHandler.countNodes( newJobStatus, "log_date" ) );
    assertEquals( "The \"log_date\" node should match the original value", XMLHandler.date2string( logDate ),
      XMLHandler.getTagValue( newJobStatus, "log_date" ) );
  }

  @Test
  public void testSerialization() throws HopException {
    // TODO Add Result
    List<String> attributes = Arrays.asList( "JobName", "Id", "StatusDescription", "ErrorDescription",
      "LogDate", "LoggingString", "FirstLoggingLineNr", "LastLoggingLineNr" );

    Map<String, IFieldLoadSaveValidator<?>> attributeMap = new HashMap<String, IFieldLoadSaveValidator<?>>();
    attributeMap.put( "LoggingString", new LoggingStringLoadSaveValidator() );

    SlaveServerJobStatusLoadSaveTester tester =
      new SlaveServerJobStatusLoadSaveTester( SlaveServerJobStatus.class, attributes, attributeMap );

    tester.testSerialization();
  }

  public static class LoggingStringLoadSaveValidator implements IFieldLoadSaveValidator<String> {

    @Override
    public String getTestObject() {
      try {
        return HttpUtil.encodeBase64ZippedString( UUID.randomUUID().toString() );
      } catch ( IOException e ) {
        throw new RuntimeException( e );
      }
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      try {
        return HttpUtil.decodeBase64ZippedString( testObject ).equals( actual );
      } catch ( IOException e ) {
        throw new RuntimeException( e );
      }
    }

  }
}
