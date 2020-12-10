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

package org.apache.hop.www;

import org.apache.hop.server.HttpUtil;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
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

public class HopServerWorkflowStatusTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testStaticFinal() {
    assertEquals( "workflow-status", HopServerWorkflowStatus.XML_TAG );
  }

  @Test
  public void testNoDate() throws HopException {
    String workflowName = "testNullDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    HopServerWorkflowStatus js = new HopServerWorkflowStatus( workflowName, id, status );
    String resultXML = js.getXml();
    Node newJobStatus = XmlHandler.getSubNode( XmlHandler.loadXmlString( resultXML ), HopServerWorkflowStatus.XML_TAG );

    assertEquals( "The XML document should match after rebuilding from XML", resultXML,
      HopServerWorkflowStatus.fromXml( resultXML ).getXml() );
    assertEquals( "There should be one \"log_date\" node in the XML", 1,
      XmlHandler.countNodes( newJobStatus, "log_date" ) );
    assertTrue( "The \"log_date\" node should have a null value",
      Utils.isEmpty( XmlHandler.getTagValue( newJobStatus, "log_date" ) ) );
  }

  @Test
  public void testWithDate() throws HopException {
    String workflowName = "testWithDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    Date logDate = new Date();
    HopServerWorkflowStatus js = new HopServerWorkflowStatus( workflowName, id, status );
    js.setLogDate( logDate );
    String resultXML = js.getXml();
    Node newJobStatus = XmlHandler.getSubNode( XmlHandler.loadXmlString( resultXML ), HopServerWorkflowStatus.XML_TAG );

    assertEquals( "The XML document should match after rebuilding from XML", resultXML,
      HopServerWorkflowStatus.fromXml( resultXML ).getXml() );
    assertEquals( "There should be one \"log_date\" node in the XML", 1,
      XmlHandler.countNodes( newJobStatus, "log_date" ) );
    assertEquals( "The \"log_date\" node should match the original value", XmlHandler.date2string( logDate ),
      XmlHandler.getTagValue( newJobStatus, "log_date" ) );
  }

  @Test
  public void testSerialization() throws HopException {
    // TODO Add Result
    List<String> attributes = Arrays.asList( "WorkflowName", "Id", "StatusDescription", "ErrorDescription",
      "LogDate", "LoggingString", "FirstLoggingLineNr", "LastLoggingLineNr" );

    Map<String, IFieldLoadSaveValidator<?>> attributeMap = new HashMap<>();
    attributeMap.put( "LoggingString", new LoggingStringLoadSaveValidator() );

    HopServerWorkflowStatusLoadSaveTester tester =
      new HopServerWorkflowStatusLoadSaveTester( HopServerWorkflowStatus.class, attributes, attributeMap );

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
