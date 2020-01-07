/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.ClassLoadingPluginInterface;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class TransExecutionConfigurationTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public interface MockRepositoryPlugin extends PluginInterface, ClassLoadingPluginInterface {
  }

  @Test
  public void testDefaultPassedBatchId() {
    TransExecutionConfiguration tec = new TransExecutionConfiguration();
    assertEquals( "default passedBatchId value must be null", null, tec.getPassedBatchId() );
  }

  @Test
  public void testCopy() {
    TransExecutionConfiguration tec = new TransExecutionConfiguration();

    tec.setPassedBatchId( null );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      TransExecutionConfiguration tecCopy = (TransExecutionConfiguration) tec.clone();
      assertEquals( "clone-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
    tec.setPassedBatchId( 0L );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      TransExecutionConfiguration tecCopy = (TransExecutionConfiguration) tec.clone();
      assertEquals( "clone-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
    tec.setPassedBatchId( 5L );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      TransExecutionConfiguration tecCopy = (TransExecutionConfiguration) tec.clone();
      assertEquals( "clone-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
  }

  @Test
  public void testCopyXml() throws Exception {
    TransExecutionConfiguration tec = new TransExecutionConfiguration();
    final Long passedBatchId0 = null;
    final long passedBatchId1 = 0L;
    final long passedBatchId2 = 5L;
    tec.setPassedBatchId( passedBatchId0 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = tec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, TransExecutionConfiguration.XML_TAG );
      TransExecutionConfiguration tecCopy = new TransExecutionConfiguration( node );
      assertEquals( "xml-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
    tec.setPassedBatchId( passedBatchId1 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = tec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, TransExecutionConfiguration.XML_TAG );
      TransExecutionConfiguration tecCopy = new TransExecutionConfiguration( node );
      assertEquals( "xml-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
    tec.setPassedBatchId( passedBatchId2 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = tec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, TransExecutionConfiguration.XML_TAG );
      TransExecutionConfiguration tecCopy = new TransExecutionConfiguration( node );
      assertEquals( "xml-copy", tec.getPassedBatchId(), tecCopy.getPassedBatchId() );
    }
  }
}
