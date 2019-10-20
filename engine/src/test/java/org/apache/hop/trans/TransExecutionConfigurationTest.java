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
import org.apache.hop.core.plugins.RepositoryPluginType;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.RepositoriesMeta;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.RepositoryMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class TransExecutionConfigurationTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testConnectRepository() throws HopException {
    TransExecutionConfiguration transExecConf = new TransExecutionConfiguration();
    final RepositoriesMeta repositoriesMeta = mock( RepositoriesMeta.class );
    final RepositoryMeta repositoryMeta = mock( RepositoryMeta.class );
    final Repository repository = mock( Repository.class );
    final String mockRepo = "mockRepo";
    final boolean[] connectionSuccess = { false };

    Repository initialRepo = mock( Repository.class );
    transExecConf.setRepository( initialRepo );

    HopLogStore.init();

    // Create mock repository plugin
    MockRepositoryPlugin mockRepositoryPlugin = mock( MockRepositoryPlugin.class );
    when( mockRepositoryPlugin.getIds() ).thenReturn( new String[] { "mockRepo" } );
    when( mockRepositoryPlugin.matches( "mockRepo" ) ).thenReturn( true );
    when( mockRepositoryPlugin.getName() ).thenReturn( "mock-repository" );
    when( mockRepositoryPlugin.getClassMap() ).thenAnswer( new Answer<Map<Class<?>, String>>() {
      @Override
      public Map<Class<?>, String> answer( InvocationOnMock invocation ) throws Throwable {
        Map<Class<?>, String> dbMap = new HashMap<Class<?>, String>();
        dbMap.put( Repository.class, repositoryMeta.getClass().getName() );
        return dbMap;
      }
    } );
    List<PluginInterface> registeredPlugins = PluginRegistry.getInstance().getPlugins( RepositoryPluginType.class );
    for ( PluginInterface registeredPlugin : registeredPlugins ) {
      PluginRegistry.getInstance().removePlugin( RepositoryPluginType.class, registeredPlugin );
    }
    PluginRegistry.getInstance().registerPlugin( RepositoryPluginType.class, mockRepositoryPlugin );

    // Define valid connection criteria
    when( repositoriesMeta.findRepository( anyString() ) ).thenAnswer( new Answer<RepositoryMeta>() {
      @Override
      public RepositoryMeta answer( InvocationOnMock invocation ) throws Throwable {
        return mockRepo.equals( invocation.getArguments()[0] ) ? repositoryMeta : null;
      }
    } );
    when( mockRepositoryPlugin.loadClass( Repository.class ) ).thenReturn( repository );
    doAnswer( new Answer() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        if ( "username".equals( invocation.getArguments()[0] ) && "password".equals( invocation.getArguments()[1] ) ) {
          connectionSuccess[0] = true;
        } else {
          connectionSuccess[0] = false;
          throw new HopException( "Mock Repository connection failed" );
        }
        return null;
      }
    } ).when( repository ).connect( anyString(), anyString() );

    // Ignore repository not found in RepositoriesMeta
    transExecConf.connectRepository( repositoriesMeta, "notFound", "username", "password" );
    assertEquals( "Repository Changed", initialRepo, transExecConf.getRepository() );

    // Ignore failed attempt to connect
    transExecConf.connectRepository( repositoriesMeta, mockRepo, "username", "" );
    assertEquals( "Repository Changed", initialRepo, transExecConf.getRepository() );

    // Save repository if connection passes
    transExecConf.connectRepository( repositoriesMeta, mockRepo, "username", "password" );
    assertEquals( "Repository didn't change", repository, transExecConf.getRepository() );
    assertTrue( "Repository not connected", connectionSuccess[0] );
  }

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
