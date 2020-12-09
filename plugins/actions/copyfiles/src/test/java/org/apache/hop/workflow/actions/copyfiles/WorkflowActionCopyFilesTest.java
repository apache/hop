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
package org.apache.hop.workflow.actions.copyfiles;

import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WorkflowActionCopyFilesTest {
  private ActionCopyFiles entry;

  private final String EMPTY = "";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
  }

  @Before
  public void setUp() {
    entry = new ActionCopyFiles();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = new LocalWorkflowEngine();
    entry.setParentWorkflow( parentWorkflow );
    WorkflowMeta mockWorkflowMeta = mock( WorkflowMeta.class );
    entry.setParentWorkflowMeta( mockWorkflowMeta );
    entry = spy( entry );
  }

  @Test
  public void fileNotCopied() throws Exception {
    entry.sourceFileFolder = new String[] { EMPTY };
    entry.destinationFileFolder = new String[] { EMPTY };
    entry.wildcard = new String[] { EMPTY };

    entry.execute( new Result(), 0 );

    verify( entry, never() ).processFileFolder( anyString(), anyString(),
      anyString(), any( Workflow.class ), any( Result.class ) );
  }

  @Test
  public void fileCopied() throws Exception {
    String srcPath = "path/to/file";
    String destPath = "path/to/dir";

    entry.sourceFileFolder = new String[] { srcPath };
    entry.destinationFileFolder = new String[] { destPath };
    entry.wildcard = new String[] { EMPTY };

    Result result = entry.execute( new Result(), 0 );

    verify( entry ).processFileFolder( anyString(), anyString(),
      anyString(), any( Workflow.class ), any( Result.class ) );
    verify( entry, atLeast( 1 ) ).preprocessfilefilder( any( String[].class ) );
    assertFalse( result.getResult() );
    assertEquals( 1, result.getNrErrors() );
  }

  @Test
  public void filesCopied() throws Exception {
    String[] srcPath = new String[] { "path1", "path2", "path3" };
    String[] destPath = new String[] { "dest1", "dest2", "dest3" };

    entry.sourceFileFolder = srcPath;
    entry.destinationFileFolder = destPath;
    entry.wildcard = new String[] { EMPTY, EMPTY, EMPTY };

    Result result = entry.execute( new Result(), 0 );

    verify( entry, times( srcPath.length ) ).processFileFolder( anyString(), anyString(),
      anyString(), any( Workflow.class ), any( Result.class ) );
    assertFalse( result.getResult() );
    assertEquals( 3, result.getNrErrors() );
  }

  @Test
  public void saveLoad() throws Exception {
    String[] srcPath = new String[] { "EMPTY_SOURCE_URL-0-" };
    String[] destPath = new String[] { "EMPTY_DEST_URL-0-" };

    entry.sourceFileFolder = srcPath;
    entry.destinationFileFolder = destPath;
    entry.wildcard = new String[] { EMPTY };

    String xml = "<entry>" + entry.getXml() + "</entry>";
    assertTrue( xml.contains( srcPath[ 0 ] ) );
    assertTrue( xml.contains( destPath[ 0 ] ) );
    ActionCopyFiles loadedentry = new ActionCopyFiles();
    InputStream is = new ByteArrayInputStream( xml.getBytes() );
    loadedentry.loadXml( XmlHandler.getSubNode(
      XmlHandler.loadXmlFile( is,
        null,
        false,
        false ),
      "entry" ),
      null, new Variables() );
    assertTrue( loadedentry.destinationFileFolder[ 0 ].equals( destPath[ 0 ] ) );
    assertTrue( loadedentry.sourceFileFolder[ 0 ].equals( srcPath[ 0 ] ) );
  }
}
