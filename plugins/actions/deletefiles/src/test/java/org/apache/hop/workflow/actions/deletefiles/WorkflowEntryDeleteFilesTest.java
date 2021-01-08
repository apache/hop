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

package org.apache.hop.workflow.actions.deletefiles;

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WorkflowEntryDeleteFilesTest {
  private final String PATH_TO_FILE = "path/to/file";
  private final String STRING_SPACES_ONLY = "   ";

  private ActionDeleteFiles action;

  @Before
  public void setUp() throws Exception {
    action = new ActionDeleteFiles();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = mock( Workflow.class );
    doReturn( false ).when( parentWorkflow ).isStopped();

    action.setParentWorkflow( parentWorkflow );
    WorkflowMeta mockWorkflowMeta = mock( WorkflowMeta.class );
    action.setParentWorkflowMeta( mockWorkflowMeta );
    action = spy( action );
    doReturn( true ).when( action ).processFile( anyString(), anyString(), eq( parentWorkflow ) );
  }

  @Test
  public void filesWithNoPath_AreNotProcessed_ArgsOfCurrentJob() throws Exception {
    action.setArguments( new String[] { Const.EMPTY_STRING, STRING_SPACES_ONLY } );
    action.setFilemasks( new String[] { null, null } );
    action.setArgFromPrevious( false );

    action.execute( new Result(), 0 );
    verify( action, never() ).processFile( anyString(), anyString(), any( Workflow.class ) );
  }


  @Test
  public void filesWithPath_AreProcessed_ArgsOfCurrentJob() throws Exception {
    String[] args = new String[] { PATH_TO_FILE };
    action.setArguments( args );
    action.setFilemasks( new String[] { null, null } );
    action.setArgFromPrevious( false );

    action.execute( new Result(), 0 );
    verify( action, times( args.length ) ).processFile( anyString(), anyString(), any( Workflow.class ) );
  }


  @Test
  public void filesWithNoPath_AreNotProcessed_ArgsOfPreviousMeta() throws Exception {
    action.setArgFromPrevious( true );

    Result prevMetaResult = new Result();
    List<RowMetaAndData> metaAndDataList = new ArrayList<>();

    metaAndDataList.add( constructRowMetaAndData( Const.EMPTY_STRING, null ) );
    metaAndDataList.add( constructRowMetaAndData( STRING_SPACES_ONLY, null ) );

    prevMetaResult.setRows( metaAndDataList );

    action.execute( prevMetaResult, 0 );
    verify( action, never() ).processFile( anyString(), anyString(), any( Workflow.class ) );
  }

  @Test
  public void filesPath_AreProcessed_ArgsOfPreviousMeta() throws Exception {
    action.setArgFromPrevious( true );

    Result prevMetaResult = new Result();
    List<RowMetaAndData> metaAndDataList = new ArrayList<>();

    metaAndDataList.add( constructRowMetaAndData( PATH_TO_FILE, null ) );
    prevMetaResult.setRows( metaAndDataList );

    action.execute( prevMetaResult, 0 );
    verify( action, times( metaAndDataList.size() ) ).processFile( anyString(), anyString(), any( Workflow.class ) );
  }

  @Test
  public void filesPathVariables_AreProcessed_OnlyIfValueIsNotBlank() throws Exception {
    final String pathToFileBlankValue = "pathToFileBlankValue";
    final String pathToFileValidValue = "pathToFileValidValue";

    action.setVariable( pathToFileBlankValue, Const.EMPTY_STRING );
    action.setVariable( pathToFileValidValue, PATH_TO_FILE );

    action.setArguments( new String[] { asVariable( pathToFileBlankValue ), asVariable( pathToFileValidValue ) } );
    action.setFilemasks( new String[] { null, null } );
    action.setArgFromPrevious( false );

    action.execute( new Result(), 0 );

    verify( action ).processFile( eq( PATH_TO_FILE ), anyString(), any( Workflow.class ) );
  }

  @Test
  public void specifyingTheSamePath_WithDifferentWildcards() throws Exception {
    final String fileExtensionTxt = ".txt";
    final String fileExtensionXml = ".xml";

    String[] args = new String[] { PATH_TO_FILE, PATH_TO_FILE };
    action.setArguments( args );
    action.setFilemasks( new String[] { fileExtensionTxt, fileExtensionXml } );
    action.setArgFromPrevious( false );

    action.execute( new Result(), 0 );

    verify( action ).processFile( eq( PATH_TO_FILE ), eq( fileExtensionTxt ), any( Workflow.class ) );
    verify( action ).processFile( eq( PATH_TO_FILE ), eq( fileExtensionXml ), any( Workflow.class ) );
  }

  private RowMetaAndData constructRowMetaAndData( Object... data ) {
    RowMeta meta = new RowMeta();
    meta.addValueMeta( new ValueMetaString( "filePath" ) );
    meta.addValueMeta( new ValueMetaString( "wildcard" ) );

    return new RowMetaAndData( meta, data );
  }

  private String asVariable( String variable ) {
    return "${" + variable + "}";
  }
}
