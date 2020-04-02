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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineExecutorMeta_GetFields_Test {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private PipelineExecutorMeta meta;

  private TransformMeta executionResult;
  private TransformMeta resultFiles;
  private TransformMeta outputRows;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setUp() {
    executionResult = mock( TransformMeta.class );
    resultFiles = mock( TransformMeta.class );
    outputRows = mock( TransformMeta.class );

    meta = new PipelineExecutorMeta();
    meta.setExecutionResultTargetTransformMeta( executionResult );
    meta.setResultFilesTargetTransformMeta( resultFiles );
    meta.setOutputRowsSourceTransformMeta( outputRows );

    meta.setExecutionTimeField( "executionTime" );
    meta.setExecutionResultField( "true" );
    meta.setExecutionNrErrorsField( "1" );

    meta.setResultFilesFileNameField( "resultFileName" );

    meta.setOutputRowsField( new String[] { "outputRow" } );
    meta.setOutputRowsType( new int[] { 0 } );
    meta.setOutputRowsLength( new int[] { 0 } );
    meta.setOutputRowsPrecision( new int[] { 0 } );

    meta = spy( meta );

    TransformMeta parent = mock( TransformMeta.class );
    doReturn( parent ).when( meta ).getParentTransformMeta();
    when( parent.getName() ).thenReturn( "parent transform" );

  }

  @Test
  public void getFieldsForExecutionResults() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( executionResult );
    verify( mock, times( 3 ) ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  @Test
  public void getFieldsForResultFiles() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( resultFiles );
    verify( mock ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  @Test
  public void getFieldsForInternalPipelineOutputRows() throws Exception {
    RowMetaInterface mock = invokeGetFieldsWith( outputRows );
    verify( mock ).addValueMeta( any( ValueMetaInterface.class ) );
  }

  private RowMetaInterface invokeGetFieldsWith( TransformMeta transformMeta ) throws Exception {
    RowMetaInterface rowMeta = mock( RowMetaInterface.class );
    meta.getFields( rowMeta, "", null, transformMeta, null, null );
    return rowMeta;
  }
}
