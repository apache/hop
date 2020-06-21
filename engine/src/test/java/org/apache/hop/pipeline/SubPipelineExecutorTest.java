/*! ******************************************************************************
 *
 * Hop Data Integration
 *
 * http://www.project-hop.org
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
package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutorParameters;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class SubPipelineExecutorTest {
  @Mock private ILogChannelFactory logChannelFactory;
  @Mock private LogChannel logChannel;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() throws Exception {
    HopLogStore.setLogChannelFactory( this.logChannelFactory );
    Mockito.when( this.logChannelFactory.create( any(), any() ) ).thenReturn( this.logChannel );
  }

  @BeforeClass
  public static void init() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType( TransformPluginType.getInstance() );
    PluginRegistry.init();
  }

  @Test
  public void testRunningZeroRowsIsEmptyOptional() throws Exception {
    SubPipelineExecutor subPipelineExecutor = new SubPipelineExecutor( "sub-pipeline-name", null, null, false, null, "", null );
    Optional<Result> execute = subPipelineExecutor.execute( Collections.emptyList() );
    assertFalse( execute.isPresent() );
  }

  @Test
  @Ignore // TODO : move database connections out of .hpl files and move to metadata if needed
  public void testRunsAPipeline() throws Exception {
    PipelineMeta parentMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-parent.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    PipelineMeta subMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-sub.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    ILoggingObject loggingObject = new LoggingObject( "anything" );
    Pipeline parentPipeline = spy( new LocalPipelineEngine( parentMeta, loggingObject ) );
    SubPipelineExecutor subPipelineExecutor = new SubPipelineExecutor( "subpipelinename", parentPipeline,
      subMeta, true, new PipelineExecutorParameters(), "Group By", new PipelineRunConfiguration() ); // TODO: pass in local stub run configuration
    IRowMeta rowMeta = parentMeta.getTransformFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Hop", 1L ),
      new RowMetaAndData( rowMeta, "Hop", 2L ),
      new RowMetaAndData( rowMeta, "Hop", 3L ),
      new RowMetaAndData( rowMeta, "Hop", 4L ) );
    Optional<Result> optionalResult = subPipelineExecutor.execute( rows );
    assertEquals( 1, optionalResult.orElseThrow( AssertionError::new ).getRows().size() );
    verify( this.logChannel )
      .logBasic(
        Const.CR
          + "------------> Linenr 1------------------------------"
          + Const.CR
          + "name = Hop"
          + Const.CR
          + "sum = 10"
          + Const.CR
          + Const.CR
          + "===================="
      );

    Map<String, TransformStatus> statuses = subPipelineExecutor.getStatuses();
    assertEquals( 3, statuses.size() );
    List<TransformStatus> statusList = new ArrayList<>( statuses.values() );
    assertEquals( "Get rows from result", statusList.get( 0 ).getTransformName() );
    assertEquals( "Group by", statusList.get( 1 ).getTransformName() );
    assertEquals( "Write to log", statusList.get( 2 ).getTransformName() );
    for ( Map.Entry<String, TransformStatus> entry : statuses.entrySet() ) {
      TransformStatus statusSpy = spy( entry.getValue() );
      statuses.put( entry.getKey(), statusSpy );
    }

    subPipelineExecutor.execute( rows );
    for ( Map.Entry<String, TransformStatus> entry : statuses.entrySet() ) {
      verify( entry.getValue() ).updateAll( any() );
    }

    verify( parentPipeline, atLeastOnce() ).addActiveSubPipeline( eq( "sub-pipeline-name" ), any( Pipeline.class ) );
  }

  @Test
  @Ignore // TODO : move database connections out of .hpl files and move to metadata if needed
  public void stopsAll() throws HopException {
    PipelineMeta parentMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-parent.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    PipelineMeta subMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-sub.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    ILoggingObject loggingObject = new LoggingObject( "anything" );
    IPipelineEngine<PipelineMeta> parentPipeline = new LocalPipelineEngine( parentMeta, loggingObject );
    SubPipelineExecutor subPipelineExecutor = new SubPipelineExecutor( "sub-pipeline-name", parentPipeline,
      subMeta, true, new PipelineExecutorParameters(), "", new PipelineRunConfiguration(  ) ); // TODO pass run cfg
    subPipelineExecutor.setRunning( Mockito.spy( subPipelineExecutor.getRunning() ));
    IRowMeta rowMeta = parentMeta.getTransformFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Hop", 1L ),
      new RowMetaAndData( rowMeta, "Hop", 2L ),
      new RowMetaAndData( rowMeta, "Hop", 3L ),
      new RowMetaAndData( rowMeta, "Hop", 4L ) );
    subPipelineExecutor.execute( rows );
    verify( subPipelineExecutor.getRunning() ).add( any() );
    subPipelineExecutor.stop();
    assertTrue( subPipelineExecutor.getRunning().isEmpty() );
  }

  @Test
  @Ignore // TODO : move database connections out of .hpl files and move to metadata if needed
  public void doesNotExecuteWhenStopped() throws HopException {

    PipelineMeta parentMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-parent.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    PipelineMeta subMeta =
      new PipelineMeta( this.getClass().getResource( "subpipeline-executor-sub.hpl" ).getPath(), new MemoryMetadataProvider(), true, new Variables() );
    ILoggingObject loggingObject = new LoggingObject( "anything" );
    IPipelineEngine<PipelineMeta> parentPipeline = new LocalPipelineEngine( parentMeta, loggingObject );
    SubPipelineExecutor subPipelineExecutor = new SubPipelineExecutor( "sub-pipeline-name", parentPipeline,
      subMeta, true, new PipelineExecutorParameters(), "", new PipelineRunConfiguration(  ) ); // TODO pass in run cfg
    IRowMeta rowMeta = parentMeta.getTransformFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Hop", 1L ),
      new RowMetaAndData( rowMeta, "Hop", 2L ),
      new RowMetaAndData( rowMeta, "Hop", 3L ),
      new RowMetaAndData( rowMeta, "Hop", 4L ) );
    subPipelineExecutor.stop();
    subPipelineExecutor.execute( rows );

    verify( this.logChannel, never() )
      .logBasic(
        "\n"
          + "------------> Linenr 1------------------------------\n"
          + "name = Hop\n"
          + "sum = 10\n"
          + "\n"
          + "===================="
      );
  }
}
