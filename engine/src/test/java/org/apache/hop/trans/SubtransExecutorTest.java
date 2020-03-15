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

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterfaceFactory;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.trans.step.StepStatus;
import org.apache.hop.trans.steps.transexecutor.TransExecutorParameters;
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
public class SubtransExecutorTest {
  @Mock private LogChannelInterfaceFactory logChannelFactory;
  @Mock private LogChannel logChannel;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() throws Exception {
    HopLogStore.setLogChannelInterfaceFactory( this.logChannelFactory );
    Mockito.when( this.logChannelFactory.create( any(), any() ) ).thenReturn( this.logChannel );
  }

  @BeforeClass
  public static void init() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init();
    }
  }

  @Test
  public void testRunningZeroRowsIsEmptyOptional() throws Exception {
    SubtransExecutor subtransExecutor = new SubtransExecutor( "subtransname", null, null, false, null, "" );
    Optional<Result> execute = subtransExecutor.execute( Collections.emptyList() );
    assertFalse( execute.isPresent() );
  }

  @Test
  @Ignore // TODO : move database connections out of .ktr files and move to metastore if needed
  public void testRunsATrans() throws Exception {
    TransMeta parentMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-parent.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    TransMeta subMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-sub.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    LoggingObjectInterface loggingObject = new LoggingObject( "anything" );
    Trans parentTrans = spy( new Trans( parentMeta, loggingObject ) );
    SubtransExecutor subtransExecutor =
      new SubtransExecutor( "subtransname", parentTrans, subMeta, true, new TransExecutorParameters(), "Group By" );
    RowMetaInterface rowMeta = parentMeta.getStepFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Pentaho", 1L ),
      new RowMetaAndData( rowMeta, "Pentaho", 2L ),
      new RowMetaAndData( rowMeta, "Pentaho", 3L ),
      new RowMetaAndData( rowMeta, "Pentaho", 4L ) );
    Optional<Result> optionalResult = subtransExecutor.execute( rows );
    assertEquals( 1, optionalResult.orElseThrow( AssertionError::new ).getRows().size() );
    verify( this.logChannel )
      .logBasic(
        Const.CR
          + "------------> Linenr 1------------------------------"
          + Const.CR
          + "name = Pentaho"
          + Const.CR
          + "sum = 10"
          + Const.CR
          + Const.CR
          + "===================="
      );

    Map<String, StepStatus> statuses = subtransExecutor.getStatuses();
    assertEquals( 3, statuses.size() );
    List<StepStatus> statusList = new ArrayList<>( statuses.values() );
    assertEquals( "Get rows from result", statusList.get( 0 ).getStepname() );
    assertEquals( "Group by", statusList.get( 1 ).getStepname() );
    assertEquals( "Write to log", statusList.get( 2 ).getStepname() );
    for ( Map.Entry<String, StepStatus> entry : statuses.entrySet() ) {
      StepStatus statusSpy = spy( entry.getValue() );
      statuses.put( entry.getKey(), statusSpy );
    }

    subtransExecutor.execute( rows );
    for ( Map.Entry<String, StepStatus> entry : statuses.entrySet() ) {
      verify( entry.getValue() ).updateAll( any() );
    }

    verify( parentTrans, atLeastOnce() ).addActiveSubTransformation( eq( "subtransname" ), any( Trans.class ) );
  }

  @Test
  @Ignore // TODO : move database connections out of .ktr files and move to metastore if needed
  public void stopsAll() throws HopException {
    TransMeta parentMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-parent.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    TransMeta subMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-sub.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    LoggingObjectInterface loggingObject = new LoggingObject( "anything" );
    Trans parentTrans = new Trans( parentMeta, loggingObject );
    SubtransExecutor subtransExecutor =
      new SubtransExecutor( "subtransname", parentTrans, subMeta, true, new TransExecutorParameters(), "" );
    subtransExecutor.running = Mockito.spy( subtransExecutor.running );
    RowMetaInterface rowMeta = parentMeta.getStepFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Pentaho", 1L ),
      new RowMetaAndData( rowMeta, "Pentaho", 2L ),
      new RowMetaAndData( rowMeta, "Pentaho", 3L ),
      new RowMetaAndData( rowMeta, "Pentaho", 4L ) );
    subtransExecutor.execute( rows );
    verify( subtransExecutor.running ).add( any() );
    subtransExecutor.stop();
    assertTrue( subtransExecutor.running.isEmpty() );
  }

  @Test
  @Ignore // TODO : move database connections out of .ktr files and move to metastore if needed
  public void doesNotExecuteWhenStopped() throws HopException {

    TransMeta parentMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-parent.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    TransMeta subMeta =
      new TransMeta( this.getClass().getResource( "subtrans-executor-sub.ktr" ).getPath(), new MemoryMetaStore(), true, new Variables() );
    LoggingObjectInterface loggingObject = new LoggingObject( "anything" );
    Trans parentTrans = new Trans( parentMeta, loggingObject );
    SubtransExecutor subtransExecutor =
      new SubtransExecutor( "subtransname", parentTrans, subMeta, true, new TransExecutorParameters(), "" );
    RowMetaInterface rowMeta = parentMeta.getStepFields( "Data Grid" );
    List<RowMetaAndData> rows = Arrays.asList(
      new RowMetaAndData( rowMeta, "Pentaho", 1L ),
      new RowMetaAndData( rowMeta, "Pentaho", 2L ),
      new RowMetaAndData( rowMeta, "Pentaho", 3L ),
      new RowMetaAndData( rowMeta, "Pentaho", 4L ) );
    subtransExecutor.stop();
    subtransExecutor.execute( rows );

    verify( this.logChannel, never() )
      .logBasic(
        "\n"
          + "------------> Linenr 1------------------------------\n"
          + "name = Pentaho\n"
          + "sum = 10\n"
          + "\n"
          + "===================="
      );
  }
}
