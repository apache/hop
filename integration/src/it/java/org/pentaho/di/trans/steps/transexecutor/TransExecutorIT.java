/*
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 * **************************************************************************
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
 */

package org.apache.hop.trans.steps.transexecutor;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ObjectLocationSpecificationMethod;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.trans.RowProducer;
import org.apache.hop.trans.RowStepCollector;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.dummytrans.DummyTransMeta;
import org.apache.hop.trans.steps.injector.InjectorMeta;

public class TransExecutorIT {
  private static final String SAMPLE_INPUT = "abc";
  private static final String EXPECTED_SUBTRANS_OUTPUT_PATTERN = "aa";
  private static final int EXPECTED_SUBTRANS_OUTPUT_AMOUNT = 10;

  private static final String SUBTRANS_PATH = "src/it/resources/org.apache.hop/trans/steps/transexecutor/subtrans.ktr";


  @BeforeClass
  public static void setUpClass() throws Exception {
    HopEnvironment.init();
  }


  private PluginRegistry pluginRegistry;

  private StepMeta injector;
  private StepMeta transExecutor;
  private StepMeta dummy;
  private TransMeta transMeta;


  @Before
  public void setUp() throws Exception {
    pluginRegistry = PluginRegistry.getInstance();

    injector = createInjector( "Injector" );
    transExecutor = createExecutor( "Trans Executor" );
    dummy = createDummy( "Dummy Output" );

    TransExecutorMeta executorMeta = getExecutorMeta( transExecutor );
    executorMeta.setFileName( SUBTRANS_PATH );
    executorMeta.setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );

    transMeta = new TransMeta();
    transMeta.setName( "transformation executor" );

    transMeta.addStep( injector );
    transMeta.addStep( transExecutor );
    transMeta.addStep( dummy );

    // injector -> executor
    transMeta.addTransHop( new TransHopMeta( injector, transExecutor ) );
    // executor -> dummy
    transMeta.addTransHop( new TransHopMeta( transExecutor, dummy ) );
  }

  private StepMeta createInjector( String stepname ) {
    InjectorMeta im = new InjectorMeta();
    String injectorPid = pluginRegistry.getPluginId( StepPluginType.class, im );
    return new StepMeta( injectorPid, stepname, im );
  }

  private StepMeta createExecutor( String stepname ) {
    TransExecutorMeta transExecutorMeta = new TransExecutorMeta();
    String transExecutorPID = pluginRegistry.getPluginId( StepPluginType.class, transExecutorMeta );
    return new StepMeta( transExecutorPID, stepname, transExecutorMeta );
  }

  private static TransExecutorMeta getExecutorMeta( StepMeta stepMeta ) {
    return (TransExecutorMeta) stepMeta.getStepMetaInterface();
  }

  private StepMeta createDummy( String stepname ) {
    DummyTransMeta dummyTransMeta = new DummyTransMeta();
    String dummyStepPID = pluginRegistry.getPluginId( StepPluginType.class, dummyTransMeta );
    return new StepMeta( dummyStepPID, stepname, dummyTransMeta );
  }


  @Test
  public void subTransOutputIsAccessibleOutside() throws Exception {
    TransExecutorMeta executorMeta = getExecutorMeta( transExecutor );
    executorMeta.setOutputRowsSourceStepMeta( dummy );

    Trans trans = createTrans( transMeta );
    RowStepCollector endRc = listenExecutor( trans );
    RowProducer rp = trans.addRowProducer( injector.getName(), 0 );

    trans.startThreads();

    RowMetaAndData testInput = new RowMetaAndData( createRowMetaForOneField(), SAMPLE_INPUT );
    rp.putRow( testInput.getRowMeta(), testInput.getData() );
    rp.finished();

    trans.waitUntilFinished();

    assertEquals( EXPECTED_SUBTRANS_OUTPUT_AMOUNT, endRc.getRowsWritten().size() );
    assertThat( asList( endRc.getRowsWritten().get( 0 ).getData() ),
      hasItem( (Object) EXPECTED_SUBTRANS_OUTPUT_PATTERN )
    );
  }

  private static Trans createTrans( TransMeta transMeta ) throws Exception {
    Trans trans = new Trans( transMeta );
    trans.prepareExecution( null );
    return trans;
  }

  private RowStepCollector listenExecutor( Trans trans ) {
    StepInterface transExecutorStep = trans.getStepInterface( transExecutor.getName(), 0 );
    RowStepCollector rc = new RowStepCollector();
    transExecutorStep.addRowListener( rc );
    return rc;
  }

  private static RowMetaInterface createRowMetaForOneField() {
    RowMetaInterface rm = new RowMeta();
    ValueMetaInterface[] valuesMeta = { new ValueMetaString( "field1" ), };
    for ( ValueMetaInterface aValuesMeta : valuesMeta ) {
      rm.addValueMeta( aValuesMeta );
    }
    return rm;
  }


  @Test
  public void executorsInputIsStraightlyCopiedToOutput() throws Exception {
    TransExecutorMeta executorMeta = getExecutorMeta( transExecutor );
    executorMeta.setExecutorsOutputStepMeta( dummy );

    Trans trans = createTrans( transMeta );
    RowStepCollector endRc = listenExecutor( trans );
    RowProducer rp = trans.addRowProducer( injector.getName(), 0 );

    trans.startThreads();

    RowMetaAndData testInput = new RowMetaAndData( createRowMetaForOneField(), SAMPLE_INPUT );
    rp.putRow( testInput.getRowMeta(), testInput.getData() );
    rp.finished();

    trans.waitUntilFinished();

    assertEquals( testInput.size(), endRc.getRowsWritten().size() );
    assertThat( asList( endRc.getRowsWritten().get( 0 ).getData() ),
      hasItem( (Object) SAMPLE_INPUT )
    );
  }


  @Test
  public void subTransExecutionStatisticsIsCollected() throws Exception {
    TransExecutorMeta executorMeta = getExecutorMeta( transExecutor );
    executorMeta.setExecutionTimeField( "time" );
    executorMeta.setExecutionResultTargetStepMeta( dummy );

    Trans trans = createTrans( transMeta );
    RowStepCollector endRc = listenExecutor( trans );
    RowProducer rp = trans.addRowProducer( injector.getName(), 0 );

    trans.startThreads();

    RowMetaAndData testInput = new RowMetaAndData( createRowMetaForOneField(), SAMPLE_INPUT );
    rp.putRow( testInput.getRowMeta(), testInput.getData() );
    rp.finished();

    trans.waitUntilFinished();

    assertFalse( endRc.getRowsWritten().isEmpty() );
    // execution time field
    assertNotNull( endRc.getRowsWritten().get( 0 ).getData()[ 0 ] );
  }

}
