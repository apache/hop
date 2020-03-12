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

package org.apache.hop.trans.steps.rowgenerator;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.RowAdapter;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RowGeneratorUnitTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private RowGenerator rowGenerator;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() throws HopException {
    // add variable to row generator step
    StepMetaInterface stepMetaInterface = spy( new RowGeneratorMeta() );
    ( (RowGeneratorMeta) stepMetaInterface ).setRowLimit( "${ROW_LIMIT}" );
    String[] strings = {};
    when( ( (RowGeneratorMeta) stepMetaInterface ).getFieldName() ).thenReturn( strings );

    StepMeta stepMeta = new StepMeta();
    stepMeta.setStepMetaInterface( stepMetaInterface );
    stepMeta.setName( "ROW_STEP_META" );
    StepDataInterface stepDataInterface = stepMeta.getStepMetaInterface().getStepData();

    // add variable to transformation variable space
    Map<String, String> map = new HashMap<>();
    map.put( "ROW_LIMIT", "1440" );
    TransMeta transMeta = spy( new TransMeta() );
    transMeta.injectVariables( map );
    when( transMeta.findStep( anyString() ) ).thenReturn( stepMeta );

    Trans trans = spy( new Trans( transMeta, null ) );
    when( trans.getSocketRepository() ).thenReturn( null );
    when( trans.getLogChannelId() ).thenReturn( "ROW_LIMIT" );

    //prepare row generator, substitutes variable by value from transformation variable space
    rowGenerator = spy( new RowGenerator( stepMeta, stepDataInterface, 0, transMeta, trans ) );
    rowGenerator.initializeVariablesFrom( trans );
    rowGenerator.init( stepMetaInterface, stepDataInterface );
  }

  @Test
  public void testReadRowLimitAsTransformationVar() throws HopException {
    long rowLimit = ( (RowGeneratorData) rowGenerator.getStepDataInterface() ).rowLimit;
    assertEquals( rowLimit, 1440 );
  }

  @Ignore
  @Test
  public void doesNotWriteRowOnTimeWhenStopped() throws HopException, InterruptedException {
    TransMeta transMeta = new TransMeta( getClass().getResource( "safe-stop.ktr" ).getPath(), new MemoryMetaStore(), true, Variables.getADefaultVariableSpace() );
    Trans trans = new Trans( transMeta );
    trans.prepareExecution();
    trans.getSteps().get( 1 ).step.addRowListener( new RowAdapter() {
      @Override public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {
        trans.safeStop();
      }
    } );
    trans.startThreads();
    trans.waitUntilFinished();
    assertEquals( 1, trans.getSteps().get( 0 ).step.getLinesWritten() );
    assertEquals( 1, trans.getSteps().get( 1 ).step.getLinesRead() );
  }
}
