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

package org.apache.hop.trans.steps.cubeinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransTestFactory;
import org.apache.hop.trans.steps.cubeoutput.CubeOutputMeta;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CubeInputStepIntIT {

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  List<RowMetaAndData> getSampleData() {
    List<RowMetaAndData> rmd = new ArrayList<RowMetaAndData>();
    RowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaString( "col1" ) );
    rmd.add( new RowMetaAndData( rm, new Object[] { "data1" } ) );
    rmd.add( new RowMetaAndData( rm, new Object[] { "data2" } ) );
    rmd.add( new RowMetaAndData( rm, new Object[] { "data3" } ) );
    rmd.add( new RowMetaAndData( rm, new Object[] { "data4" } ) );
    rmd.add( new RowMetaAndData( rm, new Object[] { "data5" } ) );
    return rmd;
  }

  @Test
  public void testPDI12897() throws HopException, IOException {
    File tempOutputFile = File.createTempFile( "testPDI11374", ".cube" );
    tempOutputFile.deleteOnExit();

    String stepName = "Cube Output";
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setFilename( tempOutputFile.getAbsolutePath() );

    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, meta, stepName );
    List<RowMetaAndData> inputList = getSampleData();
    TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
      TransTestFactory.DUMMY_STEPNAME, inputList );

    try {
      Thread.sleep( 1000 );
    } catch ( InterruptedException ignored ) {
      // Continue
    }

    // Now, check the result
    Variables varSpace = new Variables();
    varSpace.setVariable( "ROWS", "2" );

    String checkStepName = "Cube Input";
    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setFilename( tempOutputFile.getAbsolutePath() );
    inputMeta.setRowLimit( "${ROWS}" );

    transMeta = TransTestFactory.generateTestTransformation( varSpace, inputMeta, checkStepName );

    //Remove the Injector hop, as it's not needed for this transformation
    TransHopMeta injectHop = transMeta.findTransHop( transMeta.findStep( TransTestFactory.INJECTOR_STEPNAME ),
      transMeta.findStep( stepName ) );
    transMeta.removeTransHop( transMeta.indexOfTransHop( injectHop ) );

    inputList = new ArrayList<RowMetaAndData>();
    List<RowMetaAndData> result =
      TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
        TransTestFactory.DUMMY_STEPNAME, inputList );

    assertNotNull( result );
    assertEquals( 2, result.size() );
    assertEquals( 1, result.get( 0 ).getRowMeta().size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, result.get( 0 ).getValueMeta( 0 ).getType() );
    assertEquals( "col1", result.get( 0 ).getValueMeta( 0 ).getName() );
    assertEquals( "data1", result.get( 0 ).getString( 0, "fail" ) );
    assertEquals( "data2", result.get( 1 ).getString( 0, "fail" ) );
  }

  @Test
  public void testNoLimit() throws HopException, IOException {
    File tempOutputFile = File.createTempFile( "testNoLimit", ".cube" );
    tempOutputFile.deleteOnExit();

    String stepName = "Cube Output";
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setFilename( tempOutputFile.getAbsolutePath() );

    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, meta, stepName );
    List<RowMetaAndData> inputList = getSampleData();
    TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
      TransTestFactory.DUMMY_STEPNAME, inputList );

    try {
      Thread.sleep( 1000 );
    } catch ( InterruptedException ignored ) {
      // Continue
    }

    // Now, check the result
    String checkStepName = "Cube Input";
    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setFilename( tempOutputFile.getAbsolutePath() );

    transMeta = TransTestFactory.generateTestTransformation( null, inputMeta, checkStepName );

    //Remove the Injector hop, as it's not needed for this transformation
    TransHopMeta injectHop = transMeta.findTransHop( transMeta.findStep( TransTestFactory.INJECTOR_STEPNAME ),
      transMeta.findStep( stepName ) );
    transMeta.removeTransHop( transMeta.indexOfTransHop( injectHop ) );

    inputList = new ArrayList<RowMetaAndData>();
    List<RowMetaAndData> result =
      TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
        TransTestFactory.DUMMY_STEPNAME, inputList );

    assertNotNull( result );
    assertEquals( 5, result.size() );
    assertEquals( 1, result.get( 0 ).getRowMeta().size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, result.get( 0 ).getValueMeta( 0 ).getType() );
    assertEquals( "col1", result.get( 0 ).getValueMeta( 0 ).getName() );
    assertEquals( "data1", result.get( 0 ).getString( 0, "fail" ) );
    assertEquals( "data2", result.get( 1 ).getString( 0, "fail" ) );
    assertEquals( "data3", result.get( 2 ).getString( 0, "fail" ) );
    assertEquals( "data4", result.get( 3 ).getString( 0, "fail" ) );
    assertEquals( "data5", result.get( 4 ).getString( 0, "fail" ) );
  }

  @Test
  public void testNumericLimit() throws HopException, IOException {
    File tempOutputFile = File.createTempFile( "testNumericLimit", ".cube" );
    tempOutputFile.deleteOnExit();

    String stepName = "Cube Output";
    CubeOutputMeta meta = new CubeOutputMeta();
    meta.setDefault();
    meta.setFilename( tempOutputFile.getAbsolutePath() );

    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, meta, stepName );
    List<RowMetaAndData> inputList = getSampleData();
    TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
      TransTestFactory.DUMMY_STEPNAME, inputList );

    try {
      Thread.sleep( 1000 );
    } catch ( InterruptedException ignored ) {
      // Continue
    }

    // Now, check the result
    String checkStepName = "Cube Input";
    CubeInputMeta inputMeta = new CubeInputMeta();
    inputMeta.setFilename( tempOutputFile.getAbsolutePath() );
    inputMeta.setRowLimit( "3" );

    transMeta = TransTestFactory.generateTestTransformation( null, inputMeta, checkStepName );

    //Remove the Injector hop, as it's not needed for this transformation
    TransHopMeta injectHop = transMeta.findTransHop( transMeta.findStep( TransTestFactory.INJECTOR_STEPNAME ),
      transMeta.findStep( stepName ) );
    transMeta.removeTransHop( transMeta.indexOfTransHop( injectHop ) );

    inputList = new ArrayList<RowMetaAndData>();
    List<RowMetaAndData> result =
      TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
        TransTestFactory.DUMMY_STEPNAME, inputList );

    assertNotNull( result );
    assertEquals( 3, result.size() );
    assertEquals( 1, result.get( 0 ).getRowMeta().size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, result.get( 0 ).getValueMeta( 0 ).getType() );
    assertEquals( "col1", result.get( 0 ).getValueMeta( 0 ).getName() );
    assertEquals( "data1", result.get( 0 ).getString( 0, "fail" ) );
    assertEquals( "data2", result.get( 1 ).getString( 0, "fail" ) );
    assertEquals( "data3", result.get( 2 ).getString( 0, "fail" ) );
  }
}
