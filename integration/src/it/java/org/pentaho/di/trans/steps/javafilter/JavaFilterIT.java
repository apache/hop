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

package org.apache.hop.trans.steps.javafilter;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransTestFactory;

import static org.junit.Assert.assertTrue;

public class JavaFilterIT {

  private static final String realCondition = "VehicleType.equals(\"Car\")";
  private static final String stepName = "MyJavaFilter Step";
  private static final String fieldName = "VehicleType";

  @BeforeClass
  public static void before() throws HopException {
    HopEnvironment.init();
  }

  private static List<RowMetaAndData> getInputData() {
    List<RowMetaAndData> input = new ArrayList<RowMetaAndData>();
    RowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaString( fieldName ) );
    input.add( new RowMetaAndData( rm, new Object[]{ "Car" } ) );
    input.add( new RowMetaAndData( rm, new Object[]{ "Bus" } ) );

    return input;
  }

  @Test
  public void testCondition() throws HopException {
    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition( realCondition );

    TransMeta trans = TransTestFactory.generateTestTransformation( null, meta, stepName );
    List<RowMetaAndData> input = getInputData();

    List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( trans,
      TransTestFactory.INJECTOR_STEPNAME, stepName, TransTestFactory.DUMMY_STEPNAME, input );

    //Only the "Car" row should be returned, based upon the Java Filter condition
    assertTrue( result.size() == 1 );
    assertTrue( result.get( 0 ).getString( fieldName, null ).equals( "Car" ) );
  }

  @Test
  public void testVariableCondition() throws HopException {
    Variables varSpace = new Variables();
    varSpace.setVariable( "myVarCondition", realCondition );

    JavaFilterMeta meta = new JavaFilterMeta();
    meta.setCondition( "${myVarCondition}" );

    TransMeta trans = TransTestFactory.generateTestTransformation( varSpace, meta, stepName );
    List<RowMetaAndData> input = getInputData();

    List<RowMetaAndData> result = TransTestFactory.executeTestTransformation( trans,
      TransTestFactory.INJECTOR_STEPNAME, stepName, TransTestFactory.DUMMY_STEPNAME, input );

    //Only the "Car" row should be returned, based upon the Java Filter condition
    assertTrue( result.size() == 1 );
    assertTrue( result.get( 0 ).getString( fieldName, null ).equals( "Car" ) );
  }
}
