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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for calculator transform
 *
 * @author Pavel Sakun
 * @see Calculator
 */
public class CalculatorBackwardCompatibilityUnitTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private TransformMockHelper<CalculatorMeta, CalculatorData> smh;

  private static final String SYS_PROPERTY_ROUND_2_MODE = "ROUND_2_MODE";
  private static final int OBSOLETE_ROUND_2_MODE = BigDecimal.ROUND_HALF_EVEN;
  private static final int DEFAULT_ROUND_2_MODE = Const.ROUND_HALF_CEILING;

  /**
   * Get value of private static field ValueDataUtil.ROUND_2_MODE.
   *
   * @return
   */
  private static int getRound2Mode() {
    int value = -1;
    try {
      Class<ValueDataUtil> cls = ValueDataUtil.class;
      Field f = cls.getDeclaredField( SYS_PROPERTY_ROUND_2_MODE );
      f.setAccessible( true );
      value = (Integer) f.get( null );
      f.setAccessible( false );
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
    return value;
  }

  /**
   * Set new value of value of private static field ValueDataUtil.ROUND_2_MODE.
   *
   * @param newValue
   */
  private static void setRound2Mode( int newValue ) {
    try {
      Class<ValueDataUtil> cls = ValueDataUtil.class;
      Field f = cls.getDeclaredField( SYS_PROPERTY_ROUND_2_MODE );
      f.setAccessible( true );
      f.set( null, newValue );
      f.setAccessible( false );
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  @BeforeClass
  public static void init() throws HopException {
    assertEquals( DEFAULT_ROUND_2_MODE, getRound2Mode() );
    setRound2Mode( OBSOLETE_ROUND_2_MODE );
    assertEquals( OBSOLETE_ROUND_2_MODE, getRound2Mode() );

    HopEnvironment.init();
  }

  @AfterClass
  public static void restore() throws Exception {
    setRound2Mode( DEFAULT_ROUND_2_MODE );
    assertEquals( DEFAULT_ROUND_2_MODE, getRound2Mode() );
  }

  @Before
  public void setUp() {
    smh =
      new TransformMockHelper<>( "Calculator", CalculatorMeta.class,
        CalculatorData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testRound() throws HopException {
    assertRound( 1.0, 1.2 );
    assertRound( 2.0, 1.5 );
    assertRound( 2.0, 1.7 );
    assertRound( 2.0, 2.2 );
    assertRound( 3.0, 2.5 );
    assertRound( 3.0, 2.7 );
    assertRound( -1.0, -1.2 );
    assertRound( -1.0, -1.5 );
    assertRound( -2.0, -1.7 );
    assertRound( -2.0, -2.2 );
    assertRound( -2.0, -2.5 );
    assertRound( -3.0, -2.7 );
  }

  public void assertRound( final double expectedResult, final double value ) throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaNumber valueMeta = new ValueMetaNumber( "Value" );
    inputRowMeta.addValueMeta( valueMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[] { value } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] { new CalculatorMetaFunction( "test",
      CalculatorMetaFunction.CALC_ROUND_1, "Value", null, null, IValueMeta.TYPE_NUMBER, 2, 0, false, "", "",
      "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    // Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          assertEquals( expectedResult, row[ 1 ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }

  }

  @Test
  public void testRound2() throws HopException {
    assertRound2( 1.0, 1.2, 0 );
    assertRound2( 2.0, 1.5, 0 );
    assertRound2( 2.0, 1.7, 0 );
    assertRound2( 2.0, 2.2, 0 );
    assertRound2( 2.0, 2.5, 0 );
    assertRound2( 3.0, 2.7, 0 );
    assertRound2( -1.0, -1.2, 0 );
    assertRound2( -2.0, -1.5, 0 );
    assertRound2( -2.0, -1.7, 0 );
    assertRound2( -2.0, -2.2, 0 );
    assertRound2( -2.0, -2.5, 0 );
    assertRound2( -3.0, -2.7, 0 );
  }

  public void assertRound2( final double expectedResult, final double value, final long precision )
    throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaNumber valueMeta = new ValueMetaNumber( "Value" );
    ValueMetaInteger precisionMeta = new ValueMetaInteger( "Precision" );
    inputRowMeta.addValueMeta( valueMeta );
    inputRowMeta.addValueMeta( precisionMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[] { value, precision } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] { new CalculatorMetaFunction( "test",
      CalculatorMetaFunction.CALC_ROUND_2, "Value", "Precision", null, IValueMeta.TYPE_NUMBER, 2, 0, false,
      "", "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    // Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          assertEquals( expectedResult, row[ 2 ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }

  }

  @Test
  public void testRoundStd() throws HopException {
    assertRoundStd( 1.0, 1.2 );
    assertRoundStd( 2.0, 1.5 );
    assertRoundStd( 2.0, 1.7 );
    assertRoundStd( 2.0, 2.2 );
    assertRoundStd( 3.0, 2.5 );
    assertRoundStd( 3.0, 2.7 );
    assertRoundStd( -1.0, -1.2 );
    assertRoundStd( -2.0, -1.5 );
    assertRoundStd( -2.0, -1.7 );
    assertRoundStd( -2.0, -2.2 );
    assertRoundStd( -3.0, -2.5 );
    assertRoundStd( -3.0, -2.7 );
  }

  public void assertRoundStd( final double expectedResult, final double value ) throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaNumber valueMeta = new ValueMetaNumber( "Value" );
    inputRowMeta.addValueMeta( valueMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[] { value } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] { new CalculatorMetaFunction( "test",
      CalculatorMetaFunction.CALC_ROUND_STD_1, "Value", null, null, IValueMeta.TYPE_NUMBER, 2, 0, false, "",
      "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();



    // Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          assertEquals( expectedResult, row[ 1 ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }

  }

  @Test
  public void testRoundStd2() throws HopException {
    assertRoundStd2( 1.0, 1.2, 0 );
    assertRoundStd2( 2.0, 1.5, 0 );
    assertRoundStd2( 2.0, 1.7, 0 );
    assertRoundStd2( 2.0, 2.2, 0 );
    assertRoundStd2( 3.0, 2.5, 0 );
    assertRoundStd2( 3.0, 2.7, 0 );
    assertRoundStd2( -1.0, -1.2, 0 );
    assertRoundStd2( -2.0, -1.5, 0 );
    assertRoundStd2( -2.0, -1.7, 0 );
    assertRoundStd2( -2.0, -2.2, 0 );
    assertRoundStd2( -3.0, -2.5, 0 );
    assertRoundStd2( -3.0, -2.7, 0 );
  }

  public void assertRoundStd2( final double expectedResult, final double value, final long precision )
    throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaNumber valueMeta = new ValueMetaNumber( "Value" );
    ValueMetaInteger precisionMeta = new ValueMetaInteger( "Precision" );
    inputRowMeta.addValueMeta( valueMeta );
    inputRowMeta.addValueMeta( precisionMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[] { value, precision } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] { new CalculatorMetaFunction( "test",
      CalculatorMetaFunction.CALC_ROUND_STD_2, "Value", "Precision", null, IValueMeta.TYPE_NUMBER, 2, 0,
      false, "", "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();


    // Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override
        public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          assertEquals( expectedResult, row[ 2 ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }

  }

}
