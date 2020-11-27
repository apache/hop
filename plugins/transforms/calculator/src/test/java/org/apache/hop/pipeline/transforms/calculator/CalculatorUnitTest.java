/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.calculator;

import junit.framework.Assert;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for calculator transform
 *
 * @author Pavel Sakun
 * @see Calculator
 */
public class CalculatorUnitTest {
  private static final Class<?> PKG = CalculatorUnitTest.class; // Needed by Translator
  private TransformMockHelper<CalculatorMeta, CalculatorData> smh;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void init() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    smh = new TransformMockHelper<>( "Calculator", CalculatorMeta.class, CalculatorData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
    when( smh.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testMissingFile() throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaString pathMeta = new ValueMetaString( "Path" );
    inputRowMeta.addValueMeta( pathMeta );

    String filepath = "missingFile";
    Object[] rows = new Object[] { filepath };
    IRowSet inputRowSet = smh.getMockInputRowSet( rows );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    CalculatorMetaFunction[] calculations = new CalculatorMetaFunction[] {
      new CalculatorMetaFunction( "result", CalculatorMetaFunction.CALC_MD5, "Path", null, null,
        IValueMeta.TYPE_STRING, 0, 0, false, "", "", "", "" ) };
    meta.setCalculation( calculations );
    meta.setFailIfNoFile( true );

    CalculatorData data = new CalculatorData();

    Calculator calculator = spy( new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline ) );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();



    boolean processed = calculator.processRow();
    verify( calculator, times( 1 ) ).logError( argThat( new ArgumentMatcher<String>() {
      @Override
      public boolean matches( Object o ) {
        return ( (String) o ).contains( BaseMessages.getString( PKG, "Calculator.Log.NoFile" ) );
      }
    } ) );
    assertFalse( processed );
  }

  @Test
  public void testAddSeconds() throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaDate dayMeta = new ValueMetaDate( "Day" );
    inputRowMeta.addValueMeta( dayMeta );
    ValueMetaInteger secondsMeta = new ValueMetaInteger( "Seconds" );
    inputRowMeta.addValueMeta( secondsMeta );

    IRowSet inputRowSet = null;
    try {
      inputRowSet = smh.getMockInputRowSet( new Object[][] {
        { new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse( "2014-01-01 00:00:00" ), new Long( 10 ) },
        { new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse( "2014-10-31 23:59:50" ), new Long( 30 ) } } );
    } catch ( ParseException pe ) {
      pe.printStackTrace();
      fail();
    }
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] {
      new CalculatorMetaFunction( "new_day", CalculatorMetaFunction.CALC_ADD_SECONDS, "Day", "Seconds", null,
        IValueMeta.TYPE_DATE, 0, 0, false, "", "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    //Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          try {
            assertEquals( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse( "2014-01-01 00:00:10" ), row[ 2 ] );
          } catch ( ParseException pe ) {
            throw new HopTransformException( pe );
          }
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }
  }

  @Test
  public void testReturnDigitsOnly() throws HopException {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaString nameMeta = new ValueMetaString( "Name" );
    inputRowMeta.addValueMeta( nameMeta );
    ValueMetaString valueMeta = new ValueMetaString( "Value" );
    inputRowMeta.addValueMeta( valueMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[][] { { "name1", "qwe123asd456zxc" }, { "name2", null } } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] {
      new CalculatorMetaFunction( "digits", CalculatorMetaFunction.CALC_GET_ONLY_DIGITS, "Value", null, null,
        IValueMeta.TYPE_STRING, 0, 0, false, "", "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    // Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          assertEquals( "123456", row[ 2 ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }
  }

  @Test
  public void calculatorShouldClearDataInstance() throws Exception {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaInteger valueMeta = new ValueMetaInteger( "Value" );
    inputRowMeta.addValueMeta( valueMeta );

    IRowSet inputRowSet = smh.getMockInputRowSet( new Object[] { -1L } );
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] {
      new CalculatorMetaFunction( "test", CalculatorMetaFunction.CALC_ABS, "Value", null, null, IValueMeta.TYPE_STRING, 0, 0, false, "", "", "", "" ) } );

    CalculatorData data = spy( new CalculatorData() );

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    calculator.processRow();
    verify( data ).getValueMetaFor( eq( valueMeta.getType() ), anyString() );

    calculator.processRow();
    verify( data ).clearValuesMetaMapping();
  }

  @Test
  public void testRound1() throws HopException {
    assertRound1( 1.0, 1.2 );
    assertRound1( 2.0, 1.5 );
    assertRound1( 2.0, 1.7 );
    assertRound1( 2.0, 2.2 );
    assertRound1( 3.0, 2.5 );
    assertRound1( 3.0, 2.7 );
    assertRound1( -1.0, -1.2 );
    assertRound1( -1.0, -1.5 );
    assertRound1( -2.0, -1.7 );
    assertRound1( -2.0, -2.2 );
    assertRound1( -2.0, -2.5 );
    assertRound1( -3.0, -2.7 );
    assertRound1( 1.0, 1.0 );
    assertRound1( 2.0, 2.0 );
    assertRound1( -3.0, -3.0 );
  }

  @Test
  public void testRound2() throws HopException {
    assertRound2( 1.0, 1.2, 0 );
    assertRound2( 2.0, 1.5, 0 );
    assertRound2( 2.0, 1.7, 0 );
    assertRound2( 2.0, 2.2, 0 );
    assertRound2( 3.0, 2.5, 0 );
    assertRound2( 3.0, 2.7, 0 );
    assertRound2( -1.0, -1.2, 0 );
    assertRound2( -1.0, -1.5, 0 );
    assertRound2( -2.0, -1.7, 0 );
    assertRound2( -2.0, -2.2, 0 );
    assertRound2( -2.0, -2.5, 0 );
    assertRound2( -3.0, -2.7, 0 );
    assertRound2( 1.0, 1.0, 0 );
    assertRound2( 2.0, 2.0, 0 );
    assertRound2( -3.0, -3.0, 0 );

    assertRound2( 0.010, 0.012, 2 );
    assertRound2( 0.020, 0.015, 2 );
    assertRound2( 0.020, 0.017, 2 );
    assertRound2( 0.020, 0.022, 2 );
    assertRound2( 0.030, 0.025, 2 );
    assertRound2( 0.030, 0.027, 2 );
    assertRound2( -0.010, -0.012, 2 );
    assertRound2( -0.010, -0.015, 2 );
    assertRound2( -0.020, -0.017, 2 );
    assertRound2( -0.020, -0.022, 2 );
    assertRound2( -0.020, -0.025, 2 );
    assertRound2( -0.030, -0.027, 2 );
    assertRound2( 0.010, 0.010, 2 );
    assertRound2( 0.020, 0.020, 2 );
    assertRound2( -0.030, -0.030, 2 );

    assertRound2( 100, 120, -2 );
    assertRound2( 200, 150, -2 );
    assertRound2( 200, 170, -2 );
    assertRound2( 200, 220, -2 );
    assertRound2( 300, 250, -2 );
    assertRound2( 300, 270, -2 );
    assertRound2( -100, -120, -2 );
    assertRound2( -100, -150, -2 );
    assertRound2( -200, -170, -2 );
    assertRound2( -200, -220, -2 );
    assertRound2( -200, -250, -2 );
    assertRound2( -300, -270, -2 );
    assertRound2( 100, 100, -2 );
    assertRound2( 200, 200, -2 );
    assertRound2( -300, -300, -2 );
  }

  @Test
  public void testRoundStd1() throws HopException {
    assertRoundStd1( 1.0, 1.2 );
    assertRoundStd1( 2.0, 1.5 );
    assertRoundStd1( 2.0, 1.7 );
    assertRoundStd1( 2.0, 2.2 );
    assertRoundStd1( 3.0, 2.5 );
    assertRoundStd1( 3.0, 2.7 );
    assertRoundStd1( -1.0, -1.2 );
    assertRoundStd1( -2.0, -1.5 );
    assertRoundStd1( -2.0, -1.7 );
    assertRoundStd1( -2.0, -2.2 );
    assertRoundStd1( -3.0, -2.5 );
    assertRoundStd1( -3.0, -2.7 );
    assertRoundStd1( 1.0, 1.0 );
    assertRoundStd1( 2.0, 2.0 );
    assertRoundStd1( -3.0, -3.0 );
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

  @Test
  public void testRoundCustom1() throws HopException {
    assertRoundCustom1( 2.0, 1.2, BigDecimal.ROUND_UP );
    assertRoundCustom1( 1.0, 1.2, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 2.0, 1.2, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 1.0, 1.2, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 1.0, 1.2, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 1.0, 1.2, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 1.0, 1.2, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 1.0, 1.2, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( 2.0, 1.5, BigDecimal.ROUND_UP );
    assertRoundCustom1( 1.0, 1.5, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 2.0, 1.5, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 1.0, 1.5, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 2.0, 1.5, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 1.0, 1.5, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 2.0, 1.5, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 2.0, 1.5, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( 2.0, 1.7, BigDecimal.ROUND_UP );
    assertRoundCustom1( 1.0, 1.7, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 2.0, 1.7, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 1.0, 1.7, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 2.0, 1.7, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 2.0, 1.7, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 2.0, 1.7, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 2.0, 1.7, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( 3.0, 2.2, BigDecimal.ROUND_UP );
    assertRoundCustom1( 2.0, 2.2, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 3.0, 2.2, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 2.0, 2.2, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 2.0, 2.2, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 2.0, 2.2, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 2.0, 2.2, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 2.0, 2.2, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( 3.0, 2.5, BigDecimal.ROUND_UP );
    assertRoundCustom1( 2.0, 2.5, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 3.0, 2.5, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 2.0, 2.5, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 3.0, 2.5, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 2.0, 2.5, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 2.0, 2.5, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 3.0, 2.5, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( 3.0, 2.7, BigDecimal.ROUND_UP );
    assertRoundCustom1( 2.0, 2.7, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( 3.0, 2.7, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( 2.0, 2.7, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( 3.0, 2.7, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( 3.0, 2.7, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( 3.0, 2.7, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( 3.0, 2.7, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -2.0, -1.2, BigDecimal.ROUND_UP );
    assertRoundCustom1( -1.0, -1.2, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -1.0, -1.2, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -2.0, -1.2, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -1.0, -1.2, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -1.0, -1.2, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -1.0, -1.2, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -1.0, -1.2, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -2.0, -1.5, BigDecimal.ROUND_UP );
    assertRoundCustom1( -1.0, -1.5, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -1.0, -1.5, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -2.0, -1.5, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -2.0, -1.5, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -1.0, -1.5, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -2.0, -1.5, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -1.0, -1.5, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -2.0, -1.7, BigDecimal.ROUND_UP );
    assertRoundCustom1( -1.0, -1.7, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -1.0, -1.7, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -2.0, -1.7, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -2.0, -1.7, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -2.0, -1.7, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -2.0, -1.7, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -2.0, -1.7, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -3.0, -2.2, BigDecimal.ROUND_UP );
    assertRoundCustom1( -2.0, -2.2, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -2.0, -2.2, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -3.0, -2.2, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -2.0, -2.2, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -2.0, -2.2, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -2.0, -2.2, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -2.0, -2.2, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -3.0, -2.5, BigDecimal.ROUND_UP );
    assertRoundCustom1( -2.0, -2.5, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -2.0, -2.5, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -3.0, -2.5, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -3.0, -2.5, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -2.0, -2.5, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -2.0, -2.5, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -2.0, -2.5, Const.ROUND_HALF_CEILING );

    assertRoundCustom1( -3.0, -2.7, BigDecimal.ROUND_UP );
    assertRoundCustom1( -2.0, -2.7, BigDecimal.ROUND_DOWN );
    assertRoundCustom1( -2.0, -2.7, BigDecimal.ROUND_CEILING );
    assertRoundCustom1( -3.0, -2.7, BigDecimal.ROUND_FLOOR );
    assertRoundCustom1( -3.0, -2.7, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom1( -3.0, -2.7, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom1( -3.0, -2.7, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom1( -3.0, -2.7, Const.ROUND_HALF_CEILING );
  }

  @Test
  public void testRoundCustom2() throws HopException {
    assertRoundCustom2( 2.0, 1.2, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 1.0, 1.2, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 2.0, 1.2, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 1.0, 1.2, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 1.0, 1.2, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 1.0, 1.2, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 1.0, 1.2, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 1.0, 1.2, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( 2.0, 1.5, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 1.0, 1.5, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 2.0, 1.5, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 1.0, 1.5, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 2.0, 1.5, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 1.0, 1.5, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 2.0, 1.5, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 2.0, 1.5, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( 2.0, 1.7, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 1.0, 1.7, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 2.0, 1.7, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 1.0, 1.7, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 2.0, 1.7, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 2.0, 1.7, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 2.0, 1.7, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 2.0, 1.7, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( 3.0, 2.2, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 2.0, 2.2, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 3.0, 2.2, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 2.0, 2.2, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 2.0, 2.2, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 2.0, 2.2, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 2.0, 2.2, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 2.0, 2.2, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( 3.0, 2.5, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 2.0, 2.5, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 3.0, 2.5, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 2.0, 2.5, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 3.0, 2.5, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 2.0, 2.5, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 2.0, 2.5, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 3.0, 2.5, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( 3.0, 2.7, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( 2.0, 2.7, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( 3.0, 2.7, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( 2.0, 2.7, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( 3.0, 2.7, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( 3.0, 2.7, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( 3.0, 2.7, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( 3.0, 2.7, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -2.0, -1.2, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -1.0, -1.2, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -1.0, -1.2, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -2.0, -1.2, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -1.0, -1.2, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -1.0, -1.2, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -1.0, -1.2, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -1.0, -1.2, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -2.0, -1.5, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -1.0, -1.5, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -1.0, -1.5, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -2.0, -1.5, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -2.0, -1.5, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -1.0, -1.5, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -2.0, -1.5, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -1.0, -1.5, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -2.0, -1.7, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -1.0, -1.7, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -1.0, -1.7, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -2.0, -1.7, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -2.0, -1.7, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -2.0, -1.7, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -2.0, -1.7, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -2.0, -1.7, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -3.0, -2.2, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -2.0, -2.2, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -2.0, -2.2, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -3.0, -2.2, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -2.0, -2.2, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -2.0, -2.2, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -2.0, -2.2, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -2.0, -2.2, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -3.0, -2.5, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -2.0, -2.5, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -2.0, -2.5, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -3.0, -2.5, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -3.0, -2.5, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -2.0, -2.5, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -2.0, -2.5, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -2.0, -2.5, 0, Const.ROUND_HALF_CEILING );

    assertRoundCustom2( -3.0, -2.7, 0, BigDecimal.ROUND_UP );
    assertRoundCustom2( -2.0, -2.7, 0, BigDecimal.ROUND_DOWN );
    assertRoundCustom2( -2.0, -2.7, 0, BigDecimal.ROUND_CEILING );
    assertRoundCustom2( -3.0, -2.7, 0, BigDecimal.ROUND_FLOOR );
    assertRoundCustom2( -3.0, -2.7, 0, BigDecimal.ROUND_HALF_UP );
    assertRoundCustom2( -3.0, -2.7, 0, BigDecimal.ROUND_HALF_DOWN );
    assertRoundCustom2( -3.0, -2.7, 0, BigDecimal.ROUND_HALF_EVEN );
    assertRoundCustom2( -3.0, -2.7, 0, Const.ROUND_HALF_CEILING );
  }

  public void assertRoundGeneral( final Object expectedResult, final int calcFunction, final Number value,
                                  final Long precision, final Long roundingMode, final int valueDataType, final int functionDataType ) throws HopException {

    final String msg = getHopTypeName( valueDataType ) + "->" + getHopTypeName( functionDataType ) + " ";

    final RowMeta inputRowMeta = new RowMeta();
    final List<Object> inputValues = new ArrayList<Object>( 3 );

    final String fieldValue = "Value";
    final IValueMeta valueMeta;
    switch ( valueDataType ) {
      case IValueMeta.TYPE_BIGNUMBER:
        valueMeta = new ValueMetaBigNumber( fieldValue );
        break;
      case IValueMeta.TYPE_NUMBER:
        valueMeta = new ValueMetaNumber( fieldValue );
        break;
      case IValueMeta.TYPE_INTEGER:
        valueMeta = new ValueMetaInteger( fieldValue );
        break;
      default:
        throw new IllegalArgumentException( msg + "Unexpected value dataType: " + value.getClass().getName()
          + ". Long, Double or BigDecimal expected." );
    }
    inputRowMeta.addValueMeta( valueMeta );
    inputValues.add( value );

    final String fieldPrecision;
    final ValueMetaInteger precisionMeta;
    if ( precision == null ) {
      fieldPrecision = null;
      precisionMeta = null;
    } else {
      fieldPrecision = "Precision";
      precisionMeta = new ValueMetaInteger( fieldPrecision );
      inputRowMeta.addValueMeta( precisionMeta );
      inputValues.add( precision );
    }

    final String fieldRoundingMode;
    final ValueMetaInteger roundingModeMeta;
    if ( roundingMode == null ) {
      fieldRoundingMode = null;
      roundingModeMeta = null;
    } else {
      fieldRoundingMode = "RoundingMode";
      roundingModeMeta = new ValueMetaInteger( fieldRoundingMode );
      inputRowMeta.addValueMeta( roundingModeMeta );
      inputValues.add( roundingMode );
    }

    IRowSet inputRowSet = smh.getMockInputRowSet( inputValues.toArray() );
    inputRowSet.setRowMeta( inputRowMeta );
    final String fieldA = inputRowMeta.size() > 0 ? inputRowMeta.getValueMetaList().get( 0 ).getName() : null;
    final String fieldB = inputRowMeta.size() > 1 ? inputRowMeta.getValueMetaList().get( 1 ).getName() : null;
    final String fieldC = inputRowMeta.size() > 2 ? inputRowMeta.getValueMetaList().get( 2 ).getName() : null;

    final int resultDataType = functionDataType;

    final String fieldResult = "test";
    final int expectedResultRowSize = inputRowMeta.size() + 1;

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] { new CalculatorMetaFunction( fieldResult, calcFunction, fieldA,
      fieldB, fieldC, resultDataType, 2, 0, false, "", "", "", "" ) } );

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
          assertEquals( msg + " resultRowSize", expectedResultRowSize, rowMeta.size() );
          final int fieldResultIndex = rowMeta.size() - 1;
          assertEquals( msg + " fieldResult", fieldResult, rowMeta.getValueMeta( fieldResultIndex ).getName() );
          assertEquals( msg, expectedResult, row[ fieldResultIndex ] );
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail( msg + ke.getMessage() );
    }
  }

  /**
   * Asserts different data types according to specified expectedResult and value.<br/>
   * Double - TYPE_NUMBER, TYPE_BIGNUMBER<br/>
   * Integer - TYPE_NUMBER, TYPE_BIGNUMBER, TYPE_INTEGER
   *
   * @param expectedResult Double and Integer values allowed
   * @param calcFunction
   * @param value
   * @param precision
   * @param roundingMode
   * @throws HopException
   */
  public void assertRoundEveryDataType( final Number expectedResult, final int calcFunction, final Number value,
                                        final Long precision, final Long roundingMode ) throws HopException {
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      final double resultValue = expectedResult.doubleValue();
      assertRoundGeneral( resultValue, calcFunction, value.doubleValue(), precision, roundingMode, IValueMeta.TYPE_NUMBER,
        IValueMeta.TYPE_NUMBER );
      assertRoundGeneral( resultValue, calcFunction, new BigDecimal( String.valueOf( value.doubleValue() ) ),
        precision, roundingMode, IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_NUMBER );
      if ( isInt( value ) ) {
        assertRoundGeneral( resultValue, calcFunction, value.longValue(), precision, roundingMode, IValueMeta.TYPE_INTEGER,
          IValueMeta.TYPE_NUMBER );
      }
    }
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      final BigDecimal resultValue = BigDecimal.valueOf( expectedResult.doubleValue() );
      assertRoundGeneral( resultValue, calcFunction, value.doubleValue(), precision, roundingMode, IValueMeta.TYPE_NUMBER,
        IValueMeta.TYPE_BIGNUMBER );
      assertRoundGeneral( resultValue, calcFunction, new BigDecimal( String.valueOf( value.doubleValue() ) ),
        precision, roundingMode, IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_BIGNUMBER );
      if ( isInt( value ) ) {
        assertRoundGeneral( resultValue, calcFunction, value.longValue(), precision, roundingMode, IValueMeta.TYPE_INTEGER,
          IValueMeta.TYPE_BIGNUMBER );
      }
    }
    if ( isInt( expectedResult ) ) {
      final Long resultValue = expectedResult.longValue();
      assertRoundGeneral( resultValue, calcFunction, value.doubleValue(), precision, roundingMode, IValueMeta.TYPE_NUMBER,
        IValueMeta.TYPE_INTEGER );
      assertRoundGeneral( resultValue, calcFunction, new BigDecimal( String.valueOf( value.doubleValue() ) ),
        precision, roundingMode, IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_INTEGER );
      if ( isInt( value ) ) {
        assertRoundGeneral( resultValue, calcFunction, value.longValue(), precision, roundingMode, IValueMeta.TYPE_INTEGER,
          IValueMeta.TYPE_INTEGER );
      }
    }
  }

  public void assertRound1( final Number expectedResult, final Number value ) throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_1, value, null, null );
  }

  public void assertRound2( final Number expectedResult, final Number value, final long precision )
    throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_2, value, precision, null );
  }

  public void assertRoundStd1( final Number expectedResult, final Number value ) throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_STD_1, value, null, null );
  }

  public void assertRoundStd2( final Number expectedResult, final Number value, final long precision )
    throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_STD_2, value, precision, null );
  }

  public void assertRoundCustom1( final Number expectedResult, final Number value, final long roundingMode )
    throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_CUSTOM_1, value, null, roundingMode );
  }

  public void assertRoundCustom2( final Number expectedResult, final Number value, final long precision,
                                  final long roundingMode ) throws HopException {
    assertRoundEveryDataType( expectedResult, CalculatorMetaFunction.CALC_ROUND_CUSTOM_2, value, precision, roundingMode );
  }

  /**
   * Check whether value represents a whole number
   *
   * @param value
   * @return
   */
  private static boolean isInt( Number value ) {
    if ( value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte
      || value instanceof BigInteger ) {
      return true;
    }
    final BigDecimal bigDecimalValue;
    if ( value instanceof Double || value instanceof Float ) {
      bigDecimalValue = new BigDecimal( value.toString() );
    } else if ( value instanceof BigDecimal ) {
      bigDecimalValue = (BigDecimal) value;
    } else {
      throw new IllegalArgumentException( "Unexpected dataType: " + value.getClass().getName() );
    }
    try {
      bigDecimalValue.longValueExact();
      return true;
    } catch ( ArithmeticException e ) {
      return false;
    }
  }

  private String getHopTypeName( int kettleNumberDataType ) {
    final String kettleNumberDataTypeName;
    switch ( kettleNumberDataType ) {
      case IValueMeta.TYPE_BIGNUMBER:
        kettleNumberDataTypeName = "BigNumber(" + kettleNumberDataType + ")";
        break;
      case IValueMeta.TYPE_NUMBER:
        kettleNumberDataTypeName = "Number(" + kettleNumberDataType + ")";
        break;
      case IValueMeta.TYPE_INTEGER:
        kettleNumberDataTypeName = "Integer(" + kettleNumberDataType + ")";
        break;
      default:
        kettleNumberDataTypeName = "?(" + kettleNumberDataType + ")";
    }
    return kettleNumberDataTypeName;
  }

  public static void assertEquals( Object expected, Object actual ) {
    assertEquals( null, expected, actual );
  }

  public static void assertEquals( String msg, Object expected, Object actual ) {
    if ( expected instanceof BigDecimal && actual instanceof BigDecimal ) {
      if ( ( (BigDecimal) expected ).compareTo( (BigDecimal) actual ) != 0 ) {
        Assert.assertEquals( msg, expected, actual );
      }
    } else {
      Assert.assertEquals( msg, expected, actual );
    }
  }

  @Test
  public void calculatorReminder() throws Exception {
    assertCalculatorReminder( new Double( "0.10000000000000053" ), new Object[] { new Long( "10" ), new Double( "3.3" ) },
      new int[] { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER } );
    assertCalculatorReminder( new Double( "1.0" ), new Object[] { new Long( "10" ), new Double( "4.5" ) },
      new int[] { IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER } );
    assertCalculatorReminder( new Double( "4.0" ), new Object[] { new Double( "12.5" ), new Double( "4.25" ) },
      new int[] { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_NUMBER } );
    assertCalculatorReminder( new Double( "2.6000000000000005" ), new Object[] { new Double( "12.5" ), new Double( "3.3" ) },
      new int[] { IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_NUMBER } );
  }

  private void assertCalculatorReminder( final Object expectedResult, final Object[] values, final int[] types ) throws Exception {
    RowMeta inputRowMeta = new RowMeta();
    for ( int i = 0; i < types.length; i++ ) {
      switch ( types[ i ] ) {
        case IValueMeta.TYPE_BIGNUMBER:
          inputRowMeta.addValueMeta( new ValueMetaBigNumber( "f" + i ) );
          break;
        case IValueMeta.TYPE_NUMBER:
          inputRowMeta.addValueMeta( new ValueMetaNumber( "f" + i ) );
          break;
        case IValueMeta.TYPE_INTEGER:
          inputRowMeta.addValueMeta( new ValueMetaInteger( "f" + i ) );
          break;
        default:
          throw new IllegalArgumentException( "Unexpected value dataType: " + types[ i ]
            + ". Long, Double or BigDecimal expected." );
      }
    }

    IRowSet inputRowSet = null;
    try {
      inputRowSet = smh.getMockInputRowSet( new Object[][] {
        { values[ 0 ], values[ 1 ] } } );
    } catch ( Exception pe ) {
      pe.printStackTrace();
      fail();
    }
    inputRowSet.setRowMeta( inputRowMeta );

    CalculatorMeta meta = new CalculatorMeta();
    meta.setCalculation( new CalculatorMetaFunction[] {
      new CalculatorMetaFunction( "res", CalculatorMetaFunction.CALC_REMAINDER, "f0", "f1", null,
        IValueMeta.TYPE_NUMBER, 0, 0, false, "", "", "", "" ) } );

    CalculatorData data = new CalculatorData();

    Calculator calculator = new Calculator( smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline );
    calculator.addRowSetToInputRowSets( inputRowSet );
    calculator.setInputRowMeta( inputRowMeta );
    calculator.init();

    //Verify output
    try {
      calculator.addRowListener( new RowAdapter() {
        @Override public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
          try {
            assertEquals( expectedResult, row[ 2 ] );
          } catch ( Exception pe ) {
            throw new HopTransformException( pe );
          }
        }
      } );
      calculator.processRow();
    } catch ( HopException ke ) {
      ke.printStackTrace();
      fail();
    }
  }
}
