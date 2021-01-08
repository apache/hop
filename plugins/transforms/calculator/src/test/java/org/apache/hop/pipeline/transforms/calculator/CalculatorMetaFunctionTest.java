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

import org.apache.hop.core.row.IValueMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CalculatorMetaFunctionTest {

  @Test
  public void testEquals() {
    CalculatorMetaFunction meta1 = new CalculatorMetaFunction();
    CalculatorMetaFunction meta2 = (CalculatorMetaFunction) meta1.clone();
    assertNotSame( meta1, meta2 );

    assertFalse( meta1.equals( null ) );
    assertFalse( meta1.equals( new Object() ) );
    assertTrue( meta1.equals( meta2 ) );

    meta2.setCalcType( CalculatorMetaFunction.CALC_ADD_DAYS );
    assertFalse( meta1.equals( meta2 ) );
  }

  @Test
  public void testGetCalcFunctionLongDesc() {
    assertNull( CalculatorMetaFunction.getCalcFunctionLongDesc( Integer.MIN_VALUE ) );
    assertNull( CalculatorMetaFunction.getCalcFunctionLongDesc( Integer.MAX_VALUE ) );
    assertNull( CalculatorMetaFunction.getCalcFunctionLongDesc( CalculatorMetaFunction.calcLongDesc.length ) );
  }

  @Test
  public void testGetCalcFunctionDefaultResultType() {
    assertEquals( IValueMeta.TYPE_NONE,
      CalculatorMetaFunction.getCalcFunctionDefaultResultType( Integer.MIN_VALUE ) );
    assertEquals( IValueMeta.TYPE_NONE,
      CalculatorMetaFunction.getCalcFunctionDefaultResultType( Integer.MAX_VALUE ) );
    assertEquals( IValueMeta.TYPE_NONE,
      CalculatorMetaFunction.getCalcFunctionDefaultResultType( -1 ) );
    assertEquals( IValueMeta.TYPE_STRING,
      CalculatorMetaFunction.getCalcFunctionDefaultResultType( CalculatorMetaFunction.CALC_CONSTANT ) );
    assertEquals( IValueMeta.TYPE_NUMBER,
      CalculatorMetaFunction.getCalcFunctionDefaultResultType( CalculatorMetaFunction.CALC_ADD ) );
  }
}
