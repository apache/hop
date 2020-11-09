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

package org.apache.hop.core;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConditionTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Test
  public void testNegatedTrueFuncEvaluatesAsFalse() throws Exception {
    String left = "test_filed";
    String right = "test_value";
    int func = Condition.FUNC_TRUE;
    boolean negate = true;

    Condition condition = new Condition( negate, left, func, right, null );
    assertFalse( condition.evaluate( new RowMeta(), new Object[] { "test" } ) );
  }

  @Test
  public void testPdi13227() throws Exception {
    IRowMeta rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta( new ValueMetaNumber( "name1" ) );
    rowMeta1.addValueMeta( new ValueMetaNumber( "name2" ) );
    rowMeta1.addValueMeta( new ValueMetaNumber( "name3" ) );

    IRowMeta rowMeta2 = new RowMeta();
    rowMeta2.addValueMeta( new ValueMetaNumber( "name2" ) );
    rowMeta2.addValueMeta( new ValueMetaNumber( "name1" ) );
    rowMeta2.addValueMeta( new ValueMetaNumber( "name3" ) );

    String left = "name1";
    String right = "name3";
    Condition condition = new Condition( left, Condition.FUNC_EQUAL, right, null );

    assertTrue( condition.evaluate( rowMeta1, new Object[] { 1.0, 2.0, 1.0 } ) );
    assertTrue( condition.evaluate( rowMeta2, new Object[] { 2.0, 1.0, 1.0 } ) );
  }

  @Test
  public void testNullLessThanNumberEvaluatesAsFalse() throws Exception {
    IRowMeta rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta( new ValueMetaInteger( "name1" ) );

    String left = "name1";
    ValueMetaAndData rightExact = new ValueMetaAndData( new ValueMetaInteger( "name1" ), new Long( -10 ) );

    Condition condition = new Condition( left, Condition.FUNC_SMALLER, null, rightExact );
    assertFalse( condition.evaluate( rowMeta1, new Object[] { null, "test" } ) );

    condition = new Condition( left, Condition.FUNC_SMALLER_EQUAL, null, rightExact );
    assertFalse( condition.evaluate( rowMeta1, new Object[] { null, "test" } ) );
  }
}
