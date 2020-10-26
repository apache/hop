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

package org.apache.hop.core.row;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowMetaAddRemoveValueTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @BeforeClass
  public static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testAddRemoveValue() throws Exception {

    IRowMeta rowMeta = new RowMeta();

    // Add values

    IValueMeta a = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_STRING );
    rowMeta.addValueMeta( a );
    assertEquals( 1, rowMeta.size() );
    IValueMeta b = ValueMetaFactory.createValueMeta( "b", IValueMeta.TYPE_INTEGER );
    rowMeta.addValueMeta( b );
    assertEquals( 2, rowMeta.size() );
    IValueMeta c = ValueMetaFactory.createValueMeta( "c", IValueMeta.TYPE_DATE );
    rowMeta.addValueMeta( c );
    assertEquals( 3, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "b" ) );
    assertEquals( 2, rowMeta.indexOfValue( "c" ) );

    IValueMeta d = ValueMetaFactory.createValueMeta( "d", IValueMeta.TYPE_NUMBER );
    rowMeta.addValueMeta( 0, d );
    assertEquals( 4, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "d" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "b" ) );
    assertEquals( 3, rowMeta.indexOfValue( "c" ) );

    IValueMeta e = ValueMetaFactory.createValueMeta( "e", IValueMeta.TYPE_BIGNUMBER );
    rowMeta.addValueMeta( 2, e );
    assertEquals( 5, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "d" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "e" ) );
    assertEquals( 3, rowMeta.indexOfValue( "b" ) );
    assertEquals( 4, rowMeta.indexOfValue( "c" ) );

    // Remove values in reverse order
    rowMeta.removeValueMeta( "e" );
    assertEquals( 4, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "d" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "b" ) );
    assertEquals( 3, rowMeta.indexOfValue( "c" ) );

    rowMeta.removeValueMeta( "d" );
    assertEquals( 3, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "b" ) );
    assertEquals( 2, rowMeta.indexOfValue( "c" ) );

    rowMeta.removeValueMeta( "c" );
    assertEquals( 2, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "b" ) );

    rowMeta.removeValueMeta( "b" );
    assertEquals( 1, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );

    rowMeta.removeValueMeta( "a" );
    assertEquals( 0, rowMeta.size() );

  }

  @Test
  public void testAddRemoveRenameValue() throws Exception {

    IRowMeta rowMeta = new RowMeta();

    // Add values

    IValueMeta a = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_STRING );
    rowMeta.addValueMeta( a );
    assertEquals( 1, rowMeta.size() );
    IValueMeta b = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_INTEGER );
    rowMeta.addValueMeta( b );
    assertEquals( 2, rowMeta.size() );
    IValueMeta c = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_DATE );
    rowMeta.addValueMeta( c );
    assertEquals( 3, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a_1" ) );
    assertEquals( 2, rowMeta.indexOfValue( "a_2" ) );

    IValueMeta d = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_NUMBER );
    rowMeta.addValueMeta( 0, d );
    assertEquals( 4, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "a_3" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "a_1" ) );
    assertEquals( 3, rowMeta.indexOfValue( "a_2" ) );

    IValueMeta e = ValueMetaFactory.createValueMeta( "a", IValueMeta.TYPE_BIGNUMBER );
    rowMeta.addValueMeta( 2, e );
    assertEquals( 5, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "a_3" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "a_4" ) );
    assertEquals( 3, rowMeta.indexOfValue( "a_1" ) );
    assertEquals( 4, rowMeta.indexOfValue( "a_2" ) );

    // Remove values in reverse order
    rowMeta.removeValueMeta( "a_4" );
    assertEquals( 4, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a_3" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "a_1" ) );
    assertEquals( 3, rowMeta.indexOfValue( "a_2" ) );

    rowMeta.removeValueMeta( "a_3" );
    assertEquals( 3, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a_1" ) );
    assertEquals( 2, rowMeta.indexOfValue( "a_2" ) );

    rowMeta.removeValueMeta( "a_2" );
    assertEquals( 2, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a_1" ) );

    rowMeta.removeValueMeta( "a_1" );
    assertEquals( 1, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );

    rowMeta.removeValueMeta( "a" );
    assertEquals( 0, rowMeta.size() );

  }

  @Test
  public void testAddRemoveValueCaseInsensitive() throws Exception {

    IRowMeta rowMeta = new RowMeta();

    // Add values

    IValueMeta a = ValueMetaFactory.createValueMeta( "A", IValueMeta.TYPE_STRING );
    rowMeta.addValueMeta( a );
    assertEquals( 1, rowMeta.size() );
    IValueMeta b = ValueMetaFactory.createValueMeta( "b", IValueMeta.TYPE_INTEGER );
    rowMeta.addValueMeta( b );
    assertEquals( 2, rowMeta.size() );
    IValueMeta c = ValueMetaFactory.createValueMeta( "C", IValueMeta.TYPE_DATE );
    rowMeta.addValueMeta( c );
    assertEquals( 3, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "B" ) );
    assertEquals( 2, rowMeta.indexOfValue( "c" ) );

    IValueMeta d = ValueMetaFactory.createValueMeta( "d", IValueMeta.TYPE_NUMBER );
    rowMeta.addValueMeta( 0, d );
    assertEquals( 4, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "D" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "B" ) );
    assertEquals( 3, rowMeta.indexOfValue( "c" ) );

    IValueMeta e = ValueMetaFactory.createValueMeta( "E", IValueMeta.TYPE_BIGNUMBER );
    rowMeta.addValueMeta( 2, e );
    assertEquals( 5, rowMeta.size() );

    assertEquals( 0, rowMeta.indexOfValue( "D" ) );
    assertEquals( 1, rowMeta.indexOfValue( "a" ) );
    assertEquals( 2, rowMeta.indexOfValue( "e" ) );
    assertEquals( 3, rowMeta.indexOfValue( "b" ) );
    assertEquals( 4, rowMeta.indexOfValue( "c" ) );

    // Remove values in reverse order
    rowMeta.removeValueMeta( "e" );
    assertEquals( 4, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "d" ) );
    assertEquals( 1, rowMeta.indexOfValue( "A" ) );
    assertEquals( 2, rowMeta.indexOfValue( "b" ) );
    assertEquals( 3, rowMeta.indexOfValue( "C" ) );

    rowMeta.removeValueMeta( "D" );
    assertEquals( 3, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "B" ) );
    assertEquals( 2, rowMeta.indexOfValue( "c" ) );

    rowMeta.removeValueMeta( "c" );
    assertEquals( 2, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );
    assertEquals( 1, rowMeta.indexOfValue( "B" ) );

    rowMeta.removeValueMeta( "b" );
    assertEquals( 1, rowMeta.size() );
    assertEquals( 0, rowMeta.indexOfValue( "a" ) );

    rowMeta.removeValueMeta( "a" );
    assertEquals( 0, rowMeta.size() );

  }

}
