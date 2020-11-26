/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.cassandra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CQLFunctionsTest {

  @Test
  public void testGetFunctionsFromString() {
    String[] fString = new String[] { "TOKEN", "COUNT", "WRITETIME", "TTL", "DATEOF", "UNIXTIMESTAMPOF" };
    for ( int i = 0; i < CQLFunctions.values().length; i++ ) {
      CQLFunctions actualF = CQLFunctions.getFromString( fString[i] );
      assertEquals( CQLFunctions.values()[i], actualF );
    }
  }

  @Test
  public void testGetFunctionsValidators() {
    String[] expectedValidators =
        new String[] { "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.LongType",
          "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.Int32Type",
          "org.apache.cassandra.db.marshal.TimestampType", "org.apache.cassandra.db.marshal.LongType" };
    assertEquals( expectedValidators.length, CQLFunctions.values().length );
    for ( int i = 0; i < expectedValidators.length; i++ ) {
      assertEquals( "Incorrect validator for the function: " + CQLFunctions.values()[i].name(), expectedValidators[i],
          CQLFunctions.values()[i].getValidator() );
    }
  }

  @Test
  public void testGetNull_IfInputIsUnknownFunction() {
    CQLFunctions actualP = CQLFunctions.getFromString( "UnknownFunction" );
    assertNull( actualP );
  }

  @Test
  public void testGetNull_IfInputIsNull() {
    CQLFunctions actualP = CQLFunctions.getFromString( null );
    assertNull( actualP );
  }
}
