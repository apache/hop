/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.database.cassandra.util;

import org.apache.hop.databases.cassandra.util.CqlFunctions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CqlFunctionsTest {

  @Test
  public void testGetFunctionsFromString() {
    String[] fString =
        new String[] {"TOKEN", "COUNT", "WRITETIME", "TTL", "DATEOF", "UNIXTIMESTAMPOF"};
    for (int i = 0; i < CqlFunctions.values().length; i++) {
      CqlFunctions actualF = CqlFunctions.getFromString(fString[i]);
      assertEquals(CqlFunctions.values()[i], actualF);
    }
  }

  @Test
  public void testGetFunctionsValidators() {
    String[] expectedValidators =
        new String[] {
          "org.apache.cassandra.db.marshal.LongType",
          "org.apache.cassandra.db.marshal.LongType",
          "org.apache.cassandra.db.marshal.LongType",
          "org.apache.cassandra.db.marshal.Int32Type",
          "org.apache.cassandra.db.marshal.TimestampType",
          "org.apache.cassandra.db.marshal.LongType"
        };
    assertEquals(expectedValidators.length, CqlFunctions.values().length);
    for (int i = 0; i < expectedValidators.length; i++) {
      assertEquals(
          "Incorrect validator for the function: " + CqlFunctions.values()[i].name(),
          expectedValidators[i],
          CqlFunctions.values()[i].getValidator());
    }
  }

  @Test
  public void testGetNull_IfInputIsUnknownFunction() {
    CqlFunctions actualP = CqlFunctions.getFromString("UnknownFunction");
    assertNull(actualP);
  }

  @Test
  public void testGetNull_IfInputIsNull() {
    CqlFunctions actualP = CqlFunctions.getFromString(null);
    assertNull(actualP);
  }
}
