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

import org.junit.Test;

public class PartitionersTest {
  String pString;
  Partitioners expectedP;

  @Test
  public void testGetNames() {
    String[] expectedName = new String[] { "Murmur3Partitioner", "RandomPartitioner", "ByteOrderedPartitioner" };
    assertEquals( expectedName.length, Partitioners.values().length );
    for ( int i = 0; i < expectedName.length; i++ ) {
      assertEquals( expectedName[i], Partitioners.values()[i].getName() );
    }
  }

  @Test
  public void testGetTypes() {
    String[] expectedType =
        new String[] { "org.apache.cassandra.db.marshal.LongType", "org.apache.cassandra.db.marshal.IntegerType",
          "org.apache.cassandra.db.marshal.BytesType" };
    assertEquals( expectedType.length, Partitioners.values().length );
    for ( int i = 0; i < expectedType.length; i++ ) {
      assertEquals( expectedType[i], Partitioners.values()[i].getType() );
    }
  }

  @Test
  public void testGetFromStringMurMur3Partitioner() {
    pString = "Murmur3Partitioner";
    expectedP = Partitioners.MURMUR3;
    Partitioners actualP = Partitioners.getFromString( pString );
    assertEquals( expectedP, actualP );
  }

  @Test
  public void testGetFromStringRandomPartitioner() {
    pString = "RandomPartitioner";
    expectedP = Partitioners.RANDOM;
    Partitioners actualP = Partitioners.getFromString( pString );
    assertEquals( expectedP, actualP );
  }

  @Test
  public void testGetFromStringByteOrderedPartitioner() {
    pString = "ByteOrderedPartitioner";
    expectedP = Partitioners.BYTEORDERED;
    Partitioners actualP = Partitioners.getFromString( pString );
    assertEquals( expectedP, actualP );
  }

  @Test
  public void testGetDefaultMurMur3Partitioner_IfInputIsUnknownPartitioner() {
    pString = "UnknownPartitioner";
    expectedP = Partitioners.MURMUR3;
    Partitioners actualP = Partitioners.getFromString( pString );
    assertEquals( expectedP, actualP );
  }

  @Test
  public void testGetDefaultMurMur3Partitioner_IfInputIsNull() {
    pString = null;
    expectedP = Partitioners.MURMUR3;
    Partitioners actualP = Partitioners.getFromString( pString );
    assertEquals( expectedP, actualP );
  }

}
