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

package org.apache.hop.beam.core;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HopRowTest {

  @Test
  public void equalsTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row2 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow hopRow1 = new HopRow(row1);
    HopRow hopRow2 = new HopRow(row2);
    assertTrue(hopRow1.equals( hopRow2 ));

    Object[] row3 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row4 = new Object[] { "AAA", "CCC", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow hopRow3 = new HopRow(row3);
    HopRow hopRow4 = new HopRow(row4);
    assertFalse(hopRow3.equals( hopRow4 ));

    Object[] row5 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row6 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow hopRow5 = new HopRow(row5);
    HopRow hopRow6 = new HopRow(row6);
    assertTrue(hopRow5.equals( hopRow6 ));

    Object[] row7 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row8 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow hopRow7 = new HopRow(row7);
    HopRow hopRow8 = new HopRow(row8);
    assertFalse(hopRow7.equals( hopRow8 ));
  }

  @Test
  public void hashCodeTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow hopRow1 = new HopRow(row1);
    assertEquals( -1023250643, hopRow1.hashCode() );
  }
}