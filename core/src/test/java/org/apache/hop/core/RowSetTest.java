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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test class for the basic functionality of IRowSet. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowSetTest {

  IRowMeta createRowMetaInterface() {
    IRowMeta rm = new RowMeta();

    IValueMeta[] valuesMeta = {
      new ValueMetaInteger("ROWNR"),
    };

    for (IValueMeta iValueMeta : valuesMeta) {
      rm.addValueMeta(iValueMeta);
    }

    return rm;
  }

  /** The basic stuff. */
  @Test
  void testBasicCreation() {
    IRowSet set = new BlockingRowSet(10);

    assertFalse(set.isDone());
    assertEquals(0, set.size());
  }

  /** Functionality test. */
  @Test
  void testFunctionality1() {
    IRowSet set = new BlockingRowSet(3);

    IRowMeta rm = createRowMetaInterface();

    Object[] r1 = new Object[] {1L};
    Object[] r2 = new Object[] {2L};
    Object[] r3 = new Object[] {3L};
    Object[] r4 = new Object[] {4L};
    Object[] r5 = new Object[] {5L};

    assertEquals(0, set.size());

    // Add first row. State 1
    set.putRow(rm, r1);

    assertEquals(1, set.size());

    // Add another row. State: 1 2
    set.putRow(rm, r2);

    assertEquals(2, set.size());

    // Pop off row. State: 2
    Object[] r = set.getRow();
    int i = rm.indexOfValue("ROWNR");
    assertEquals(1L, ((Long) r[i]).longValue());

    assertEquals(1, set.size());

    // Add another row. State: 2 3
    set.putRow(rm, r3);

    assertEquals(2, set.size());

    // Add another row. State: 2 3 4
    set.putRow(rm, r4);

    assertEquals(3, set.size());

    /*
     * This was made in more restrict in older version v2.5.0 with a new IRowSet implementation. After older version v2.5.0 you may not try to put
     * more rows in a rowset then it can hold (this functionality was also never used in Apache Hop anyway).
     */

    // Pop off row. State: 3 4
    r = set.getRow();
    i = rm.indexOfValue("ROWNR");
    assertEquals(2L, ((Long) r[i]).longValue());

    assertEquals(2, set.size());

    // Add another row. State: 3 4 5
    set.putRow(rm, r5);
    assertEquals(3, set.size());

    // Pop off row. State: 4 5
    r = set.getRow();
    i = rm.indexOfValue("ROWNR");
    assertEquals(3L, ((Long) r[i]).longValue());

    assertEquals(2, set.size());

    // Pop off row. State: 5
    r = set.getRow();
    i = rm.indexOfValue("ROWNR");
    assertEquals(4L, ((Long) r[i]).longValue());

    assertEquals(1, set.size());

    // Pop off row. State:
    r = set.getRow();
    i = rm.indexOfValue("ROWNR");
    assertEquals(5L, ((Long) r[i]).longValue());

    assertEquals(0, set.size());
  }

  /** Names test. Just for completeness. */
  @Test
  void testNames() {
    IRowSet set = new BlockingRowSet(3);

    set.setThreadNameFromToCopy("from", 2, "to", 3);

    assertEquals("from", set.getOriginTransformName());
    assertEquals(2, set.getOriginTransformCopy());
    assertEquals("to", set.getDestinationTransformName());
    assertEquals(3, set.getDestinationTransformCopy());
    assertEquals(set.toString(), set.getName());
    assertEquals("from.2 - to.3", set.getName());
  }
}
