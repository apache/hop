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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link BaseRowSet} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class BaseRowSetTests {

  @Test
  void testCompareToEqualsHashCode() {
    TestRowSet r1 = new TestRowSet();
    TestRowSet r2 = new TestRowSet();

    r1.setRemoteHopServerName("server");
    r2.setRemoteHopServerName("server");

    r1.setThreadNameFromToCopy("A", 1, "B", 2);
    r2.setThreadNameFromToCopy("X", 9, "B", 2);

    assertEquals(0, r1.compareTo(r2));
    assertEquals(r1, r2);
    assertEquals(r1.hashCode(), r2.hashCode());
  }

  @Test
  void testCompareToNotEqual() {
    TestRowSet r1 = new TestRowSet();
    TestRowSet r2 = new TestRowSet();

    r1.setRemoteHopServerName("server");
    r2.setRemoteHopServerName("server");

    r1.setThreadNameFromToCopy("A", 1, "B", 2);
    r2.setThreadNameFromToCopy("A", 1, "C", 2);

    assertNotEquals(0, r1.compareTo(r2));
    assertNotEquals(r1, r2);
  }

  @Test
  void testDoneFlag() {
    TestRowSet rowSet = new TestRowSet();
    assertFalse(rowSet.isDone());

    rowSet.setDone();
    assertTrue(rowSet.isDone());
  }

  @Test
  void testSetThreadName() {
    TestRowSet rowSet = new TestRowSet();
    rowSet.setThreadNameFromToCopy("from", 1, "to", 2);

    assertEquals("from", rowSet.getOriginTransformName());
    assertEquals(1, rowSet.getOriginTransformCopy());
    assertEquals("to", rowSet.getDestinationTransformName());
    assertEquals(2, rowSet.getDestinationTransformCopy());
  }

  @Test
  void testToString() {
    TestRowSet rowSet = new TestRowSet();
    rowSet.setThreadNameFromToCopy("A", 1, "B", 2);
    rowSet.setRemoteHopServerName("server");

    String result = rowSet.toString();

    assertTrue(result.contains("A"));
    assertTrue(result.contains("B"));
    assertTrue(result.contains("server"));
  }

  @Test
  void testConcurrentReadWrite() throws Exception {
    TestRowSet rowSet = new TestRowSet();

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      Runnable writer =
          () -> {
            for (int i = 0; i < 1000; i++) {
              rowSet.setThreadNameFromToCopy("A" + i, i, "B" + i, i);
            }
          };

      Callable<Boolean> reader =
          () -> {
            for (int i = 0; i < 1000; i++) {
              String string = rowSet.toString();
              assertNotNull(string);

              String dest = rowSet.getDestinationTransformName();
              if (StringUtils.isNotBlank(dest)) {
                assertTrue(dest.startsWith("B"));
              }
            }
            return true;
          };

      Future<?> w = executor.submit(writer);
      Future<Boolean> r = executor.submit(reader);

      w.get();
      assertTrue(r.get());
    }
  }

  static class TestRowSet extends BaseRowSet {
    @Override
    public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
      this.rowMeta = rowMeta;
      return false;
    }

    @Override
    public boolean putRowWait(IRowMeta rowMeta, Object[] rowData, long time, TimeUnit tu) {
      this.rowMeta = rowMeta;
      return false;
    }

    @Override
    public Object[] getRow() {
      return new Object[0];
    }

    @Override
    public Object[] getRowImmediate() {
      return new Object[0];
    }

    @Override
    public Object[] getRowWait(long timeout, TimeUnit tu) {
      return new Object[0];
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public void clear() {
      // ignore code
    }
  }
}
