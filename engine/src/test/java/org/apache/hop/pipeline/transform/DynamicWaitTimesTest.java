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

package org.apache.hop.pipeline.transform;

import junit.framework.TestCase;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.IRowSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DynamicWaitTimesTest extends TestCase {

  DynamicWaitTimes.SingleStreamStatus status;
  AtomicInteger activeStreamIndex = new AtomicInteger();

  public void testSingleStreamStatus() {
    IRowSet rowSet = new BlockingRowSet(3);
    status = DynamicWaitTimes.build(Collections.singletonList(rowSet), () -> 0);
    assertEquals(1, status.get());
    status.adjust(true, rowSet);
    assertEquals(2, status.get());
    for (int i = 0; i < 10; i++) {
      status.adjust(true, rowSet);
    }
    assertEquals(DynamicWaitTimes.MAX_TIMEOUT, status.get());
  }

  public void testMultiStreamStatus() {
    List<IRowSet> rowSetList =
        new ArrayList<>(
            Arrays.asList(new BlockingRowSet(1), new BlockingRowSet(2), new BlockingRowSet(7)));
    status = DynamicWaitTimes.build(rowSetList, () -> activeStreamIndex.get());
    for (IRowSet iRowSet : rowSetList) {
      status.adjust(false, iRowSet);
      assertEquals(1, status.get());
      assertFalse(status.allowAdjust());
      activeStreamIndex.incrementAndGet();
    }

    // first. all input stream timeout
    activeStreamIndex.set(0);
    testAPeriod(rowSetList, 10);
    assertTrue(status.allowAdjust());

    // second. reset a input stream
    activeStreamIndex.set(0);
    status.doReset(0);
    testAPeriod(rowSetList, 4);
    assertTrue(status.allowAdjust());

    // third. remove a input stream
    activeStreamIndex.set(0);
    status.adjust(true, rowSetList.get(0));
    status.adjust(true, rowSetList.get(1));
    status.remove(rowSetList.get(1));
    rowSetList.remove(1);
    testAPeriod(rowSetList, 10);
    assertTrue(status.allowAdjust());

    // four. remove again
    activeStreamIndex.set(0);
    status.remove(rowSetList.get(1));
    rowSetList.remove(1);
    testAPeriod(rowSetList, 10);
    assertFalse(status.allowAdjust());
    testAPeriod(rowSetList, 10);
    assertFalse(status.allowAdjust());

    activeStreamIndex.set(0);
    status.remove(rowSetList.get(0));
    rowSetList.remove(0);
    testAPeriod(rowSetList, 10);
  }

  private void testAPeriod(List<IRowSet> rowSetList, int times) {
    for (int j = 0; j < rowSetList.size() * times; j++) {
      activeStreamIndex.set(0);
      for (IRowSet iRowSet : rowSetList) {
        status.adjust(true, iRowSet);
        activeStreamIndex.incrementAndGet();
      }
    }
  }
}
