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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import org.junit.jupiter.api.Test;

class SocketPortAllocationTest {

  @Test
  void constructorAndAccessors() {
    Date now = new Date();
    SocketPortAllocation a =
        new SocketPortAllocation(
            8080, now, "cluster1", "pipe", "srcSrv", "srcTr", "0", "tgtSrv", "tgtTr", "1");
    assertEquals(8080, a.getPort());
    assertEquals(now, a.getLastRequested());
    assertEquals("cluster1", a.getClusterRunId());
    assertEquals("pipe", a.getPipelineName());
    assertTrue(a.isAllocated());
    a.setAllocated(false);
    assertFalse(a.isAllocated());
    a.setPort(9090);
    assertEquals(9090, a.getPort());
    a.setPipelineName("p2");
    assertEquals("p2", a.getPipelineName());
  }

  @Test
  void equalsAndHashCodeUsePort() {
    Date d = new Date();
    SocketPortAllocation x =
        new SocketPortAllocation(1, d, "c", "p", "s", "t", "0", "s2", "t2", "1");
    SocketPortAllocation y =
        new SocketPortAllocation(1, d, "other", "p2", "s", "t", "0", "s2", "t2", "1");
    assertEquals(x, y);
    SocketPortAllocation z =
        new SocketPortAllocation(2, d, "c", "p", "s", "t", "0", "s2", "t2", "1");
    assertNotEquals(x, z);
    assertEquals(Integer.valueOf(1).hashCode(), x.hashCode());
  }
}
