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

package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MonetDbVersionTest {

  @Test
  void testConstructorWithInts() {
    MonetDbVersion v = new MonetDbVersion(11, 17, 17);
    assertEquals(11, v.getMajorVersion());
    assertEquals(17, v.getMinorVersion());
    assertEquals(17, v.getPatchVersion());
  }

  @Test
  void testParseFullVersion() throws MonetDbVersionException {
    MonetDbVersion v = new MonetDbVersion("11.17.17");
    assertEquals(11, v.getMajorVersion());
    assertEquals(17, v.getMinorVersion());
    assertEquals(17, v.getPatchVersion());
  }

  @Test
  void testParseMajorMinorOnly() throws MonetDbVersionException {
    MonetDbVersion v = new MonetDbVersion("11.0");
    assertEquals(11, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertNull(v.getPatchVersion());
  }

  @Test
  void testParseWithExtraParts() throws MonetDbVersionException {
    MonetDbVersion v = new MonetDbVersion("11.5.17.1");
    assertEquals(11, v.getMajorVersion());
    assertEquals(5, v.getMinorVersion());
    assertEquals(17, v.getPatchVersion());
  }

  @Test
  void testParseThrowsWhenNull() {
    MonetDbVersionException e =
        assertThrows(MonetDbVersionException.class, () -> new MonetDbVersion(null));
    assertTrue(e.getMessage() != null && e.getMessage().contains("null"));
  }

  @Test
  void testParseThrowsWhenInvalidFormat() {
    assertThrows(MonetDbVersionException.class, () -> new MonetDbVersion(""));
    assertThrows(MonetDbVersionException.class, () -> new MonetDbVersion("abc"));
    assertThrows(MonetDbVersionException.class, () -> new MonetDbVersion("11.17.a"));
    assertThrows(MonetDbVersionException.class, () -> new MonetDbVersion("11-17-17"));
  }

  @Test
  void testCompareTo() throws MonetDbVersionException {
    MonetDbVersion v111717 = new MonetDbVersion("11.17.17");
    MonetDbVersion v111717b = new MonetDbVersion("11.17.17");
    MonetDbVersion v111800 = new MonetDbVersion("11.18.0");
    MonetDbVersion v120000 = new MonetDbVersion("12.0.0");

    assertEquals(0, v111717.compareTo(v111717b));
    assertTrue(v111717.compareTo(v111800) < 0);
    assertTrue(v111800.compareTo(v111717) > 0);
    assertTrue(v111717.compareTo(v120000) < 0);
    assertTrue(v120000.compareTo(v111717) > 0);
  }

  @Test
  void testCompareToJan2014Sp2() throws MonetDbVersionException {
    MonetDbVersion older = new MonetDbVersion("11.17.16");
    MonetDbVersion same = new MonetDbVersion("11.17.17");
    MonetDbVersion newer = new MonetDbVersion("11.17.18");

    assertTrue(older.compareTo(MonetDbVersion.JAN_2014_SP2_DB_VERSION) < 0);
    assertEquals(0, same.compareTo(MonetDbVersion.JAN_2014_SP2_DB_VERSION));
    assertTrue(newer.compareTo(MonetDbVersion.JAN_2014_SP2_DB_VERSION) > 0);
  }

  @Test
  void testToString() throws MonetDbVersionException {
    MonetDbVersion v = new MonetDbVersion("11.17.17");
    String s = v.toString();
    assertTrue(s.contains("11"));
    assertTrue(s.contains("17"));
  }
}
