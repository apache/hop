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
 */

package org.apache.hop.marketplace.resolve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SnapshotVersionsTest {

  @Test
  void uniqueToBase() {
    assertEquals("0.4.0-SNAPSHOT", SnapshotVersions.toBaseVersion("0.4.0-20260721.105615-1"));
    assertEquals("2.19.0-SNAPSHOT", SnapshotVersions.toBaseVersion("2.19.0-20260719.204953-1"));
  }

  @Test
  void alreadyBaseOrRelease() {
    assertEquals("0.4.0-SNAPSHOT", SnapshotVersions.toBaseVersion("0.4.0-SNAPSHOT"));
    assertEquals("1.2.3", SnapshotVersions.toBaseVersion("1.2.3"));
  }

  @Test
  void preferPathFolder() {
    assertEquals(
        "0.4.0-SNAPSHOT",
        SnapshotVersions.toBaseVersion(
            "0.4.0-20260721.105615-1",
            "/org/apache/hop/hop-datavault/0.4.0-SNAPSHOT/hop-datavault-0.4.0-20260721.105615-1.zip"));
  }

  @Test
  void isSnapshotFlags() {
    assertTrue(SnapshotVersions.isSnapshot("0.4.0-SNAPSHOT"));
    assertTrue(SnapshotVersions.isSnapshot("0.4.0-20260721.105615-1"));
    assertTrue(SnapshotVersions.isUniqueSnapshot("0.4.0-20260721.105615-1"));
    assertFalse(SnapshotVersions.isUniqueSnapshot("0.4.0-SNAPSHOT"));
    assertFalse(SnapshotVersions.isSnapshot("1.0.0"));
  }

  @Test
  void versionFolderFromPath() {
    assertEquals(
        "0.4.0-SNAPSHOT",
        SnapshotVersions.versionFolderFromPath(
            "/org/apache/hop/hop-datavault/0.4.0-SNAPSHOT/hop-datavault-0.4.0-20260721.105615-1.zip"));
  }
}
