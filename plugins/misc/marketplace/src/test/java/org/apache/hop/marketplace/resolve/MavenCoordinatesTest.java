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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class MavenCoordinatesTest {

  @Test
  void parseArtifactOnly() throws Exception {
    MavenCoordinates c = MavenCoordinates.parse("hop-tech-parquet", "org.apache.hop", "2.19.0");
    assertEquals("org.apache.hop", c.groupId());
    assertEquals("hop-tech-parquet", c.artifactId());
    assertEquals("2.19.0", c.version());
  }

  @Test
  void parseArtifactVersion() throws Exception {
    MavenCoordinates c =
        MavenCoordinates.parse("hop-engines-spark:2.19.0-SNAPSHOT", "org.apache.hop", "x");
    assertEquals("hop-engines-spark", c.artifactId());
    assertEquals("2.19.0-SNAPSHOT", c.version());
  }

  @Test
  void parseFullGav() throws Exception {
    MavenCoordinates c = MavenCoordinates.parse("org.example:foo:1.0", "ignored", "ignored");
    assertEquals("org.example", c.groupId());
    assertEquals("foo", c.artifactId());
    assertEquals("1.0", c.version());
  }

  @Test
  void zipPath() {
    MavenCoordinates c = new MavenCoordinates("org.apache.hop", "hop-tech-parquet", "2.19.0");
    assertEquals(
        "org/apache/hop/hop-tech-parquet/2.19.0/hop-tech-parquet-2.19.0.zip",
        c.zipRepositoryPath());
  }

  @Test
  void missingVersionFails() {
    HopException ex =
        assertThrows(
            HopException.class, () -> MavenCoordinates.parse("hop-tech-parquet", "g", null));
    assertTrue(ex.getMessage().toLowerCase().contains("version"));
  }
}
