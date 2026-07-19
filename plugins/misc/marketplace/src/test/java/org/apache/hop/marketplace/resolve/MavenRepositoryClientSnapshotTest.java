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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class MavenRepositoryClientSnapshotTest {

  @Test
  void parseSnapshotZipFromMetadata() {
    String xml =
        """
        <metadata>
          <version>2.19.0-SNAPSHOT</version>
          <versioning>
            <snapshot>
              <timestamp>20260719.204953</timestamp>
              <buildNumber>1</buildNumber>
            </snapshot>
            <snapshotVersions>
              <snapshotVersion>
                <extension>zip</extension>
                <value>2.19.0-20260719.204953-1</value>
                <updated>20260719204953</updated>
              </snapshotVersion>
              <snapshotVersion>
                <extension>pom</extension>
                <value>2.19.0-20260719.204953-1</value>
              </snapshotVersion>
            </snapshotVersions>
          </versioning>
        </metadata>
        """;
    assertEquals(
        "2.19.0-20260719.204953-1",
        MavenRepositoryClient.parseSnapshotZipValue(xml, "hop-tech-parquet", "2.19.0-SNAPSHOT"));
  }

  @Test
  void parseSnapshotFromTimestampOnly() {
    String xml =
        """
        <metadata>
          <versioning>
            <snapshot>
              <timestamp>20260101.120000</timestamp>
              <buildNumber>3</buildNumber>
            </snapshot>
          </versioning>
        </metadata>
        """;
    assertEquals(
        "1.0.0-20260101.120000-3",
        MavenRepositoryClient.parseSnapshotZipValue(xml, "x", "1.0.0-SNAPSHOT"));
  }

  @Test
  void emptyMetadata() {
    assertNull(MavenRepositoryClient.parseSnapshotZipValue("", "a", "1-SNAPSHOT"));
  }
}
