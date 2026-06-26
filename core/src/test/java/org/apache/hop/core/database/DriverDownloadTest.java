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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DriverDownloadTest {

  @Test
  void coordinateUsesDefaultVersionAndHonoursOverride() {
    DriverDownload download =
        DriverDownload.builder()
            .mavenCoordinate("com.oracle.database.jdbc:ojdbc11")
            .defaultVersion("23.5.0.24.07")
            .licenseCategory("X")
            .build();

    assertEquals("com.oracle.database.jdbc:ojdbc11:23.5.0.24.07", download.toCoordinate(null));
    assertEquals("com.oracle.database.jdbc:ojdbc11:21.0.0", download.toCoordinate("21.0.0"));
    assertEquals("ojdbc11", download.getArtifactId());
  }

  @Test
  void coordinateKeepsAnExplicitClassifier() {
    // Some drivers ship a self-contained classified artifact, e.g. ClickHouse's "all" jar. The
    // version is appended after the group:artifact:packaging:classifier, and the artifactId is
    // still the second segment.
    DriverDownload download =
        DriverDownload.builder()
            .mavenCoordinate("com.clickhouse:clickhouse-jdbc:jar:all")
            .defaultVersion("0.9.8")
            .licenseCategory("A")
            .build();

    assertEquals("com.clickhouse:clickhouse-jdbc:jar:all:0.9.8", download.toCoordinate(null));
    assertEquals("clickhouse-jdbc", download.getArtifactId());
  }

  @Test
  void coordinateUsesDefaultVersionWhenOverrideIsBlank() {
    DriverDownload download =
        DriverDownload.builder()
            .mavenCoordinate("org.postgresql:postgresql")
            .defaultVersion("42.7.4")
            .build();

    assertEquals("org.postgresql:postgresql:42.7.4", download.toCoordinate("   "));
  }

  @Test
  void artifactIdIsNullForAnIncompleteCoordinate() {
    assertNull(DriverDownload.builder().build().getArtifactId());
    assertNull(DriverDownload.builder().mavenCoordinate("single-segment").build().getArtifactId());
  }

  @Test
  void restrictedIsBasedOnCategoryX() {
    assertTrue(DriverDownload.builder().licenseCategory("X").build().isRestricted());
    assertTrue(DriverDownload.builder().licenseCategory("x").build().isRestricted());
    assertFalse(DriverDownload.builder().licenseCategory("A").build().isRestricted());
    assertFalse(DriverDownload.builder().build().isRestricted());
  }

  @Test
  void databaseWithoutDownloadReturnsNullByDefault() {
    IDatabase database = new NoneDatabaseMeta();
    assertNull(database.getDriverDownload());
  }

  @Test
  void excludesDefaultToEmptyAndCarryValues() {
    assertTrue(DriverDownload.builder().build().getExcludes().isEmpty());
    assertEquals(
        java.util.List.of("com.google.protobuf:protobuf-java"),
        DriverDownload.builder()
            .excludes(java.util.List.of("com.google.protobuf:protobuf-java"))
            .build()
            .getExcludes());
  }

  @Test
  void repositoryUrlDefaultsToNullAndCarriesValue() {
    org.junit.jupiter.api.Assertions.assertNull(
        DriverDownload.builder().build().getRepositoryUrl());
    assertEquals(
        "https://clojars.org/repo/",
        DriverDownload.builder()
            .repositoryUrl("https://clojars.org/repo/")
            .build()
            .getRepositoryUrl());
  }
}
