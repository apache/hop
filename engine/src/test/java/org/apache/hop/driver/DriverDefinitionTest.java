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

package org.apache.hop.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.database.DriverDownload;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DriverDefinition}, the catalog entry that pairs a database plugin with the
 * {@link DriverDownload} it declares. Pure logic, no plugin registry or network needed.
 */
class DriverDefinitionTest {

  private static DriverDownload restricted() {
    return DriverDownload.builder()
        .mavenCoordinate("com.oracle.database.jdbc:ojdbc11")
        .defaultVersion("23.5.0.24.07")
        .licenseCategory("X")
        .build();
  }

  @Test
  void idIsTheDatabaseTypeLowercased() {
    DriverDownload download = restricted();
    DriverDefinition def =
        new DriverDefinition("ORACLE", "Oracle", "oracle.jdbc.OracleDriver", download);

    assertEquals("oracle", def.getId());
    assertEquals("ORACLE", def.getDatabaseType());
    assertEquals("Oracle", def.getName());
    assertEquals("oracle.jdbc.OracleDriver", def.getDriverClass());
    assertSame(download, def.getDownload(), "the declared descriptor is exposed as-is");
  }

  @Test
  void nullDatabaseTypeYieldsNullId() {
    DriverDefinition def = new DriverDefinition(null, "Nameless", "no.Driver", null);
    assertNull(def.getId(), "a null database type must not throw and must give a null id");
  }

  @Test
  void restrictedFollowsTheDownloadCategory() {
    assertTrue(
        new DriverDefinition("ORACLE", "Oracle", "oracle.jdbc.OracleDriver", restricted())
            .isRestricted());

    DriverDownload open =
        DriverDownload.builder()
            .mavenCoordinate("org.postgresql:postgresql")
            .defaultVersion("42.7.4")
            .licenseCategory("A")
            .build();
    assertFalse(
        new DriverDefinition("POSTGRESQL", "PostgreSQL", "org.postgresql.Driver", open)
            .isRestricted());
  }

  @Test
  void withoutADownloadItIsNeitherRestrictedNorResolvable() {
    DriverDefinition def = new DriverDefinition("GENERIC", "Generic", "some.Driver", null);
    assertFalse(def.isRestricted(), "no descriptor -> not restricted");
    assertNull(def.toCoordinate(null), "no descriptor -> no coordinate to resolve");
  }

  @Test
  void coordinateDelegatesToTheDownloadAndHonoursOverride() {
    DriverDefinition def =
        new DriverDefinition("ORACLE", "Oracle", "oracle.jdbc.OracleDriver", restricted());

    assertEquals("com.oracle.database.jdbc:ojdbc11:23.5.0.24.07", def.toCoordinate(null));
    assertEquals("com.oracle.database.jdbc:ojdbc11:21.0.0", def.toCoordinate("21.0.0"));
  }
}
