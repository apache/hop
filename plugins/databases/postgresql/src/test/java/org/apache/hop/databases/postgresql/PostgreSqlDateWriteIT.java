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
package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.dates.DateWriteScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/** Date values written through Hop are stored exactly as with a fresh calendar per value. */
class PostgreSqlDateWriteIT {

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  @Test
  void datesAreStoredAsWithAFreshCalendarPerValue() throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "pg",
            "POSTGRESQL",
            "Native",
            container.getHost(),
            container.getDatabaseName(),
            Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
            container.getUsername(),
            container.getPassword());
    try (Database db = new Database(new LoggingObject("dates"), new Variables(), databaseMeta)) {
      db.connect();
      db.setCommit(1000);
      DateWriteScenario.run(db, "TIMESTAMP(3)", "DATE", "DATES");
    }
  }
}
