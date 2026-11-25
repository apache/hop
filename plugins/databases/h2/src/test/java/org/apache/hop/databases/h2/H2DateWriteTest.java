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
package org.apache.hop.databases.h2;

import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.dates.DateWriteScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Date values written through Hop are stored exactly as with a fresh calendar per value. */
class H2DateWriteTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @Test
  void datesAreStoredAsWithAFreshCalendarPerValue() throws Exception {
    String name =
        "mem:dates" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta = new DatabaseMeta("h2", "H2", "Native", "", name, "", "sa", "");
    try (Database db = new Database(new LoggingObject("dates"), new Variables(), databaseMeta)) {
      db.connect();
      db.setCommit(1000);
      DateWriteScenario.run(db, "TIMESTAMP(3)", "DATE", "DATES");
    }
  }
}
