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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Every Oracle connection that existed before the connection type options were added deserializes
 * to AUTOMATIC, so AUTOMATIC has to keep producing byte-for-byte the same URL the old code did.
 * This runs the original algorithm, copied verbatim from before the change, against the current one
 * over every combination of the inputs that used to matter.
 */
class OracleLegacyUrlParityTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private OracleDatabaseMeta meta;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
    meta = new OracleDatabaseMeta();
    meta.setPluginId("ORACLE");
    meta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    meta.addDefaultOptions();
  }

  /** The pre-change implementation, copied verbatim. Do not tidy: it is the reference. */
  private static String legacyUrl(String hostname, String port, String databaseName) {
    if (!Utils.isEmpty(databaseName)
        && (databaseName.startsWith("/") || databaseName.startsWith(":"))) {
      return "jdbc:oracle:thin:@" + hostname + ":" + port + databaseName;
    } else if (Utils.isEmpty(hostname) && (Utils.isEmpty(port) || port.equals("-1"))) {
      return "jdbc:oracle:thin:@" + databaseName;
    } else {
      return "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + databaseName;
    }
  }

  @Test
  @DisplayName("AUTOMATIC matches the pre-change URL for every combination of inputs")
  void automaticIsByteForByteIdenticalToTheOldCode() throws Exception {
    List<String> hostnames = Arrays.asList("", "FOO", "db.example.com");
    List<String> ports = Arrays.asList("", "-1", "1521", "65534");
    List<String> databaseNames =
        Arrays.asList(
            "",
            "BAR",
            ":BAR",
            "/BAR",
            "ORCLPDB1",
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1521))(CONNECT_DATA=(SID=s)))");

    List<String> mismatches = new ArrayList<>();
    for (String hostname : hostnames) {
      for (String port : ports) {
        for (String databaseName : databaseNames) {
          String expected = legacyUrl(hostname, port, databaseName);
          String actual = meta.getURL(hostname, port, databaseName);
          if (!expected.equals(actual)) {
            mismatches.add(
                "("
                    + hostname
                    + ", "
                    + port
                    + ", "
                    + databaseName
                    + ") old="
                    + expected
                    + " new="
                    + actual);
          }
        }
      }
    }
    assertEquals(List.of(), mismatches, "AUTOMATIC must not change any existing connection's URL");
  }

  @Test
  @DisplayName("a connection with no stored options behaves exactly as before")
  void untouchedConnectionIsUnchanged() throws Exception {
    // What deserialization leaves behind for metadata written before these options existed.
    meta.setConnectionType(null);
    meta.setTlsCredentialType(null);

    assertEquals(legacyUrl("FOO", "1521", "BAR"), meta.getURL("FOO", "1521", "BAR"));
    assertEquals(legacyUrl("", "", "TNSALIAS"), meta.getURL("", "", "TNSALIAS"));
    assertEquals(legacyUrl(null, "-1", "TNSALIAS"), meta.getURL(null, "-1", "TNSALIAS"));
  }
}
