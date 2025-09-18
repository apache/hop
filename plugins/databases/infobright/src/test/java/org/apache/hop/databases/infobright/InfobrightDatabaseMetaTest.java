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

package org.apache.hop.databases.infobright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InfobrightDatabaseMetaTest {

  @BeforeEach
  void setup() throws HopException {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(InfobrightDatabaseMeta.class);
  }

  @Test
  void mysqlTestOverrides() {
    InfobrightDatabaseMeta idm = new InfobrightDatabaseMeta();
    idm.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    assertEquals(5029, idm.getDefaultDatabasePort());
  }

  @Test
  void testAddOptionsInfobright() {
    DatabaseMeta databaseMeta =
        new DatabaseMeta("", "Infobright", "JDBC", null, "stub:stub", null, null, null);
    Map<String, String> options = databaseMeta.getExtraOptions();
    if (!options.keySet().contains("INFOBRIGHT.characterEncoding")) {
      fail();
    }
  }

  @Test
  void testAttributesVariable() throws HopDatabaseException {
    IVariables variables = new Variables();
    DatabaseMeta databaseMeta =
        new DatabaseMeta("", "Infobright", "JDBC", null, "stub:stub", null, null, null);
    variables.setVariable("someVar", "someValue");
    databaseMeta.setAttributes(new HashMap<>());
    Map<String, String> props = databaseMeta.getAttributes();
    props.put("EXTRA_OPTION_Infobright.additional_param", "${someVar}");
    databaseMeta.getURL(variables);
    assertTrue(databaseMeta.getURL(variables).contains("someValue"));
  }

  @Test
  void testfindDatabase() {
    List<DatabaseMeta> databases = new ArrayList<>();
    databases.add(
        new DatabaseMeta("  1", "Infobright", "JDBC", null, "stub:stub", null, null, null));
    databases.add(
        new DatabaseMeta("  1  ", "Infobright", "JDBC", null, "stub:stub", null, null, null));
    databases.add(
        new DatabaseMeta("1  ", "Infobright", "JDBC", null, "stub:stub", null, null, null));
    assertNotNull(DatabaseMeta.findDatabase(databases, "1"));
    assertNotNull(DatabaseMeta.findDatabase(databases, "1 "));
    assertNotNull(DatabaseMeta.findDatabase(databases, " 1"));
    assertNotNull(DatabaseMeta.findDatabase(databases, " 1 "));
  }
}
