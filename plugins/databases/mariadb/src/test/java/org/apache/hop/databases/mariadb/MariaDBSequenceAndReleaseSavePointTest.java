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

package org.apache.hop.databases.mariadb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MariaDBSequenceAndReleaseSavePointTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  final String sequenceName = "sequence_name";

  // Set these parameters for the test
  IDatabase db = new MariaDBDatabaseMeta();
  Boolean sequenceSupport = true;
  Boolean savepointSupport = false;

  @Test
  void testSequenceSupport() {
    assertSupports(db, sequenceSupport);
    assertEquals("SELECT NEXTVAL(sequence_name)", db.getSqlNextSequenceValue(sequenceName));
    assertEquals("SELECT LASTVAL(sequence_name)", db.getSqlCurrentSequenceValue(sequenceName));
    assertEquals(
        "SELECT TABLE_NAME AS sequence_name FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_TYPE = 'SEQUENCE' AND TABLE_SCHEMA = DATABASE()",
        db.getSqlListOfSequences());
  }

  @Test
  void testSequenceExistsInCurrentDatabase() {
    assertEquals(
        "SELECT TABLE_NAME AS sequence_name FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_TYPE = 'SEQUENCE' AND TABLE_SCHEMA = DATABASE()"
            + " AND TABLE_NAME = 'sequence_name'",
        db.getSqlSequenceExists(sequenceName));
  }

  @Test
  void testSequenceExistsInQuotedSchema() {
    // Hop passes schema-qualified names quoted the MySQL way, e.g. `sales`.`order_seq`
    assertEquals(
        "SELECT TABLE_NAME AS sequence_name FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_TYPE = 'SEQUENCE' AND TABLE_SCHEMA = 'sales'"
            + " AND TABLE_NAME = 'order_seq'",
        db.getSqlSequenceExists("`sales`.`order_seq`"));
    assertEquals(
        "SELECT NEXTVAL(`sales`.`order_seq`)", db.getSqlNextSequenceValue("`sales`.`order_seq`"));
  }

  @Test
  void testSequenceExistsEscapesQuotes() {
    assertEquals(
        "SELECT TABLE_NAME AS sequence_name FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_TYPE = 'SEQUENCE' AND TABLE_SCHEMA = DATABASE()"
            + " AND TABLE_NAME = 'o''seq'",
        db.getSqlSequenceExists("o'seq"));
  }

  @Test
  void testSavepointSuport() {
    if (savepointSupport) {
      assertTrue(db.isReleaseSavepoint());
    } else {
      assertFalse(db.isReleaseSavepoint());
    }
  }

  public static void assertSupports(IDatabase db, boolean expected) {
    String dbType = db.getClass().getSimpleName();
    if (expected) {
      assertTrue(db.isSupportsSequences(), dbType);
      assertFalse(Utils.isEmpty(db.getSqlListOfSequences()), dbType + ": List of Sequences");
      assertFalse(Utils.isEmpty(db.getSqlSequenceExists("testSeq")), dbType + ": Sequence Exists");
      assertFalse(
          Utils.isEmpty(db.getSqlCurrentSequenceValue("testSeq")), dbType + ": Current Value");
      assertFalse(Utils.isEmpty(db.getSqlNextSequenceValue("testSeq")), dbType + ": Next Value");
    } else {
      assertFalse(db.isSupportsSequences(), db.getClass().getSimpleName());
      assertTrue(Utils.isEmpty(db.getSqlListOfSequences()), dbType + ": List of Sequences");
      assertTrue(Utils.isEmpty(db.getSqlSequenceExists("testSeq")), dbType + ": Sequence Exists");
      assertTrue(
          Utils.isEmpty(db.getSqlCurrentSequenceValue("testSeq")), dbType + ": Current Value");
      assertTrue(Utils.isEmpty(db.getSqlNextSequenceValue("testSeq")), dbType + ": Next Value");
    }
  }
}
