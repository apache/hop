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

package org.apache.hop.core.database.dml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.ConcreteBaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The size estimate that keeps a statement of several rows within a connection's limit, MySQL's
 * {@code max_allowed_packet} for one: it must never be less than what a driver sends.
 */
class DmlStatementsBytesTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void aValueIsCountedAsItsQuotedAndEscapedLiteralInUtf8() {
    assertEquals(4, DmlStatements.valueBytes(null), "NULL");
    assertEquals(5, DmlStatements.valueBytes("abc"), "'abc'");
    // a, an escaped quote, two bytes, three bytes and a four-byte surrogate pair, quoted.
    String mixed = "a'é中😀";
    assertEquals(14, DmlStatements.valueBytes(mixed));
    assertTrue(
        DmlStatements.valueBytes(mixed) >= mixed.getBytes(StandardCharsets.UTF_8).length + 3,
        "never less than the UTF-8 bytes, the quotes and the escape");
    assertEquals(2 + 2 * 3 + 3, DmlStatements.valueBytes("\\\n\0" + "abc"), "escapes count 2");
    assertEquals(36, DmlStatements.valueBytes(new byte[10]), "every byte escaped, and a prefix");
    assertEquals(15, DmlStatements.valueBytes(new BigDecimal("123.45")));
    assertTrue(
        DmlStatements.valueBytes(new BigDecimal("1E+40"))
            >= new BigDecimal("1E+40").toPlainString().length() + 2);
    assertEquals(32, DmlStatements.valueBytes(Long.MIN_VALUE));
    assertEquals(32, DmlStatements.valueBytes(-Double.MAX_VALUE));
    assertEquals(40, DmlStatements.valueBytes(new Date()));
  }

  @Test
  void theLimitLeavesRoomForTheStatementItselfAndARowCountsItsSeparators() throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new ConcreteBaseDatabaseMeta());
    DmlSyntax syntax = DmlSyntax.builder().insertForm(DmlSyntax.InsertForm.VALUES_LIST).build();
    DmlSpec spec = DmlSpec.insert("T", List.of(DmlColumn.of("ID"), DmlColumn.of("NAME")));

    DmlStatements limited =
        new DmlStatements(
            databaseMeta,
            DmlPlan.multiRow(DmlOperation.INSERT, syntax, 10, 10_000, null),
            spec,
            null,
            (statement, parameterIndex, sourceIndex, value) -> {});
    int head = limited.statement(1).getSql().getBytes(StandardCharsets.UTF_8).length;
    assertEquals(10_000 - head - 1024, limited.getMaxStatementBytes());
    // The row's parentheses and comma, and per value its literal and a parameter's overhead.
    assertEquals(4 + (12 + 32) + (12 + 5), limited.estimateBytes(new Object[] {1L, "abc"}));

    DmlStatements unlimited =
        new DmlStatements(
            databaseMeta,
            DmlPlan.multiRow(DmlOperation.INSERT, syntax, 10, null),
            spec,
            null,
            (statement, parameterIndex, sourceIndex, value) -> {});
    assertEquals(0, unlimited.getMaxStatementBytes());
  }
}
