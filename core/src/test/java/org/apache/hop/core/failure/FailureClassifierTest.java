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

package org.apache.hop.core.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FailureClassifierTest {

  @ParameterizedTest
  @CsvSource({
    "08001, CONNECTIVITY",
    "08S01, CONNECTIVITY",
    "28000, AUTHENTICATION",
    "28P01, AUTHENTICATION",
    "42501, AUTHENTICATION",
    "42601, CONFIGURATION",
    "42P01, SCHEMA_MISMATCH",
    "42703, SCHEMA_MISMATCH",
    "42S22, SCHEMA_MISMATCH",
    "3F000, SCHEMA_MISMATCH",
    "22001, DATA_CONVERSION",
    "22P02, DATA_CONVERSION",
    "23505, CONSTRAINT_VIOLATION",
    "40001, RESOURCE",
    "40P01, RESOURCE",
    "53100, RESOURCE",
    "57P01, CONNECTIVITY",
    "HYT00, CONNECTIVITY",
    "3D000, CONFIGURATION",
  })
  void sqlStates(String sqlState, FailureCategory expected) {
    SQLException sqlException = new SQLException("database error", sqlState);
    HopDatabaseException wrapped = new HopDatabaseException("Unable to execute", sqlException);
    assertEquals(expected, FailureClassifier.classify(wrapped));
  }

  @Test
  void sqlStatesThatDecideNothing() {
    assertNull(FailureClassifier.categoryOfSqlState(null));
    assertNull(FailureClassifier.categoryOfSqlState("42000"));
    assertNull(FailureClassifier.categoryOfSqlState("HY000"));
    assertEquals(
        FailureCategory.UNKNOWN,
        FailureClassifier.classify(new SQLException("vendor specific", "HY000")));
  }

  @Test
  void theRootCauseDecides() {
    // The outer HopValueException would say DATA_CONVERSION, the root cause says CONNECTIVITY
    HopValueException outer =
        new HopValueException("Unable to read", new ConnectException("Connection refused"));
    assertEquals(FailureCategory.CONNECTIVITY, FailureClassifier.classify(outer));
  }

  @Test
  void anOuterRuleAppliesWhenTheRootDecidesNothing() {
    HopValueException outer =
        new HopValueException("Unable to convert", new IllegalStateException("something"));
    assertEquals(FailureCategory.DATA_CONVERSION, FailureClassifier.classify(outer));
  }

  @Test
  void subclassesOfARuleCount() {
    assertEquals(
        FailureCategory.CONNECTIVITY,
        FailureClassifier.classify(new HopException(new SSLHandshakeException("bad cert"))));
    assertEquals(
        FailureCategory.CONSTRAINT_VIOLATION,
        FailureClassifier.classify(new SQLIntegrityConstraintViolationException("duplicate key")));
  }

  @Test
  void outOfDiskSpace() {
    assertEquals(
        FailureCategory.RESOURCE,
        FailureClassifier.classify(
            new HopException(new IOException("write failed: No space left on device"))));
  }

  @Test
  void nothingMatches() {
    assertEquals(
        FailureCategory.UNKNOWN,
        FailureClassifier.classify(new HopTransformException("Something went wrong")));
    assertEquals(FailureCategory.UNKNOWN, FailureClassifier.classify(null));
  }

  @Test
  void aLoopInTheCauseChainEnds() {
    Exception a = new Exception("a");
    Exception b = new Exception("b", a);
    a.initCause(b);
    assertEquals(FailureCategory.UNKNOWN, FailureClassifier.classify(a));
  }

  @Test
  void causeOfAnException() {
    SQLException sqlException = new SQLException("Connection refused", "08001", 17002);
    HopDatabaseException wrapped = new HopDatabaseException("Unable to connect", sqlException);

    FailureCause cause = FailureClassifier.causeOf(wrapped);

    assertEquals(HopDatabaseException.class.getName(), cause.getExceptionClass());
    assertTrue(cause.getMessage().contains("Unable to connect"));
    assertEquals(SQLException.class.getName(), cause.getRootCauseClass());
    assertEquals("Connection refused", cause.getRootCauseMessage());
    assertEquals("08001", cause.getSqlState());
    assertEquals(17002, cause.getVendorCode());
    assertTrue(cause.getStackTrace().contains("SQLException"));
    assertEquals(false, cause.isFromLog());
  }

  @Test
  void longMessagesAreCut() {
    String longMessage = "x".repeat(FailureClassifier.MAX_MESSAGE_LENGTH + 100);
    FailureCause cause = FailureClassifier.causeOf(new HopException(longMessage));
    assertTrue(cause.getMessage().length() <= FailureClassifier.MAX_MESSAGE_LENGTH + 3);
    assertTrue(cause.getStackTrace().length() <= FailureClassifier.MAX_STACK_TRACE_LENGTH + 3);
  }

  @Test
  void causeAndCategoryFromALoggedStackTrace() {
    String stackTrace =
        "org.apache.hop.core.exception.HopException: \n"
            + "Unable to open the file\n"
            + "\tat org.apache.hop.pipeline.transforms.Foo.init(Foo.java:12)\n"
            + "\tat org.apache.hop.core.exception.HopException.<init>(HopException.java:40)\n"
            + "Caused by: java.nio.file.NoSuchFileException: /data/in/orders.csv\n"
            + "\tat java.base/sun.nio.fs.UnixException.translateToIOException(Unknown Source)\n";

    FailureCause cause = FailureClassifier.causeFromLog("Unable to open the file", stackTrace);

    assertTrue(cause.isFromLog());
    assertEquals("Unable to open the file", cause.getMessage());
    assertEquals("org.apache.hop.core.exception.HopException", cause.getExceptionClass());
    assertEquals("java.nio.file.NoSuchFileException", cause.getRootCauseClass());
    assertEquals(
        FailureCategory.MISSING_INPUT,
        FailureClassifier.classifyText("Unable to open the file", stackTrace));
  }

  @Test
  void aLoggedMessageWithoutExceptions() {
    FailureCause cause = FailureClassifier.causeFromLog("Field 'id' not found", null);
    assertNull(cause.getExceptionClass());
    assertEquals(
        FailureCategory.UNKNOWN, FailureClassifier.classifyText("Field 'id' not found", null));
    assertNull(FailureClassifier.causeFromLog(null, null));
  }
}
