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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * getFirstRows has to work for databases that limit rows after SELECT as well as those that limit
 * at the end of the statement. See issue 8013.
 *
 * <p>The statement is captured on its way to getRows, so this exercises the real method rather than
 * a copy of what it is believed to build.
 */
class FirstRowsLimitTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private String firstRowsSql(String prefix, String suffix, int limit) throws Exception {
    IDatabase dialect = mock(IDatabase.class);
    when(dialect.getLimitClausePrefix(limit)).thenReturn(prefix);
    when(dialect.getLimitClause(limit)).thenReturn(suffix);
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(dialect);

    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);

    Database database = spy(new Database(log, new Variables(), databaseMeta));
    doReturn(List.of()).when(database).getRows(anyString(), anyInt(), any());

    database.getFirstRows("CUSTOMER", limit, null);

    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(database).getRows(sql.capture(), eq(limit), any());
    return sql.getValue();
  }

  @Test
  void aDatabaseThatLimitsAfterSelectGetsItsClauseThere() throws Exception {
    assertEquals("SELECT TOP 10 * FROM CUSTOMER", firstRowsSql(" TOP 10", "", 10));
  }

  @Test
  void aDatabaseThatLimitsAtTheEndIsUnaffected() throws Exception {
    assertEquals("SELECT * FROM CUSTOMER LIMIT 10", firstRowsSql("", " LIMIT 10", 10));
  }

  @Test
  void aDatabaseWithNoLimitClauseAtAllStillProducesValidSql() throws Exception {
    assertEquals("SELECT * FROM CUSTOMER", firstRowsSql("", "", 10));
  }

  @Test
  void noLimitMeansNeitherClauseIsAskedFor() throws Exception {
    assertEquals("SELECT * FROM CUSTOMER", firstRowsSql(" TOP 10", " LIMIT 10", 0));
  }

  @Test
  void theDefaultForADialectThatDeclaresNothingIsEmpty() {
    assertEquals("", new NoneDatabaseMeta().getLimitClausePrefix(10));
  }
}
