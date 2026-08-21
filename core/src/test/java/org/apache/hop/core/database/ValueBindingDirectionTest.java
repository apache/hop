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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A binding is asked for in both directions, so one that serves only a single direction says so by
 * throwing UnsupportedOperationException from the other. The engine then reads or writes the value
 * as it did before the binding existed, rather than failing the row.
 */
class ValueBindingDirectionTest {

  /** Rejects reading, the way the Postgres JSON and the DuckDB date bindings do. */
  private static final IValueBinding WRITE_ONLY =
      new IValueBinding() {
        @Override
        public Object read(
            IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index) {
          throw new UnsupportedOperationException("This binding only writes values");
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value) {
          // Not what these tests are about.
        }
      };

  private static final IValueBinding READ_ONLY =
      new IValueBinding() {
        @Override
        public Object read(
            IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index) {
          return null;
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value) {
          throw new UnsupportedOperationException("This binding only reads values");
        }
      };

  @DatabaseMetaPlugin(type = "WRITE_ONLY_BINDING", typeDescription = "Write only binding dialect")
  static class WriteOnlyBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, WRITE_ONLY).build();
    }
  }

  @DatabaseMetaPlugin(type = "READ_ONLY_BINDING", typeDescription = "Read only binding dialect")
  static class ReadOnlyBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return DatabaseTypes.rules().bind(IValueMeta.TYPE_STRING, READ_ONLY).build();
    }
  }

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    // Binding rules are cached per dialect class.
    DatabaseTypeRuleRegistry.clearCache();
  }

  @Test
  void aWriteOnlyBindingLeavesReadingToTheValueType() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn("read by the value type");

    Object value =
        new WriteOnlyBindingDialect().getValueFromResultSet(resultSet, new ValueMetaString("s"), 0);

    assertEquals("read by the value type", value);
  }

  @Test
  void aReadOnlyBindingLeavesWritingToTheValueType() throws Exception {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getIDatabase()).thenReturn(new ReadOnlyBindingDialect());
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);

    new Database(log, new Variables(), databaseMeta)
        .setValue(preparedStatement, new ValueMetaString("s"), "written by the value type", 1);

    verify(preparedStatement).setString(1, "written by the value type");
  }
}
