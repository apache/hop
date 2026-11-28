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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlOperation;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatements;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The INSERT statements of several rows bind every value through bindings resolved once for the row
 * layout, as {@link Database#setValues(IRowMeta, Object[], PreparedStatement)} does, not once per
 * value, and at the parameter index of its row.
 */
class DmlStatementsBindingTest {

  /** Counts how often a binding is asked for, and how often it writes. */
  static final class CountingRule implements IDatabaseTypeRule {
    static final AtomicInteger lookups = new AtomicInteger();
    static final AtomicInteger writes = new AtomicInteger();

    private static final IValueBinding BINDING =
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
              Object value)
              throws java.sql.SQLException {
            writes.incrementAndGet();
            preparedStatement.setObject(index, value);
          }
        };

    @Override
    public IValueBinding getBinding(IDatabase database, IValueMeta valueMeta) {
      lookups.incrementAndGet();
      return valueMeta.isString() ? BINDING : null;
    }

    @Override
    public boolean suppliesBindings() {
      return true;
    }
  }

  @DatabaseMetaPlugin(type = "COUNTING_BINDING", typeDescription = "Counting binding dialect")
  static class CountingBindingDialect extends NoneDatabaseMeta {
    @Override
    public List<IDatabaseTypeRule> getTypeRules() {
      return List.of(new CountingRule());
    }
  }

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    DatabaseTypeRuleRegistry.clearCache();
    CountingRule.lookups.set(0);
    CountingRule.writes.set(0);
  }

  @Test
  void bindingsAreResolvedOncePerLayoutNotPerValue() throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new CountingBindingDialect());
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    Database database = new Database(log, new Variables(), databaseMeta);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("S"));
    rowMeta.addValueMeta(new ValueMetaInteger("I"));
    DmlSpec spec = DmlSpec.insert("T", List.of(DmlColumn.of("S"), DmlColumn.of("I")));
    DmlSyntax syntax = DmlSyntax.builder().insertForm(DmlSyntax.InsertForm.VALUES_LIST).build();
    DmlStatements statements =
        DmlStatements.of(
            database, DmlPlan.multiRow(DmlOperation.INSERT, syntax, 3, null), spec, rowMeta);

    PreparedStatement statement = mock(PreparedStatement.class);
    for (long execution = 0; execution < 4; execution++) {
      for (int row = 0; row < 3; row++) {
        statements.bind(statement, 3, row, new Object[] {"v" + row, execution * 10 + row});
      }
    }

    // One lookup per column, for 4 statements of 3 rows of 2 values.
    assertEquals(2, CountingRule.lookups.get());
    assertEquals(12, CountingRule.writes.get());
    // Row 2 of a statement binds at parameters 5 and 6.
    verify(statement, times(4)).setObject(5, "v2");
    verify(statement).setLong(6, 32L);
  }
}
