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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class DatabaseMetaTest {

  private static final String TABLE_NAME = "tableName";
  private static final String DROP_STATEMENT = "dropStatement";

  private DatabaseMeta databaseMeta;
  private IDatabase iDatabase;
  private IVariables variables;

  @BeforeAll
  static void setUpOnce() throws HopException {
    // Register Natives to create a default DatabaseMeta
    DatabasePluginType.getInstance().searchPlugins();
    ValueMetaPluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    iDatabase = mock(IDatabase.class);
    databaseMeta.setIDatabase(iDatabase);
    variables = Variables.getADefaultVariableSpace();
  }

  @Test
  void testGetDatabaseInterfacesMapWontReturnNullIfCalledSimultaneouslyWithClear()
      throws InterruptedException, ExecutionException {
    final AtomicBoolean done = new AtomicBoolean(false);
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.submit(
        () -> {
          while (!done.get()) {
            DatabaseMeta.clearDatabaseInterfacesMap();
          }
        });
    Future<Exception> getFuture =
        executorService.submit(
            () -> {
              int i = 0;
              while (!done.get()) {
                assertNotNull(DatabaseMeta.getIDatabaseMap(), "Got null on try: " + i++);
                if (i > 30000) {
                  done.set(true);
                }
              }
              return null;
            });
    getFuture.get();
  }

  @Test
  void testApplyingDefaultOptions() {
    HashMap<String, String> existingOptions = new HashMap<>();
    existingOptions.put("type1.extra", "extraValue");
    existingOptions.put("type1.existing", "existingValue");
    existingOptions.put("type2.extra", "extraValue2");

    HashMap<String, String> newOptions = new HashMap<>();
    newOptions.put("type1.new", "newValue");
    newOptions.put("type1.existing", "existingDefault");

    when(iDatabase.getExtraOptions()).thenReturn(existingOptions);
    when(iDatabase.getDefaultOptions()).thenReturn(newOptions);

    databaseMeta.applyDefaultOptions(iDatabase);
    verify(iDatabase).addExtraOption("type1", "new", "newValue");
    verify(iDatabase, never()).addExtraOption("type1", "existing", "existingDefault");
  }

  @Test
  void testGetFeatureSummary() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    NoneDatabaseMeta odbm = new NoneDatabaseMeta();
    doCallRealMethod().when(meta).setIDatabase(any(IDatabase.class));
    doCallRealMethod().when(meta).getFeatureSummary(variables);
    doCallRealMethod().when(meta).getAttributes();
    meta.setIDatabase(odbm);
    List<RowMetaAndData> result = meta.getFeatureSummary(variables);
    assertNotNull(result);
    for (RowMetaAndData rmd : result) {
      assertEquals(2, rmd.getRowMeta().size());
      assertEquals("Parameter", rmd.getRowMeta().getValueMeta(0).getName());
      assertEquals(IValueMeta.TYPE_STRING, rmd.getRowMeta().getValueMeta(0).getType());
      assertEquals("Value", rmd.getRowMeta().getValueMeta(1).getName());
      assertEquals(IValueMeta.TYPE_STRING, rmd.getRowMeta().getValueMeta(1).getType());
    }
  }

  @Test
  void testQuoteReservedWords() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    doCallRealMethod().when(meta).quoteReservedWords(any(IRowMeta.class));
    doCallRealMethod().when(meta).quoteField(anyString());
    doCallRealMethod().when(meta).setIDatabase(any(IDatabase.class));
    doReturn("\"").when(meta).getStartQuote();
    doReturn("\"").when(meta).getEndQuote();
    final IDatabase database = mock(IDatabase.class);
    doReturn(true).when(database).isQuoteAllFields();
    meta.setIDatabase(database);

    final RowMeta fields = new RowMeta();
    for (int i = 0; i < 10; i++) {
      final IValueMeta valueMeta = new ValueMetaNone("test_" + i);
      fields.addValueMeta(valueMeta);
    }

    for (int i = 0; i < 10; i++) {
      meta.quoteReservedWords(fields);
    }

    for (int i = 0; i < 10; i++) {
      meta.quoteReservedWords(fields);
      final String name = fields.getValueMeta(i).getName();
      // check valueMeta index in list
      assertTrue(name.contains("test_" + i));
      // check valueMeta is found by quoted name
      assertNotNull(fields.searchValueMeta(name));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testModifyingName() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    NoneDatabaseMeta odbm = new NoneDatabaseMeta();
    doCallRealMethod().when(meta).setIDatabase(any(IDatabase.class));
    doCallRealMethod().when(meta).setName(anyString());
    doCallRealMethod().when(meta).getName();
    meta.setIDatabase(odbm);
    meta.setName("test");

    List<DatabaseMeta> list = new ArrayList<>();
    list.add(meta);

    DatabaseMeta databaseMeta2 = mock(DatabaseMeta.class);
    NoneDatabaseMeta odbm2 = new NoneDatabaseMeta();
    doCallRealMethod().when(databaseMeta2).setIDatabase(any(IDatabase.class));
    doCallRealMethod().when(databaseMeta2).setName(anyString());
    doCallRealMethod().when(databaseMeta2).getName();
    doCallRealMethod().when(databaseMeta2).verifyAndModifyDatabaseName(any(ArrayList.class), any());
    databaseMeta2.setIDatabase(odbm2);
    databaseMeta2.setName("test");

    databaseMeta2.verifyAndModifyDatabaseName(list, null);

    assertNotEquals(meta.getName(), databaseMeta2.getName());
  }

  @Test
  void indexOfName_NullArray() {
    assertEquals(-1, DatabaseMeta.indexOfName(null, ""));
  }

  @Test
  void indexOfName_NullName() {
    assertEquals(-1, DatabaseMeta.indexOfName(new String[] {"1"}, null));
  }

  @Test
  void indexOfName_ExactMatch() {
    assertEquals(1, DatabaseMeta.indexOfName(new String[] {"a", "b", "c"}, "b"));
  }

  @Test
  void indexOfName_NonExactMatch() {
    assertEquals(1, DatabaseMeta.indexOfName(new String[] {"a", "b", "c"}, "B"));
  }

  /**
   * Given that the {@link IDatabase} object is of a new extended type. <br>
   * When {@link DatabaseMeta#getDropTableIfExistsStatement(String)} is called, then the underlying
   * new method of {@link IDatabase} should be used.
   */
  @Test
  void shouldCallNewMethodWhenDatabaseInterfaceIsOfANewType() {
    IDatabase databaseInterfaceNew = mock(IDatabase.class);
    databaseMeta.setIDatabase(databaseInterfaceNew);
    when(databaseInterfaceNew.getDropTableIfExistsStatement(TABLE_NAME)).thenReturn(DROP_STATEMENT);

    String statement = databaseMeta.getDropTableIfExistsStatement(TABLE_NAME);

    assertEquals(DROP_STATEMENT, statement);
  }

  @Test
  void testCheckParameters() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    BaseDatabaseMeta baseMeta = mock(BaseDatabaseMeta.class);
    when(baseMeta.isRequiresName()).thenReturn(true);
    when(baseMeta.getManualUrl()).thenReturn("");
    when(meta.getIDatabase()).thenReturn(baseMeta);
    when(meta.getName()).thenReturn(null);
    when(meta.checkParameters()).thenCallRealMethod();
    assertEquals(2, meta.checkParameters().length);
  }
}
