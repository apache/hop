/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.tableinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 2/4/14 Time: 5:47 PM
 */
public class TableInputMetaTest {
  LoadSaveTester loadSaveTester;
  Class<TableInputMeta> testMetaClass = TableInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public class TableInputMetaHandler extends TableInputMeta {
    public Database database = mock( Database.class );

    @Override
    protected Database getDatabase() {
      return database;
    }
  }

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "databaseMeta", "sQL", "rowLimit", "executeEachInputRow", "variableReplacementActive", "lazyConversionActive" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testGetFields() throws Exception {
    TableInputMetaHandler meta = new TableInputMetaHandler();
    meta.setLazyConversionActive( true );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    meta.setDatabaseMeta( dbMeta );
    Database mockDB = meta.getDatabase();
    when( mockDB.getQueryFields( anyString(), anyBoolean() ) ).thenReturn( createMockFields() );

    IRowMeta expectedRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString( "field1" );
    valueMeta.setStorageMetadata( new ValueMetaString( "field1" ) );
    valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    expectedRowMeta.addValueMeta( valueMeta );

    IVariables variables = mock( IVariables.class );
    IRowMeta iRowMeta = new RowMeta();
    meta.getFields( iRowMeta, "TABLE_INPUT_META", null, null, variables, null );

    assertEquals( expectedRowMeta.toString(), iRowMeta.toString() );
  }

  private IRowMeta createMockFields() {
    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString( "field1" );
    iRowMeta.addValueMeta( valueMeta );
    return iRowMeta;
  }
}
